"""
dl-zendesk-tests | Lambda de pruebas de calidad de datos
Pipeline: zendesk (Zendesk -> AppFlow -> S3 -> Athena)
Ejecuta 10 queries SQL de validacion sobre la tabla zendesk_tickets en la capa raw.
Criterio: 0 filas = PASS, >0 filas = FAIL
"""

import json
import time
import re
import logging
import os
import boto3
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ─── Configuracion ─────────────────────────────────────────────────────────────
DATABASE = os.environ['GLUE_DATABASE']
ATHENA_OUTPUT = os.environ['ATHENA_OUTPUT']
ATHENA_WORKGROUP = os.environ['ATHENA_WORKGROUP']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

PIPELINE = 'zendesk'
QUERY_TIMEOUT = 120
POLL_INTERVAL = 2

athena = boto3.client('athena')
sns = boto3.client('sns')


# ─── Definicion de las 10 pruebas ──────────────────────────────────────────────
# {db} se reemplaza con DATABASE en tiempo de ejecucion.
# Las subconsultas (SELECT MAX(dt) FROM ...) se reemplazan con el dt literal
# si se proporciona el parametro dt.
# pre_wrapped=True indica que la query ya incluye el COUNT(*) wrapper (para CTEs).

TESTS = [
    # ── Validacion de Datos ────────────────────────────────────────────────────
    {
        "id": 1,
        "name": "Campos obligatorios nulos en zendesk_tickets (id, status, created_at, brand_id)",
        "query": (
            "SELECT id, status, created_at, brand_id, dt "
            "FROM {db}.zendesk_tickets "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.zendesk_tickets) "
            "AND ("
            "id IS NULL "
            "OR status IS NULL OR TRIM(status) = '' "
            "OR created_at IS NULL "
            "OR brand_id IS NULL"
            ")"
        )
    },
    {
        "id": 2,
        "name": "Campos de contexto operativo nulos en zendesk_tickets (subject, requester_id, assignee_id)",
        "query": (
            "SELECT * "
            "FROM {db}.zendesk_tickets "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.zendesk_tickets) "
            "AND ("
            "subject IS NULL OR TRIM(subject) = '' "
            "OR requester_id IS NULL OR TRIM(CAST(requester_id AS VARCHAR)) = '' "
            "OR assignee_id IS NULL OR TRIM(CAST(assignee_id AS VARCHAR)) = ''"
            ")"
        )
    },
    # ── Calidad de Datos ───────────────────────────────────────────────────────
    {
        "id": 3,
        "name": "Valores invalidos de status en zendesk_tickets",
        "query": (
            "SELECT * "
            "FROM {db}.zendesk_tickets "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.zendesk_tickets) "
            "AND LOWER(TRIM(status)) NOT IN ('new', 'open', 'pending', 'hold', 'solved', 'closed')"
        )
    },
    {
        "id": 4,
        "name": "Valores invalidos de type en zendesk_tickets",
        "query": (
            "SELECT * "
            "FROM {db}.zendesk_tickets "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.zendesk_tickets) "
            "AND type IS NOT NULL "
            "AND TRIM(type) <> '' "
            "AND LOWER(TRIM(type)) NOT IN ('problem', 'incident', 'question', 'task')"
        )
    },
    {
        "id": 5,
        "name": "Valores invalidos de priority en zendesk_tickets",
        "query": (
            "SELECT * "
            "FROM {db}.zendesk_tickets "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.zendesk_tickets) "
            "AND priority IS NOT NULL "
            "AND TRIM(priority) <> '' "
            "AND LOWER(TRIM(priority)) NOT IN ('low', 'normal', 'high', 'urgent')"
        )
    },
    {
        "id": 6,
        "name": "Coherencia temporal created_at / updated_at en zendesk_tickets",
        "query": (
            "SELECT * "
            "FROM {db}.zendesk_tickets "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.zendesk_tickets) "
            "AND TRY_CAST(updated_at AS TIMESTAMP) < TRY_CAST(created_at AS TIMESTAMP)"
        )
    },
    {
        "id": 7,
        "name": "Formato invalido de fechas en zendesk_tickets",
        "query": (
            "SELECT * "
            "FROM {db}.zendesk_tickets "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.zendesk_tickets) "
            "AND ("
            "(created_at IS NOT NULL AND TRY_CAST(created_at AS TIMESTAMP) IS NULL) "
            "OR (updated_at IS NOT NULL AND TRY_CAST(updated_at AS TIMESTAMP) IS NULL)"
            ")"
        )
    },
    {
        "id": 8,
        "name": "Fechas created_at fuera de rango en zendesk_tickets",
        "query": (
            "SELECT * "
            "FROM {db}.zendesk_tickets "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.zendesk_tickets) "
            "AND ("
            "TRY_CAST(created_at AS TIMESTAMP) < TIMESTAMP '2020-01-01 00:00:00' "
            "OR TRY_CAST(created_at AS TIMESTAMP) > NOW()"
            ")"
        )
    },
    # ── Integridad de Datos ────────────────────────────────────────────────────
    {
        "id": 9,
        "name": "Unicidad del campo id por particion en zendesk_tickets",
        "query": (
            "SELECT id, COUNT(*) AS cantidad_ocurrencias "
            "FROM {db}.zendesk_tickets "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.zendesk_tickets) "
            "GROUP BY id "
            "HAVING COUNT(*) > 1 "
            "ORDER BY cantidad_ocurrencias DESC"
        )
    },
    {
        "id": 10,
        "name": "Completitud del volumen de registros por particion en zendesk_tickets",
        "no_dt": True,
        "pre_wrapped": True,
        "query": (
            "WITH conteo_por_particion AS ("
            "SELECT dt, COUNT(*) AS total_registros "
            "FROM {db}.zendesk_tickets "
            "GROUP BY dt"
            "), "
            "promedio_historico AS ("
            "SELECT AVG(total_registros) AS promedio "
            "FROM conteo_por_particion"
            ") "
            "SELECT COUNT(*) AS error_count "
            "FROM conteo_por_particion c "
            "CROSS JOIN promedio_historico p "
            "WHERE c.total_registros < p.promedio * 0.5"
        )
    },
]


# ─── Funciones auxiliares ───────────────────────────────────────────────────────

def apply_dt(query, dt_value):
    """Reemplaza subconsultas MAX(dt) con el valor literal de dt si se provee."""
    if not dt_value:
        return query
    pattern = r"\(\s*SELECT\s+MAX\s*\(\s*dt\s*\)\s+FROM\s+\S+\s*\)"
    return re.sub(pattern, f"'{dt_value}'", query, flags=re.IGNORECASE)


def wrap_count(query, pre_wrapped=False):
    """Envuelve la query en COUNT(*) para evaluar pass/fail."""
    if pre_wrapped:
        return query
    clean = query.strip().rstrip(';')
    return f"SELECT COUNT(*) AS error_count FROM ({clean}) _t"


def execute_athena_query(sql):
    """Ejecuta query en Athena y espera el resultado. Retorna query_execution_id."""
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            'Database': DATABASE,
            'Catalog': 'AwsDataCatalog'
        },
        ResultConfiguration={
            'OutputLocation': ATHENA_OUTPUT
        },
        WorkGroup=ATHENA_WORKGROUP
    )
    qid = response['QueryExecutionId']

    start = time.time()
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status['QueryExecution']['Status']['State']

        if state == 'SUCCEEDED':
            return qid

        if state in ('FAILED', 'CANCELLED'):
            reason = status['QueryExecution']['Status'].get(
                'StateChangeReason', 'Sin detalle'
            )
            raise Exception(f"Query {state}: {reason}")

        if time.time() - start > QUERY_TIMEOUT:
            try:
                athena.stop_query_execution(QueryExecutionId=qid)
            except Exception:
                pass
            raise TimeoutError(f"Query excedio timeout de {QUERY_TIMEOUT}s")

        time.sleep(POLL_INTERVAL)


def get_error_count(query_execution_id):
    """Obtiene el error_count del resultado de la query COUNT(*)."""
    response = athena.get_query_results(
        QueryExecutionId=query_execution_id,
        MaxResults=2
    )
    rows = response['ResultSet']['Rows']
    # Fila 0 = header, Fila 1 = dato
    if len(rows) >= 2:
        return int(rows[1]['Data'][0].get('VarCharValue', '0'))
    return 0


def send_notification(failures, dt_value, request_id, summary, event):
    """Envia notificacion SNS detallada solo si hay pruebas fallidas."""
    if not failures or not SNS_TOPIC_ARN:
        return

    dt_display = dt_value or 'MAX(dt)'
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    separator = '=' * 48
    line = '-' * 48

    msg = []
    msg.append(separator)
    msg.append('ERROR EN PRUEBAS DE CALIDAD - PIPELINE ZENDESK')
    msg.append(separator)
    msg.append('')
    msg.append(f'Fecha/Hora: {now}')
    msg.append('')

    # ── Informacion del pipeline ──
    msg.append(line)
    msg.append('INFORMACION DEL PIPELINE')
    msg.append(line)
    msg.append(f'Pipeline:       {PIPELINE}')
    msg.append(f'Database:       {DATABASE}')
    msg.append(f'Athena Output:  {ATHENA_OUTPUT}')
    msg.append(f'Workgroup:      {ATHENA_WORKGROUP}')
    msg.append(f'Particion (dt): {dt_display}')
    msg.append(f'Request ID:     {request_id}')
    msg.append('')

    # ── Resumen de ejecucion ──
    msg.append(line)
    msg.append('RESUMEN DE EJECUCION')
    msg.append(line)
    msg.append(f'Total pruebas:  {summary.get("total", len(TESTS))}')
    msg.append(f'Aprobadas:      {summary.get("passed", 0)}')
    msg.append(f'Fallidas:       {summary.get("failed", 0)}')
    msg.append(f'Con error:      {summary.get("errored", 0)}')
    msg.append('')

    # ── Detalle de pruebas fallidas ──
    msg.append(line)
    msg.append('DETALLE DE PRUEBAS FALLIDAS')
    msg.append(line)
    for f in failures:
        test_id = f['id']
        test_name = f['name']
        errors = f['errors']
        exception = f.get('exception')

        if errors == -1:
            msg.append(f'Prueba {test_id}: {test_name}')
            msg.append(f'  Estado:  ERROR DE EJECUCION')
            msg.append(f'  Detalle: {exception[:300] if exception else "Sin detalle"}')
        else:
            msg.append(f'Prueba {test_id}: {test_name}')
            msg.append(f'  Estado:  FAIL')
            msg.append(f'  Registros con error: {errors}')
        msg.append('')

    # ── Evento original ──
    msg.append(line)
    msg.append('EVENTO ORIGINAL (JSON)')
    msg.append(line)
    try:
        msg.append(json.dumps(event, indent=2, default=str))
    except Exception:
        msg.append(str(event))
    msg.append('')
    msg.append(separator)

    message = '\n'.join(msg)

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f'ERROR: Pipeline {PIPELINE} - {summary.get("failed", 0)} pruebas fallidas | dt={dt_display}',
        Message=message
    )
    logger.info(f'Notificacion SNS enviada ({len(failures)} pruebas fallidas)')


# ─── Handler principal ─────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Evento esperado (todos los campos opcionales):
    {
        "dt": "2026-04-01"   // Fecha de particion. Si no se provee, usa MAX(dt).
    }
    """
    dt_value = event.get('dt')
    request_id = getattr(context, 'aws_request_id', 'local-test')

    logger.info(json.dumps({
        "action": "START",
        "pipeline": PIPELINE,
        "dt": dt_value or "MAX(dt)",
        "request_id": request_id,
        "total_tests": len(TESTS)
    }))

    results = []
    failures = []
    passed = 0
    failed = 0
    errored = 0

    for test in TESTS:
        test_id = test['id']
        test_name = test['name']

        try:
            # Construir query
            query = test['query'].format(db=DATABASE)

            # Aplicar parametro dt (si no tiene flag no_dt)
            if not test.get('no_dt'):
                query = apply_dt(query, dt_value)

            # Envolver en COUNT(*)
            count_query = wrap_count(query, test.get('pre_wrapped', False))

            logger.info(f"Prueba {test_id}: {test_name} | Ejecutando...")

            # Ejecutar en Athena
            qid = execute_athena_query(count_query)
            error_count = get_error_count(qid)

            status = 'PASS' if error_count == 0 else 'FAIL'

            if status == 'PASS':
                passed += 1
            else:
                failed += 1
                failures.append({
                    'id': test_id,
                    'name': test_name,
                    'errors': error_count
                })

            logger.info(f"Prueba {test_id}: {test_name} | {status} | {error_count} errores")

            results.append({
                'id': test_id,
                'name': test_name,
                'status': status,
                'error_count': error_count,
                'query_execution_id': qid
            })

        except Exception as e:
            errored += 1
            error_msg = str(e)
            logger.error(f"Prueba {test_id}: {test_name} | ERROR | {error_msg}")

            failures.append({
                'id': test_id,
                'name': test_name,
                'errors': -1,
                'exception': error_msg
            })

            results.append({
                'id': test_id,
                'name': test_name,
                'status': 'ERROR',
                'error_count': -1,
                'error_message': error_msg
            })

    # Resumen
    summary = {
        'pipeline': PIPELINE,
        'dt': dt_value or 'MAX(dt)',
        'request_id': request_id,
        'total': len(TESTS),
        'passed': passed,
        'failed': failed,
        'errored': errored,
        'all_passed': len(failures) == 0
    }

    logger.info(json.dumps({"action": "SUMMARY", **summary}))

    # Notificar solo si hay fallos
    if failures:
        send_notification(failures, dt_value, request_id, summary, event)

    return {
        'statusCode': 200 if not failures else 500,
        'body': json.dumps({
            'summary': summary,
            'results': results
        })
    }