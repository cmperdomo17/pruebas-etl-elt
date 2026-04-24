"""
dl-magento-tests | Lambda de pruebas de calidad de datos
Pipeline: magento (MySQL Magento -> Lambda -> S3 -> Athena)
Ejecuta 22 queries SQL de validacion sobre tablas de la capa Stage (prefijo jmb_).
Criterio: 0 filas = PASS, >0 filas = FAIL

Nota: A diferencia de los pipelines icomm y zendesk, este pipeline no recibe un
parametro dt opcional. La particion evaluada queda determinada automaticamente
por SELECT MAX(dt) en cada tabla. Las pruebas 10 y 11 (continuidad de particiones)
analizan el rango historico completo y no una unica particion.
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

PIPELINE = 'magento'
QUERY_TIMEOUT = 120
POLL_INTERVAL = 2

athena = boto3.client('athena')
sns = boto3.client('sns')


# ─── Definicion de las 22 pruebas ──────────────────────────────────────────────
# {db} se reemplaza con DATABASE en tiempo de ejecucion.
# pre_wrapped=True indica que la query ya incluye el COUNT(*) wrapper (para CTEs
# o queries que ya devuelven un conteo directo).
# no_dt=True indica que la query opera sobre multiples particiones y no debe
# aplicarse sustitucion de MAX(dt).

TESTS = [
    # ── Validacion de Datos ────────────────────────────────────────────────────
    {
        "id": 1,
        "name": "entity_id nulo en jmb_sales_order",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order) "
            "AND entity_id IS NULL"
        )
    },
    {
        "id": 2,
        "name": "increment_id nulo en jmb_sales_order",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order) "
            "AND increment_id IS NULL"
        )
    },
    {
        "id": 3,
        "name": "created_at nulo en jmb_sales_order",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order) "
            "AND created_at IS NULL"
        )
    },
    {
        "id": 4,
        "name": "item_id nulo en jmb_sales_order_item",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_sales_order_item "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order_item) "
            "AND item_id IS NULL"
        )
    },
    {
        "id": 5,
        "name": "order_id nulo en jmb_sales_order_item",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_sales_order_item "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order_item) "
            "AND order_id IS NULL"
        )
    },
    {
        "id": 6,
        "name": "entity_id nulo en jmb_customer_entity",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_customer_entity "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_customer_entity) "
            "AND entity_id IS NULL"
        )
    },
    {
        "id": 7,
        "name": "email nulo o vacio en jmb_customer_entity",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_customer_entity "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_customer_entity) "
            "AND (email IS NULL OR TRIM(email) = '')"
        )
    },
    {
        "id": 8,
        "name": "entity_id nulo en jmb_catalog_product_entity",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_catalog_product_entity "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_catalog_product_entity) "
            "AND entity_id IS NULL"
        )
    },
    {
        "id": 9,
        "name": "sku nulo o vacio en jmb_catalog_product_entity",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_catalog_product_entity "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_catalog_product_entity) "
            "AND (sku IS NULL OR TRIM(sku) = '')"
        )
    },
    {
        "id": 10,
        "name": "Continuidad de particiones en jmb_sales_order (desde 2024-01-01)",
        "no_dt": True,
        "pre_wrapped": True,
        "query": (
            "WITH particiones AS ("
            "SELECT DISTINCT CAST(dt AS DATE) AS fecha "
            "FROM {db}.jmb_sales_order "
            "WHERE CAST(dt AS DATE) >= DATE '2024-01-01' "
            "ORDER BY fecha), "
            "con_siguiente AS ("
            "SELECT fecha, "
            "LEAD(fecha) OVER (ORDER BY fecha) AS siguiente "
            "FROM particiones) "
            "SELECT COUNT(*) AS error_count "
            "FROM con_siguiente "
            "WHERE date_diff('day', fecha, siguiente) > 1"
        )
    },
    {
        "id": 11,
        "name": "Continuidad de particiones en jmb_sales_order_item (desde 2024-01-01)",
        "no_dt": True,
        "pre_wrapped": True,
        "query": (
            "WITH particiones AS ("
            "SELECT DISTINCT CAST(dt AS DATE) AS fecha "
            "FROM {db}.jmb_sales_order_item "
            "WHERE CAST(dt AS DATE) >= DATE '2024-01-01' "
            "ORDER BY fecha), "
            "con_siguiente AS ("
            "SELECT fecha, "
            "LEAD(fecha) OVER (ORDER BY fecha) AS siguiente "
            "FROM particiones) "
            "SELECT COUNT(*) AS error_count "
            "FROM con_siguiente "
            "WHERE date_diff('day', fecha, siguiente) > 1"
        )
    },
    # ── Calidad de Datos ───────────────────────────────────────────────────────
    {
        "id": 12,
        "name": "Valores invalidos de status en jmb_sales_order",
        "query": (
            "SELECT status, COUNT(*) AS total_registros "
            "FROM {db}.jmb_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order) "
            "AND status NOT IN ("
            "'canceled', 'holded', 'pending', 'processing', "
            "'delivered', 'order_in_route', 'delivery_failed', "
            "'pickedup', 'ready_for_packaging', 'stand_by', "
            "'center_101', 'pending_electronic_billing', 'verified', "
            "'return_completed', 'return_in_situ', 'return_in_store', "
            "'return_to_store') "
            "GROUP BY status "
            "ORDER BY total_registros DESC"
        )
    },
    {
        "id": 13,
        "name": "Inconsistencia temporal created_at > updated_at en jmb_sales_order",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order) "
            "AND created_at IS NOT NULL "
            "AND updated_at IS NOT NULL "
            "AND created_at > updated_at"
        )
    },
    {
        "id": 14,
        "name": "base_grand_total negativo en jmb_sales_order",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order) "
            "AND base_grand_total < 0"
        )
    },
    {
        "id": 15,
        "name": "qty_ordered negativo en jmb_sales_order_item",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_sales_order_item "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order_item) "
            "AND qty_ordered < 0"
        )
    },
    {
        "id": 16,
        "name": "base_price negativo en jmb_sales_order_item",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_sales_order_item "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order_item) "
            "AND base_price < 0"
        )
    },
    {
        "id": 17,
        "name": "entity_id duplicado en jmb_sales_order",
        "query": (
            "SELECT entity_id, COUNT(*) AS total_apariciones "
            "FROM {db}.jmb_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order) "
            "GROUP BY entity_id "
            "HAVING COUNT(*) > 1 "
            "ORDER BY total_apariciones DESC"
        )
    },
    {
        "id": 18,
        "name": "item_id duplicado en jmb_sales_order_item",
        "query": (
            "SELECT item_id, COUNT(*) AS total_apariciones "
            "FROM {db}.jmb_sales_order_item "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order_item) "
            "GROUP BY item_id "
            "HAVING COUNT(*) > 1 "
            "ORDER BY total_apariciones DESC"
        )
    },
    # ── Integridad de Datos ────────────────────────────────────────────────────
    {
        "id": 19,
        "name": "Items huerfanos en jmb_sales_order_item sin orden en jmb_sales_order",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_sales_order_item i "
            "LEFT JOIN {db}.jmb_sales_order o "
            "ON i.order_id = o.entity_id "
            "AND o.dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order) "
            "WHERE i.dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order_item) "
            "AND o.entity_id IS NULL"
        )
    },
    {
        "id": 20,
        "name": "Direcciones huerfanas en jmb_sales_order_address sin orden en jmb_sales_order",
        "no_dt": True,
        "pre_wrapped": True,
        "query": (
            "WITH ordenes AS ("
            "SELECT DISTINCT entity_id "
            "FROM {db}.jmb_sales_order) "
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_sales_order_address a "
            "LEFT JOIN ordenes o ON a.parent_id = o.entity_id "
            "WHERE a.dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order_address) "
            "AND o.entity_id IS NULL"
        )
    },
    {
        "id": 21,
        "name": "Ordenes con customer_id huerfano en jmb_sales_order",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_sales_order o "
            "LEFT JOIN {db}.jmb_customer_entity c "
            "ON CAST(o.customer_id AS BIGINT) = c.entity_id "
            "AND c.dt = (SELECT MAX(dt) FROM {db}.jmb_customer_entity) "
            "WHERE o.dt = (SELECT MAX(dt) FROM {db}.jmb_sales_order) "
            "AND o.customer_is_guest != 1 "
            "AND o.customer_id IS NOT NULL "
            "AND c.entity_id IS NULL"
        )
    },
    {
        "id": 22,
        "name": "Asociaciones huerfanas en jmb_catalog_category_product sin producto en catalogo",
        "pre_wrapped": True,
        "query": (
            "SELECT COUNT(*) AS error_count "
            "FROM {db}.jmb_catalog_category_product cp "
            "LEFT JOIN {db}.jmb_catalog_product_entity pe "
            "ON cp.product_id = CAST(pe.entity_id AS BIGINT) "
            "AND pe.dt = (SELECT MAX(dt) FROM {db}.jmb_catalog_product_entity) "
            "WHERE cp.dt = (SELECT MAX(dt) FROM {db}.jmb_catalog_category_product) "
            "AND pe.entity_id IS NULL"
        )
    },
]


# ─── Funciones auxiliares ───────────────────────────────────────────────────────

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


def send_notification(failures, request_id, summary, event):
    """Envia notificacion SNS detallada solo si hay pruebas fallidas."""
    if not failures or not SNS_TOPIC_ARN:
        return

    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    separator = '=' * 48
    line = '-' * 48

    msg = []
    msg.append(separator)
    msg.append('ERROR EN PRUEBAS DE CALIDAD - PIPELINE MAGENTO')
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
    msg.append(f'Particion (dt): MAX(dt)')
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
        Subject=f'ERROR: Pipeline {PIPELINE} - {summary.get("failed", 0)} pruebas fallidas | dt=MAX(dt)',
        Message=message
    )
    logger.info(f'Notificacion SNS enviada ({len(failures)} pruebas fallidas)')


# ─── Handler principal ─────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Evento esperado: {} (sin parametros).
    A diferencia de icomm y zendesk, este pipeline no recibe parametro dt.
    La particion evaluada se determina automaticamente con MAX(dt) en cada tabla.
    """
    request_id = getattr(context, 'aws_request_id', 'local-test')

    logger.info(json.dumps({
        "action": "START",
        "pipeline": PIPELINE,
        "dt": "MAX(dt)",
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
        'dt': 'MAX(dt)',
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
        send_notification(failures, request_id, summary, event)

    return {
        'statusCode': 200 if not failures else 500,
        'body': json.dumps({
            'summary': summary,
            'results': results
        })
    }