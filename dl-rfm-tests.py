"""
dl-rfm-tests | Lambda de pruebas de calidad de datos
Pipeline: rfm (Magento Stage tables -> Glue Job -> rfm_unified Analytics)
Ejecuta 15 queries SQL de validacion sobre tablas Stage y Analytics.
Criterio: 0 filas = PASS, >0 filas = FAIL

Nota: Las pruebas de requisitos no funcionales (16-22) no se incluyen en este
pipeline porque requieren inspeccion de codigo fuente, Amazon CloudWatch Logs,
AWS Systems Manager Parameter Store y Amazon S3, no consultas SQL sobre datos.
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
DATABASE_STAGE = os.environ['GLUE_DATABASE_STAGE']
DATABASE_ANALYTICS = os.environ['GLUE_DATABASE_ANALYTICS']
ATHENA_OUTPUT = os.environ['ATHENA_OUTPUT']
ATHENA_WORKGROUP = os.environ['ATHENA_WORKGROUP']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

PIPELINE = 'rfm'
QUERY_TIMEOUT = 120
POLL_INTERVAL = 2

athena = boto3.client('athena')
sns = boto3.client('sns')


# ─── Definicion de las 15 pruebas ──────────────────────────────────────────────
# {stage} se reemplaza con DATABASE_STAGE en tiempo de ejecucion.
# {analytics} se reemplaza con DATABASE_ANALYTICS en tiempo de ejecucion.
# Las subconsultas (SELECT MAX(dt) FROM ...) se reemplazan con el dt literal
# si se proporciona el parametro dt.
# pre_wrapped=True indica que la query ya incluye el COUNT(*) wrapper (para CTEs).
# no_dt=True indica que la prueba opera sobre multiples particiones y no debe
# recibir sustitucion de dt.

TESTS = [

    # ══════════════════════════════════════════════════════════════════════════
    # VALIDACION DE DATOS
    # ══════════════════════════════════════════════════════════════════════════

    # ── Prueba 1: Campos obligatorios nulos en tablas de ordenes (Stage) ────
    {
        "id": 1,
        "name": "Campos obligatorios nulos en tablas de ordenes",
        "query": (
            "SELECT 'cc_sales_order' AS tabla_origen, "
            "entity_id, status, created_at, base_subtotal, "
            "base_discount_amount, base_shipping_amount, customer_id "
            "FROM {stage}.cc_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.cc_sales_order) "
            "AND (entity_id IS NULL OR status IS NULL OR created_at IS NULL "
            "OR base_subtotal IS NULL OR base_discount_amount IS NULL "
            "OR base_shipping_amount IS NULL OR customer_id IS NULL) "
            "UNION ALL "
            "SELECT 'jmb_sales_order', CAST(entity_id AS INT), status, created_at, "
            "base_subtotal, base_discount_amount, base_shipping_amount, customer_id "
            "FROM {stage}.jmb_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.jmb_sales_order) "
            "AND (entity_id IS NULL OR status IS NULL OR created_at IS NULL "
            "OR base_subtotal IS NULL OR base_discount_amount IS NULL "
            "OR base_shipping_amount IS NULL OR customer_id IS NULL) "
            "UNION ALL "
            "SELECT 'bbmjgt_sales_order', CAST(entity_id AS INT), status, created_at, "
            "base_subtotal, base_discount_amount, base_shipping_amount, "
            "CAST(customer_id AS DOUBLE) "
            "FROM {stage}.bbmjgt_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.bbmjgt_sales_order) "
            "AND (entity_id IS NULL OR status IS NULL OR created_at IS NULL "
            "OR base_subtotal IS NULL OR base_discount_amount IS NULL "
            "OR base_shipping_amount IS NULL OR customer_id IS NULL) "
            "UNION ALL "
            "SELECT 'smn_sales_order', CAST(entity_id AS INT), status, created_at, "
            "base_subtotal, base_discount_amount, base_shipping_amount, customer_id "
            "FROM {stage}.smn_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.smn_sales_order) "
            "AND (entity_id IS NULL OR status IS NULL OR created_at IS NULL "
            "OR base_subtotal IS NULL OR base_discount_amount IS NULL "
            "OR base_shipping_amount IS NULL OR customer_id IS NULL)"
        )
    },

    # ── Prueba 2: Presencia de ordenes entregadas por tabla (Stage) ─────────
    {
        "id": 2,
        "name": "Tablas de ordenes sin registros delivered",
        "pre_wrapped": True,
        "query": (
            "WITH conteos AS ("
            "SELECT 'cc_sales_order' AS tabla_origen, "
            "COUNT(*) AS total_delivered "
            "FROM {stage}.cc_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.cc_sales_order) "
            "AND status = 'delivered' "
            "UNION ALL "
            "SELECT 'jmb_sales_order', COUNT(*) "
            "FROM {stage}.jmb_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.jmb_sales_order) "
            "AND status = 'delivered' "
            "UNION ALL "
            "SELECT 'bbmjgt_sales_order', COUNT(*) "
            "FROM {stage}.bbmjgt_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.bbmjgt_sales_order) "
            "AND status = 'delivered' "
            "UNION ALL "
            "SELECT 'smn_sales_order', COUNT(*) "
            "FROM {stage}.smn_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.smn_sales_order) "
            "AND status = 'delivered'"
            ") "
            "SELECT COUNT(*) AS error_count FROM conteos "
            "WHERE total_delivered = 0"
        )
    },

    # ── Prueba 3: Valores monetarios negativos en ordenes entregadas (Stage) ─
    {
        "id": 3,
        "name": "Valores monetarios negativos en ordenes entregadas",
        "query": (
            "SELECT 'cc_sales_order' AS tabla_origen, CAST(entity_id AS VARCHAR), base_subtotal, "
            "base_discount_amount, base_bin_discount_amount, base_shipping_amount "
            "FROM {stage}.cc_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.cc_sales_order) "
            "AND status = 'delivered' "
            "AND (base_subtotal < 0 OR base_discount_amount < 0 "
            "OR base_bin_discount_amount < 0 OR base_shipping_amount < 0) "
            "UNION ALL "
            "SELECT 'jmb_sales_order', CAST(entity_id AS VARCHAR), base_subtotal, "
            "base_discount_amount, base_bin_discount_amount, base_shipping_amount "
            "FROM {stage}.jmb_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.jmb_sales_order) "
            "AND status = 'delivered' "
            "AND (base_subtotal < 0 OR base_discount_amount < 0 "
            "OR base_bin_discount_amount < 0 OR base_shipping_amount < 0) "
            "UNION ALL "
            "SELECT 'bbmjgt_sales_order', CAST(entity_id AS VARCHAR), base_subtotal, "
            "base_discount_amount, "
            "TRY_CAST(base_bin_discount_amount AS DOUBLE), "
            "base_shipping_amount "
            "FROM {stage}.bbmjgt_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.bbmjgt_sales_order) "
            "AND status = 'delivered' "
            "AND (base_subtotal < 0 OR base_discount_amount < 0 "
            "OR TRY_CAST(base_bin_discount_amount AS DOUBLE) < 0 "
            "OR base_shipping_amount < 0) "
            "UNION ALL "
            "SELECT 'smn_sales_order', CAST(entity_id AS VARCHAR), base_subtotal, "
            "base_discount_amount, base_bin_discount_amount, base_shipping_amount "
            "FROM {stage}.smn_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.smn_sales_order) "
            "AND status = 'delivered' "
            "AND (base_subtotal < 0 OR base_discount_amount < 0 "
            "OR base_bin_discount_amount < 0 OR base_shipping_amount < 0)"
        )
    },

    # ── Prueba 4: store_id nulo en bbmjgt_sales_order (Stage) ──────────────
    {
        "id": 4,
        "name": "Campo store_id nulo en bbmjgt_sales_order",
        "query": (
            "SELECT entity_id, store_id, status "
            "FROM {stage}.bbmjgt_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.bbmjgt_sales_order) "
            "AND store_id IS NULL"
        )
    },

    # ── Prueba 5: Campos obligatorios nulos en rfm_unified (Analytics) ─────
    {
        "id": 5,
        "name": "Campos obligatorios nulos en rfm_unified",
        "query": (
            "SELECT customer_id, customer_type, total_orders_history, "
            "last_purchase_date, date_measurement, brand, market, dt "
            "FROM {analytics}.rfm_unified "
            "WHERE dt = (SELECT MAX(dt) FROM {analytics}.rfm_unified) "
            "AND (customer_id IS NULL OR customer_type IS NULL "
            "OR total_orders_history IS NULL OR last_purchase_date IS NULL "
            "OR date_measurement IS NULL OR brand IS NULL OR market IS NULL)"
        )
    },

    # ── Prueba 6: Dominio de customer_type en rfm_unified (Analytics) ──────
    {
        "id": 6,
        "name": "Dominio de customer_type en rfm_unified",
        "query": (
            "SELECT customer_id, brand, market, customer_type, dt "
            "FROM {analytics}.rfm_unified "
            "WHERE dt = (SELECT MAX(dt) FROM {analytics}.rfm_unified) "
            "AND customer_type NOT IN ('new', 'recurring', 'reactivation', 'customer_on_hold')"
        )
    },

    # ══════════════════════════════════════════════════════════════════════════
    # CALIDAD DE DATOS
    # ══════════════════════════════════════════════════════════════════════════

    # ── Prueba 7: Dominio de status en tablas de ordenes (Stage) ───────────
    {
        "id": 7,
        "name": "Dominio de status en tablas de ordenes",
        "query": (
            "SELECT 'cc_sales_order' AS tabla_origen, status, COUNT(*) AS cantidad "
            "FROM {stage}.cc_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.cc_sales_order) "
            "AND status NOT IN ('delivered', 'canceled', 'pending', 'processing') "
            "GROUP BY status "
            "UNION ALL "
            "SELECT 'jmb_sales_order', status, COUNT(*) "
            "FROM {stage}.jmb_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.jmb_sales_order) "
            "AND status NOT IN ('delivered', 'canceled', 'pending', 'processing') "
            "GROUP BY status "
            "UNION ALL "
            "SELECT 'bbmjgt_sales_order', status, COUNT(*) "
            "FROM {stage}.bbmjgt_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.bbmjgt_sales_order) "
            "AND status NOT IN ('delivered', 'canceled', 'pending', 'processing') "
            "GROUP BY status "
            "UNION ALL "
            "SELECT 'smn_sales_order', status, COUNT(*) "
            "FROM {stage}.smn_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.smn_sales_order) "
            "AND status NOT IN ('delivered', 'canceled', 'pending', 'processing') "
            "GROUP BY status"
        )
    },

    # ── Prueba 8: Fechas de creacion no futuras en tablas de ordenes (Stage) ─
    {
        "id": 8,
        "name": "Fechas de creacion futuras en tablas de ordenes",
        "query": (
            "SELECT 'cc_sales_order' AS tabla_origen, "
            "CAST(entity_id AS VARCHAR) AS entity_id, created_at "
            "FROM {stage}.cc_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.cc_sales_order) "
            "AND created_at > CURRENT_TIMESTAMP "
            "UNION ALL "
            "SELECT 'jmb_sales_order', CAST(entity_id AS VARCHAR), created_at "
            "FROM {stage}.jmb_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.jmb_sales_order) "
            "AND created_at > CURRENT_TIMESTAMP "
            "UNION ALL "
            "SELECT 'bbmjgt_sales_order', CAST(entity_id AS VARCHAR), created_at "
            "FROM {stage}.bbmjgt_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.bbmjgt_sales_order) "
            "AND created_at > CURRENT_TIMESTAMP "
            "UNION ALL "
            "SELECT 'smn_sales_order', CAST(entity_id AS VARCHAR), created_at "
            "FROM {stage}.smn_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.smn_sales_order) "
            "AND created_at > CURRENT_TIMESTAMP"
        )
    },

    # ── Prueba 9: Dominio de store_id en bbmjgt_sales_order (Stage) ────────
    {
        "id": 9,
        "name": "Dominio de store_id en bbmjgt_sales_order",
        "query": (
            "SELECT store_id, COUNT(*) AS cantidad "
            "FROM {stage}.bbmjgt_sales_order "
            "WHERE dt = (SELECT MAX(dt) FROM {stage}.bbmjgt_sales_order) "
            "AND store_id NOT IN (1, 3) "
            "GROUP BY store_id "
            "ORDER BY cantidad DESC"
        )
    },

    # ── Prueba 10: Rango de rfm_group en rfm_unified (Analytics) ───────────
    {
        "id": 10,
        "name": "Rango de rfm_group en rfm_unified",
        "query": (
            "SELECT brand, market, rfm_group, COUNT(*) AS cantidad "
            "FROM {analytics}.rfm_unified "
            "WHERE dt = (SELECT MAX(dt) FROM {analytics}.rfm_unified) "
            "AND rfm_group IS NOT NULL "
            "AND rfm_group NOT BETWEEN -2 AND 10 "
            "GROUP BY brand, market, rfm_group "
            "ORDER BY brand, rfm_group"
        )
    },

    # ── Prueba 11: Uniformidad de tipos de dato entre particiones (Analytics)
    {
        "id": 11,
        "name": "Uniformidad de tipos de dato entre particiones en rfm_unified",
        "no_dt": True,
        "query": (
            "SELECT column_name, "
            "COUNT(DISTINCT data_type) AS tipos_distintos, "
            "array_join(array_agg(DISTINCT data_type), ', ') AS tipos_encontrados "
            "FROM information_schema.columns "
            "WHERE table_name = 'rfm_unified' "
            "GROUP BY column_name "
            "HAVING COUNT(DISTINCT data_type) > 1 "
            "ORDER BY column_name"
        )
    },

    # ── Prueba 12: Clientes new/on_hold con campos RFM del periodo actual (Analytics)
    {
        "id": 12,
        "name": "Clientes new/on_hold con campos RFM del periodo actual poblados",
        "query": (
            "SELECT customer_id, brand, market, customer_type, "
            "recency, frequency, monetary, rfm_segment, "
            "r_quantile, f_quantile, m_quantile, rfm_score "
            "FROM {analytics}.rfm_unified "
            "WHERE dt = (SELECT MAX(dt) FROM {analytics}.rfm_unified) "
            "AND customer_type IN ('new', 'customer_on_hold') "
            "AND (recency IS NOT NULL OR frequency IS NOT NULL "
            "OR monetary IS NOT NULL OR rfm_segment IS NOT NULL "
            "OR r_quantile IS NOT NULL OR f_quantile IS NOT NULL "
            "OR m_quantile IS NOT NULL OR rfm_score IS NOT NULL)"
        )
    },

    # ══════════════════════════════════════════════════════════════════════════
    # INTEGRIDAD DE DATOS
    # ══════════════════════════════════════════════════════════════════════════

    # ── Prueba 13: Presencia de todos los mercados en rfm_unified (Analytics)
    {
        "id": 13,
        "name": "Mercados ausentes en rfm_unified",
        "pre_wrapped": True,
        "no_dt": True,
        "query": (
            "WITH mercados_esperados AS ("
            "SELECT 'cc' AS market "
            "UNION ALL SELECT 'jmb' "
            "UNION ALL SELECT 'bbmjgt' "
            "UNION ALL SELECT 'nacional'"
            "), "
            "mercados_presentes AS ("
            "SELECT DISTINCT market "
            "FROM {analytics}.rfm_unified "
            "WHERE dt = (SELECT MAX(dt) FROM {analytics}.rfm_unified)"
            ") "
            "SELECT COUNT(*) AS error_count "
            "FROM mercados_esperados e "
            "LEFT JOIN mercados_presentes p ON e.market = p.market "
            "WHERE p.market IS NULL"
        )
    },

    # ── Prueba 14: Completitud de mercados por particion (Analytics) ───────
    {
        "id": 14,
        "name": "Particiones con mercados faltantes en rfm_unified",
        "no_dt": True,
        "query": (
            "SELECT dt, "
            "COUNT(DISTINCT market) AS mercados_presentes, "
            "array_join(array_agg(DISTINCT market ORDER BY market), ', ') AS lista_mercados "
            "FROM {analytics}.rfm_unified "
            "GROUP BY dt "
            "HAVING COUNT(DISTINCT market) < 4 "
            "ORDER BY dt DESC"
        )
    },

    # ── Prueba 15: Clientes recurrentes/reactivacion sin rfm_group (Analytics)
    {
        "id": 15,
        "name": "Clientes recurrentes/reactivacion sin rfm_group",
        "query": (
            "SELECT customer_id, brand, market, customer_type, "
            "rfm_score, rfm_segment, rfm_group "
            "FROM {analytics}.rfm_unified "
            "WHERE dt = (SELECT MAX(dt) FROM {analytics}.rfm_unified) "
            "AND customer_type IN ('recurring', 'reactivation') "
            "AND rfm_group IS NULL"
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
            'Database': DATABASE_ANALYTICS,
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
    msg.append('ERROR EN PRUEBAS DE CALIDAD - PIPELINE RFM')
    msg.append(separator)
    msg.append('')
    msg.append(f'Fecha/Hora: {now}')
    msg.append('')

    # ── Informacion del pipeline ──
    msg.append(line)
    msg.append('INFORMACION DEL PIPELINE')
    msg.append(line)
    msg.append(f'Pipeline:            {PIPELINE}')
    msg.append(f'Database Stage:      {DATABASE_STAGE}')
    msg.append(f'Database Analytics:  {DATABASE_ANALYTICS}')
    msg.append(f'Athena Output:       {ATHENA_OUTPUT}')
    msg.append(f'Workgroup:           {ATHENA_WORKGROUP}')
    msg.append(f'Particion (dt):      {dt_display}')
    msg.append(f'Request ID:          {request_id}')
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
        "database_stage": DATABASE_STAGE,
        "database_analytics": DATABASE_ANALYTICS,
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
            # Construir query: reemplazar placeholders de ambas databases
            query = test['query'].format(
                stage=DATABASE_STAGE,
                analytics=DATABASE_ANALYTICS
            )

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
        'database_stage': DATABASE_STAGE,
        'database_analytics': DATABASE_ANALYTICS,
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