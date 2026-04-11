"""
dl-icomm-tests | Lambda de pruebas de calidad de datos
Pipeline: icomm (Surveyicom -> S3 -> Athena)
Ejecuta 22 queries SQL de validacion sobre tablas transformadas.
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

PIPELINE = 'icomm'
QUERY_TIMEOUT = 120
POLL_INTERVAL = 2

athena = boto3.client('athena')
sns = boto3.client('sns')


# ─── Definicion de las 22 pruebas ──────────────────────────────────────────────
# {db} se reemplaza con DATABASE en tiempo de ejecucion.
# Las subconsultas (SELECT MAX(dt) FROM ...) se reemplazan con el dt literal
# si se proporciona el parametro dt.
# pre_wrapped=True indica que la query ya incluye el COUNT(*) wrapper (para CTEs).

TESTS = [
    {
        "id": 1,
        "name": "Campos obligatorios nulos en campaigns_filtered",
        "query": (
            "SELECT 'nacional_campaigns_filtered' AS source_table, * "
            "FROM {db}.nacional_campaigns_filtered "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_campaigns_filtered) "
            "AND (id IS NULL OR TRIM(id) = '' "
            "OR project_name IS NULL OR TRIM(project_name) = '') "
            "UNION ALL "
            "SELECT 'cc_campaigns_filtered' AS source_table, * "
            "FROM {db}.cc_campaigns_filtered "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_campaigns_filtered) "
            "AND (id IS NULL OR TRIM(id) = '' "
            "OR project_name IS NULL OR TRIM(project_name) = '')"
        )
    },
    {
        "id": 2,
        "name": "Valores invalidos de state en campaigns_filtered",
        "query": (
            "SELECT 'nacional_campaigns_filtered' AS source_table, * "
            "FROM {db}.nacional_campaigns_filtered "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_campaigns_filtered) "
            "AND LOWER(TRIM(state)) NOT IN ('active', 'inactive', 'draft', 'closed', 'published') "
            "UNION ALL "
            "SELECT 'cc_campaigns_filtered' AS source_table, * "
            "FROM {db}.cc_campaigns_filtered "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_campaigns_filtered) "
            "AND LOWER(TRIM(state)) NOT IN ('active', 'inactive', 'draft', 'closed', 'published')"
        )
    },
    {
        "id": 3,
        "name": "updated_at anterior a created_at en campaigns_filtered",
        "query": (
            "SELECT 'nacional_campaigns_filtered' AS source_table, * "
            "FROM {db}.nacional_campaigns_filtered "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_campaigns_filtered) "
            "AND updated_at < created_at "
            "UNION ALL "
            "SELECT 'cc_campaigns_filtered' AS source_table, * "
            "FROM {db}.cc_campaigns_filtered "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_campaigns_filtered) "
            "AND updated_at < created_at"
        )
    },
    {
        "id": 4,
        "name": "Preguntas huerfanas sin campana vinculada",
        "query": (
            "SELECT 'nacional_campaigns_questions' AS source_table, q.* "
            "FROM {db}.nacional_campaigns_questions q "
            "LEFT JOIN {db}.nacional_campaigns_filtered c "
            "ON q.campaign_id = c.id "
            "AND c.dt = (SELECT MAX(dt) FROM {db}.nacional_campaigns_filtered) "
            "WHERE q.dt = (SELECT MAX(dt) FROM {db}.nacional_campaigns_questions) "
            "AND c.id IS NULL "
            "UNION ALL "
            "SELECT 'cc_campaigns_questions' AS source_table, q.* "
            "FROM {db}.cc_campaigns_questions q "
            "LEFT JOIN {db}.cc_campaigns_filtered c "
            "ON q.campaign_id = c.id "
            "AND c.dt = (SELECT MAX(dt) FROM {db}.cc_campaigns_filtered) "
            "WHERE q.dt = (SELECT MAX(dt) FROM {db}.cc_campaigns_questions) "
            "AND c.id IS NULL"
        )
    },
    {
        "id": 5,
        "name": "Duplicados campaign_id + question_id en campaigns_questions",
        "query": (
            "SELECT 'nacional_campaigns_questions' AS source_table, campaign_id, question_id, COUNT(*) AS total "
            "FROM {db}.nacional_campaigns_questions "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_campaigns_questions) "
            "GROUP BY campaign_id, question_id HAVING COUNT(*) > 1 "
            "UNION ALL "
            "SELECT 'cc_campaigns_questions' AS source_table, campaign_id, question_id, COUNT(*) AS total "
            "FROM {db}.cc_campaigns_questions "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_campaigns_questions) "
            "GROUP BY campaign_id, question_id HAVING COUNT(*) > 1"
        )
    },
    {
        "id": 6,
        "name": "participant_id nulo en answers_flat",
        "query": (
            "SELECT 'nacional_answers_flat' AS source_table, * "
            "FROM {db}.nacional_answers_flat "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_answers_flat) "
            "AND (participant_id IS NULL OR TRIM(participant_id) = '') "
            "UNION ALL "
            "SELECT 'cc_answers_flat' AS source_table, * "
            "FROM {db}.cc_answers_flat "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_answers_flat) "
            "AND (participant_id IS NULL OR TRIM(participant_id) = '')"
        )
    },
    {
        "id": 7,
        "name": "question_id huerfano en answers_flat",
        "query": (
            "SELECT 'nacional_answers_flat' AS source_table, f.* "
            "FROM {db}.nacional_answers_flat f "
            "LEFT JOIN (SELECT DISTINCT question_id FROM {db}.nacional_campaigns_questions) q "
            "ON f.question_id = q.question_id "
            "WHERE f.dt = (SELECT MAX(dt) FROM {db}.nacional_answers_flat) "
            "AND q.question_id IS NULL "
            "UNION ALL "
            "SELECT 'cc_answers_flat' AS source_table, f.* "
            "FROM {db}.cc_answers_flat f "
            "LEFT JOIN (SELECT DISTINCT question_id FROM {db}.cc_campaigns_questions) q "
            "ON f.question_id = q.question_id "
            "WHERE f.dt = (SELECT MAX(dt) FROM {db}.cc_answers_flat) "
            "AND q.question_id IS NULL"
        )
    },
    {
        "id": 8,
        "name": "Participantes inconsistentes entre answers_flat y answers_wide",
        "query": (
            "SELECT DISTINCT 'nacional' AS dataset, participant_id, 'en flat, no en wide' AS origen "
            "FROM {db}.nacional_answers_flat "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_answers_flat) "
            "AND participant_id NOT IN ("
            "SELECT DISTINCT participant_id FROM {db}.nacional_answers_wide "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_answers_wide) "
            "AND participant_id IS NOT NULL) "
            "UNION ALL "
            "SELECT DISTINCT 'nacional' AS dataset, participant_id, 'en wide, no en flat' AS origen "
            "FROM {db}.nacional_answers_wide "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_answers_wide) "
            "AND participant_id NOT IN ("
            "SELECT DISTINCT participant_id FROM {db}.nacional_answers_flat "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_answers_flat) "
            "AND participant_id IS NOT NULL) "
            "UNION ALL "
            "SELECT DISTINCT 'cc' AS dataset, participant_id, 'en flat, no en wide' AS origen "
            "FROM {db}.cc_answers_flat "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_answers_flat) "
            "AND participant_id NOT IN ("
            "SELECT DISTINCT participant_id FROM {db}.cc_answers_wide "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_answers_wide) "
            "AND participant_id IS NOT NULL) "
            "UNION ALL "
            "SELECT DISTINCT 'cc' AS dataset, participant_id, 'en wide, no en flat' AS origen "
            "FROM {db}.cc_answers_wide "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_answers_wide) "
            "AND participant_id NOT IN ("
            "SELECT DISTINCT participant_id FROM {db}.cc_answers_flat "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_answers_flat) "
            "AND participant_id IS NOT NULL)"
        )
    },
    {
        "id": 9,
        "name": "Campos obligatorios nulos en campaigns_questions",
        "query": (
            "SELECT 'nacional_campaigns_questions' AS source_table, * "
            "FROM {db}.nacional_campaigns_questions "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_campaigns_questions) "
            "AND (campaign_id IS NULL OR TRIM(campaign_id) = '' "
            "OR question_id IS NULL OR TRIM(question_id) = '' "
            "OR question IS NULL OR TRIM(question) = '' "
            "OR question_type IS NULL OR TRIM(question_type) = '') "
            "UNION ALL "
            "SELECT 'cc_campaigns_questions' AS source_table, * "
            "FROM {db}.cc_campaigns_questions "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_campaigns_questions) "
            "AND (campaign_id IS NULL OR TRIM(campaign_id) = '' "
            "OR question_id IS NULL OR TRIM(question_id) = '' "
            "OR question IS NULL OR TRIM(question) = '' "
            "OR question_type IS NULL OR TRIM(question_type) = '')"
        )
    },
    {
        "id": 10,
        "name": "Tipos de pregunta invalidos en campaigns_questions",
        "query": (
            "SELECT 'nacional_campaigns_questions' AS source_table, * "
            "FROM {db}.nacional_campaigns_questions "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_campaigns_questions) "
            "AND LOWER(TRIM(question_type)) NOT IN ("
            "'single_choice', 'multiple_choice', 'multiple_picture', 'open', "
            "'nps', 'rating', 'matrix', 'date', 'number', 'dropdown', "
            "'score', 'short_answer') "
            "UNION ALL "
            "SELECT 'cc_campaigns_questions' AS source_table, * "
            "FROM {db}.cc_campaigns_questions "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_campaigns_questions) "
            "AND LOWER(TRIM(question_type)) NOT IN ("
            "'single_choice', 'multiple_choice', 'multiple_picture', 'open', "
            "'nps', 'rating', 'matrix', 'date', 'number', 'dropdown', "
            "'score', 'short_answer')"
        )
    },
    {
        "id": 11,
        "name": "Campos obligatorios nulos en answers_flat",
        "query": (
            "SELECT 'nacional_answers_flat' AS source_table, * "
            "FROM {db}.nacional_answers_flat "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_answers_flat) "
            "AND (participant_id IS NULL OR TRIM(participant_id) = '' "
            "OR question_id IS NULL OR TRIM(question_id) = '' "
            "OR question IS NULL OR TRIM(question) = '' "
            "OR answer_index IS NULL) "
            "UNION ALL "
            "SELECT 'cc_answers_flat' AS source_table, * "
            "FROM {db}.cc_answers_flat "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_answers_flat) "
            "AND (participant_id IS NULL OR TRIM(participant_id) = '' "
            "OR question_id IS NULL OR TRIM(question_id) = '' "
            "OR question IS NULL OR TRIM(question) = '' "
            "OR answer_index IS NULL)"
        )
    },
    {
        "id": 12,
        "name": "finished_at anterior a created_at en answers_flat",
        "query": (
            "SELECT 'nacional_answers_flat' AS source_table, * "
            "FROM {db}.nacional_answers_flat "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_answers_flat) "
            "AND finished_at < created_at "
            "UNION ALL "
            "SELECT 'cc_answers_flat' AS source_table, * "
            "FROM {db}.cc_answers_flat "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_answers_flat) "
            "AND finished_at < created_at"
        )
    },
    {
        "id": 13,
        "name": "Respuestas huerfanas sin campana vinculada",
        "query": (
            "SELECT 'nacional_answers_flat' AS source_table, f.* "
            "FROM {db}.nacional_answers_flat f "
            "LEFT JOIN {db}.nacional_campaigns_questions q ON f.question_id = q.question_id "
            "LEFT JOIN {db}.nacional_campaigns_filtered c ON q.campaign_id = c.id "
            "WHERE f.dt = (SELECT MAX(dt) FROM {db}.nacional_answers_flat) "
            "AND c.id IS NULL "
            "UNION ALL "
            "SELECT 'cc_answers_flat' AS source_table, f.* "
            "FROM {db}.cc_answers_flat f "
            "LEFT JOIN {db}.cc_campaigns_questions q ON f.question_id = q.question_id "
            "LEFT JOIN {db}.cc_campaigns_filtered c ON q.campaign_id = c.id "
            "WHERE f.dt = (SELECT MAX(dt) FROM {db}.cc_answers_flat) "
            "AND c.id IS NULL"
        )
    },
    {
        "id": 14,
        "name": "Campos obligatorios nulos en answers_wide",
        "query": (
            "SELECT 'nacional_answers_wide' AS source_table, * "
            "FROM {db}.nacional_answers_wide "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_answers_wide) "
            "AND (participant_id IS NULL OR TRIM(CAST(participant_id AS VARCHAR)) = '' "
            "OR created_at IS NULL) "
            "UNION ALL "
            "SELECT 'cc_answers_wide' AS source_table, * "
            "FROM {db}.cc_answers_wide "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_answers_wide) "
            "AND (participant_id IS NULL OR TRIM(CAST(participant_id AS VARCHAR)) = '' "
            "OR created_at IS NULL)"
        )
    },
    {
        "id": 15,
        "name": "Participantes duplicados en answers_wide",
        "query": (
            "SELECT 'nacional_answers_wide' AS source_table, participant_id, COUNT(*) AS total "
            "FROM {db}.nacional_answers_wide "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_answers_wide) "
            "GROUP BY participant_id HAVING COUNT(*) > 1 "
            "UNION ALL "
            "SELECT 'cc_answers_wide' AS source_table, participant_id, COUNT(*) AS total "
            "FROM {db}.cc_answers_wide "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_answers_wide) "
            "GROUP BY participant_id HAVING COUNT(*) > 1"
        )
    },
    {
        "id": 16,
        "name": "Diferencia de participantes entre flat y wide",
        "no_dt": True,
        "pre_wrapped": True,
        "query": (
            "WITH _diff AS ("
            "SELECT 'nacional' AS dataset, "
            "(SELECT COUNT(DISTINCT participant_id) FROM {db}.nacional_answers_flat "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_answers_flat)) - "
            "(SELECT COUNT(DISTINCT participant_id) FROM {db}.nacional_answers_wide "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_answers_wide)) "
            "AS diferencia_participantes "
            "UNION ALL "
            "SELECT 'cc' AS dataset, "
            "(SELECT COUNT(DISTINCT participant_id) FROM {db}.cc_answers_flat "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_answers_flat)) - "
            "(SELECT COUNT(DISTINCT participant_id) FROM {db}.cc_answers_wide "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_answers_wide)) "
            "AS diferencia_participantes"
            ") "
            "SELECT COUNT(*) AS error_count FROM _diff WHERE diferencia_participantes != 0"
        )
    },
    {
        "id": 17,
        "name": "Formatos de fecha invalidos en answers_flat",
        "query": (
            "SELECT 'nacional_answers_flat' AS source_table, * "
            "FROM {db}.nacional_answers_flat "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_answers_flat) "
            "AND ((created_at IS NOT NULL AND TRY(CAST(created_at AS TIMESTAMP)) IS NULL) "
            "OR (finished_at IS NOT NULL AND TRY(CAST(finished_at AS TIMESTAMP)) IS NULL)) "
            "UNION ALL "
            "SELECT 'cc_answers_flat' AS source_table, * "
            "FROM {db}.cc_answers_flat "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_answers_flat) "
            "AND ((created_at IS NOT NULL AND TRY(CAST(created_at AS TIMESTAMP)) IS NULL) "
            "OR (finished_at IS NOT NULL AND TRY(CAST(finished_at AS TIMESTAMP)) IS NULL))"
        )
    },
    {
        "id": 18,
        "name": "Tipos de datos mixtos en columnas de answers_wide",
        "no_dt": True,
        "query": (
            "SELECT 'nacional_answers_wide' AS table_name, column_name, "
            "COUNT(DISTINCT data_type) AS tipos_distintos, "
            "ARRAY_AGG(DISTINCT data_type) AS tipos_encontrados "
            "FROM information_schema.columns "
            "WHERE table_schema = '{db}' AND table_name = 'nacional_answers_wide' "
            "GROUP BY column_name HAVING COUNT(DISTINCT data_type) > 1 "
            "UNION ALL "
            "SELECT 'cc_answers_wide' AS table_name, column_name, "
            "COUNT(DISTINCT data_type) AS tipos_distintos, "
            "ARRAY_AGG(DISTINCT data_type) AS tipos_encontrados "
            "FROM information_schema.columns "
            "WHERE table_schema = '{db}' AND table_name = 'cc_answers_wide' "
            "GROUP BY column_name HAVING COUNT(DISTINCT data_type) > 1"
        )
    },
    {
        "id": 19,
        "name": "Particiones inconsistentes entre answers_flat y answers_wide",
        "no_dt": True,
        "pre_wrapped": True,
        "query": (
            "WITH particiones_nacional AS ("
            "SELECT DISTINCT dt, 'answers_flat' AS tabla FROM {db}.nacional_answers_flat "
            "UNION ALL SELECT DISTINCT dt, 'answers_wide' FROM {db}.nacional_answers_wide), "
            "particiones_cc AS ("
            "SELECT DISTINCT dt, 'answers_flat' AS tabla FROM {db}.cc_answers_flat "
            "UNION ALL SELECT DISTINCT dt, 'answers_wide' FROM {db}.cc_answers_wide), "
            "todas_fechas_nacional AS (SELECT DISTINCT dt FROM particiones_nacional), "
            "todas_fechas_cc AS (SELECT DISTINCT dt FROM particiones_cc), "
            "_test AS ("
            "SELECT 'nacional' AS dataset, f.dt, p.tabla "
            "FROM todas_fechas_nacional f "
            "CROSS JOIN (SELECT DISTINCT tabla FROM particiones_nacional) p "
            "LEFT JOIN particiones_nacional p2 ON f.dt = p2.dt AND p.tabla = p2.tabla "
            "WHERE p2.dt IS NULL "
            "UNION ALL "
            "SELECT 'cc' AS dataset, f.dt, p.tabla "
            "FROM todas_fechas_cc f "
            "CROSS JOIN (SELECT DISTINCT tabla FROM particiones_cc) p "
            "LEFT JOIN particiones_cc p2 ON f.dt = p2.dt AND p.tabla = p2.tabla "
            "WHERE p2.dt IS NULL) "
            "SELECT COUNT(*) AS error_count FROM _test"
        )
    },
    {
        "id": 20,
        "name": "IDs no numericos en campaigns_filtered",
        "query": (
            "SELECT 'nacional_campaigns_filtered' AS source_table, * "
            "FROM {db}.nacional_campaigns_filtered "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_campaigns_filtered) "
            "AND NOT REGEXP_LIKE(TRIM(id), '^\\d+$') "
            "UNION ALL "
            "SELECT 'cc_campaigns_filtered' AS source_table, * "
            "FROM {db}.cc_campaigns_filtered "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_campaigns_filtered) "
            "AND NOT REGEXP_LIKE(TRIM(id), '^\\d+$')"
        )
    },
    {
        "id": 21,
        "name": "Fechas created_at fuera de rango en campaigns_filtered",
        "query": (
            "SELECT 'nacional_campaigns_filtered' AS source_table, * "
            "FROM {db}.nacional_campaigns_filtered "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_campaigns_filtered) "
            "AND (created_at > CURRENT_TIMESTAMP OR created_at < TIMESTAMP '2020-01-01 00:00:00') "
            "UNION ALL "
            "SELECT 'cc_campaigns_filtered' AS source_table, * "
            "FROM {db}.cc_campaigns_filtered "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_campaigns_filtered) "
            "AND (created_at > CURRENT_TIMESTAMP OR created_at < TIMESTAMP '2020-01-01 00:00:00')"
        )
    },
    {
        "id": 22,
        "name": "Participantes con mas respuestas que preguntas por campana",
        "pre_wrapped": True,
        "query": (
            "WITH respuestas_por_participante_nacional AS ("
            "SELECT f.participant_id, q.campaign_id, COUNT(*) AS total_respuestas "
            "FROM {db}.nacional_answers_flat f "
            "JOIN {db}.nacional_campaigns_questions q ON f.question_id = q.question_id "
            "WHERE f.dt = (SELECT MAX(dt) FROM {db}.nacional_answers_flat) "
            "GROUP BY f.participant_id, q.campaign_id), "
            "preguntas_por_campana_nacional AS ("
            "SELECT campaign_id, COUNT(DISTINCT question_id) AS total_preguntas "
            "FROM {db}.nacional_campaigns_questions "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.nacional_campaigns_questions) "
            "GROUP BY campaign_id), "
            "respuestas_por_participante_cc AS ("
            "SELECT f.participant_id, q.campaign_id, COUNT(*) AS total_respuestas "
            "FROM {db}.cc_answers_flat f "
            "JOIN {db}.cc_campaigns_questions q ON f.question_id = q.question_id "
            "WHERE f.dt = (SELECT MAX(dt) FROM {db}.cc_answers_flat) "
            "GROUP BY f.participant_id, q.campaign_id), "
            "preguntas_por_campana_cc AS ("
            "SELECT campaign_id, COUNT(DISTINCT question_id) AS total_preguntas "
            "FROM {db}.cc_campaigns_questions "
            "WHERE dt = (SELECT MAX(dt) FROM {db}.cc_campaigns_questions) "
            "GROUP BY campaign_id), "
            "_test AS ("
            "SELECT 'nacional' AS dataset, r.participant_id, r.campaign_id, "
            "r.total_respuestas, p.total_preguntas "
            "FROM respuestas_por_participante_nacional r "
            "JOIN preguntas_por_campana_nacional p ON r.campaign_id = p.campaign_id "
            "WHERE r.total_respuestas > p.total_preguntas "
            "UNION ALL "
            "SELECT 'cc' AS dataset, r.participant_id, r.campaign_id, "
            "r.total_respuestas, p.total_preguntas "
            "FROM respuestas_por_participante_cc r "
            "JOIN preguntas_por_campana_cc p ON r.campaign_id = p.campaign_id "
            "WHERE r.total_respuestas > p.total_preguntas) "
            "SELECT COUNT(*) AS error_count FROM _test"
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
    msg.append('ERROR EN PRUEBAS DE CALIDAD - PIPELINE ICOMM')
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
