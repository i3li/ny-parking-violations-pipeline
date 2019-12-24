from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadTableOperator
from airflow.operators import DataQualityOperator
from helpers import SqlQueries


s3_bucket = Variable.get('s3_violations_bucket')
dag = DAG('parking_violations_dag',
          description='Load and transfor parking violations data in AWS Redshift with Airflow',
          start_date=datetime(2019, 12, 11),
          schedule_interval=None
        )

def stage_table_task(s3_key, task_id, table, data_format=None):
    """Return a StageToRedshiftOperator configured with the passed parameters."""
    key = Variable.get(s3_key)
    s3_to_redshift = StageToRedshiftOperator(
        task_id=task_id,
        table=table,
        s3_bucket=s3_bucket,
        s3_key=key,
        data_format=data_format,
        dag=dag
    )
    return s3_to_redshift


# Dummy operators
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Stage operators
stage_states_to_redshift = stage_table_task('s3_parking_violations_states_key', 'Stage_states', 'stage_state', "CSV IGNOREHEADER 1 DELIMITER ';'")
stage_violation_codes_to_redshift = stage_table_task('s3_parking_violations_violation_codes_key', 'Stage_violation_codes', 'stage_violation_code')
stage_vehicle_body_types_to_redshift = stage_table_task('s3_parking_violations_vehicle_body_types_key', 'Stage_vehicle_body_types', 'stage_vehicle_body_type', f"JSON 's3://{s3_bucket}/{Variable.get('s3_parking_violations_vehicle_body_types_jsonpaths_key')}'")
stage_vehicle_plate_types_to_redshift = stage_table_task('s3_parking_violations_vehicle_plate_types_key', 'Stage_vehicle_plate_types', 'stage_vehicle_plate_type', f"JSON 's3://{s3_bucket}/{Variable.get('s3_parking_violations_vehicle_plate_types_jsonpaths_key')}'")
stage_vehicle_colors_to_redshift = stage_table_task('s3_parking_violations_vehicle_colors_key', 'Stage_vehicle_colors', 'stage_vehicle_color', f"JSON 's3://{s3_bucket}/{Variable.get('s3_parking_violations_vehicle_colors_jsonpaths_key')}'")
# stage_violations_to_redshift = stage_table_task('s3_parking_violations_key', 'Stage_violations', 'stage_violation', "CSV IGNOREHEADER 1 DELIMITER ';' ACCEPTANYDATE DATEFORMAT 'auto'")
# FOR THE SAMPLE FILE (',' AND NOT ';')
stage_violations_to_redshift = stage_table_task('s3_parking_violations_key', 'Stage_violations', 'stage_violation', "CSV IGNOREHEADER 1 ACCEPTANYDATE DATEFORMAT 'auto'")
stage_counties_to_redshift = stage_table_task('s3_parking_violations_counties_key', 'Stage_counties', 'stage_county', f"JSON 's3://{s3_bucket}/{Variable.get('s3_parking_violations_counties_jsonpaths_key')}'")
stage_issuing_agencies_to_redshift = stage_table_task('s3_parking_violations_issuing_agencies_key', 'Stage_issuing_agencies', 'stage_issuing_agency', f"JSON 's3://{s3_bucket}/{Variable.get('s3_parking_violations_issuing_agencies_jsonpaths_key')}'")

stage2_violations_to_redshift = LoadTableOperator(
    task_id='Stage2_violations',
    table='stage2_violation',
    select_sql_stmt=SqlQueries.stage2_violation_table,
    dag=dag
)


# Dimension operators
load_violation_code = LoadTableOperator(
    task_id='Load_violation_code',
    table='violation_code',
    select_sql_stmt=SqlQueries.violation_code_table,
    dag=dag
)

load_date = LoadTableOperator(
    task_id='Load_date',
    table='date',
    select_sql_stmt=SqlQueries.date_table,
    dag=dag
)

load_time = LoadTableOperator(
    task_id='Load_time',
    table='time',
    select_sql_stmt=SqlQueries.time_table,
    dag=dag
)

load_location = LoadTableOperator(
    task_id='Load_location',
    table='location',
    columns='street_code1, street_code2, street_code3, street_name, intersecting_street, precinct, county, house_number',
    select_sql_stmt=SqlQueries.location_table,
    dag=dag
)


load_vehicle = LoadTableOperator(
    task_id='Load_vehicle',
    table='vehicle',
    select_sql_stmt=SqlQueries.vehicle_table,
    dag=dag
)

# Fact operators
load_violation = LoadTableOperator(
    task_id='Load_violation',
    table='violation',
    select_sql_stmt=SqlQueries.violation_table,
    dag=dag
)

# Quality check operators
has_rows_checker = lambda records: len(records) == 1 and len(records[0]) == 1 and records[0][0] > 0

def non_missing_percentage_checker(table, column, min_percentage=100):
    """Return a checker which can be used in the DataQualityOperator
    This checker ensures that at least min_percentage% of column in table is not null.
    For example, at least 70% of ids in user table are not null.
    This function returns a checker as well as a SQL statment.
    """
    return SqlQueries.column_non_null_percentage_stmt(table, column), lambda records: len(records) == 1 and len(records[0]) == 1 and records[0][0] >= min_percentage


# At least 85% of plate_id are not null
plate_id_non_null_stmt, plate_id_non_null_checker = non_missing_percentage_checker('stage_violation', 'plate_id', 85)

# issue_date must not contain any null values
issue_date_non_null_stmt, issue_date_non_null_checker = non_missing_percentage_checker('stage_violation', 'issue_date', 100)

check_stage = DataQualityOperator(
    task_id='Check_stage',
    postgres_conn_id='redshift',
    sql_stmts = [
        plate_id_non_null_stmt,
        issue_date_non_null_stmt
    ],
    result_checkers = [
        plate_id_non_null_checker,
        issue_date_non_null_checker
    ],
    dag=dag
)

check_fact_dimension = DataQualityOperator(
    task_id='Check_fact_dimension',
    postgres_conn_id='redshift',
    sql_stmts = [
        SqlQueries.count_stmt('violation_code'),
        SqlQueries.count_stmt('date'),
        SqlQueries.count_stmt('time'),
        SqlQueries.count_stmt('location'),
        SqlQueries.count_stmt('vehicle'),
        SqlQueries.count_stmt('violation'),
    ],
    result_checkers = [
        has_rows_checker, has_rows_checker,
        has_rows_checker, has_rows_checker,
        has_rows_checker, has_rows_checker
    ],
    dag=dag
)

# Define dependencies

stage_operators = [
    stage_states_to_redshift, stage_violation_codes_to_redshift,
    stage_vehicle_body_types_to_redshift, stage_vehicle_plate_types_to_redshift,
    stage_vehicle_colors_to_redshift, stage_counties_to_redshift,
    stage_issuing_agencies_to_redshift, stage_violations_to_redshift
]

fact_operators = [
    load_violation
]

dimension_operators = [
    load_violation_code, load_date,
    load_time, load_location,
    load_vehicle
]

start_operator >> stage_operators >> check_stage >> stage2_violations_to_redshift >> fact_operators + dimension_operators >> check_fact_dimension >> end_operator
