from datetime import datetime
# import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
from airflow.models import Variable

def stage_table_task(s3_key, task_id, table, data_format=None):
    key = Variable.get(s3_key)
    if data_format is None:
        s3_to_redshift = StageToRedshiftOperator(
            task_id=task_id,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=key,
            dag=dag
        )
    else:
        s3_to_redshift = StageToRedshiftOperator(
            task_id=task_id,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=key,
            data_format=data_format,
            dag=dag
        )
    return s3_to_redshift

dag = DAG('parking_violations_dag',
          description='Load and transfor parking violations data in AWS Redshift with Airflow',
          start_date=datetime(2019, 12, 11),
          schedule_interval=None
        )

s3_bucket = Variable.get('s3_violations_bucket')

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_states_to_redshift = stage_table_task('s3_parking_violations_states_key', 'Stage_states', 'stage_state', "CSV IGNOREHEADER 1 DELIMITER ';'")
stage_violation_codes_to_redshift = stage_table_task('s3_parking_violations_violation_codes_key', 'Stage_violation_codes', 'stage_violation_code')
stage_vehicle_body_types_to_redshift = stage_table_task('s3_parking_violations_vehicle_body_types_key', 'Stage_vehicle_body_types', 'stage_vehicle_body_type')
stage_vehicle_plate_types_to_redshift = stage_table_task('s3_parking_violations_vehicle_plate_types_key', 'Stage_vehicle_plate_types', 'stage_vehicle_plate_type')
stage_vehicle_colors_to_redshift = stage_table_task('s3_parking_violations_vehicle_colors_key', 'Stage_vehicle_colors', 'stage_vehicle_color')
stage_violations_to_redshift = stage_table_task('s3_parking_violations_key', 'Stage_violations', 'stage_violation', "CSV IGNOREHEADER 1 DELIMITER ';' ACCEPTANYDATE DATEFORMAT 'auto'")
# FOR THE SAMPLE FILE (',' AND NOT ',')
# stage_violations_to_redshift = stage_table_task('s3_parking_violations_key', 'Stage_violations', 'stage_violation', "CSV IGNOREHEADER 1 ACCEPTANYDATE DATEFORMAT 'auto'")
stage_counties_to_redshift = stage_table_task('s3_parking_violations_counties_key', 'Stage_counties', 'stage_county')
stage_issuing_agencies_to_redshift = stage_table_task('s3_parking_violations_issuing_agencies_key', 'Stage_issuing_agencies', 'stage_issuing_agency')

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define dependencies
start_operator >> stage_states_to_redshift
start_operator >> stage_violation_codes_to_redshift
start_operator >> stage_vehicle_body_types_to_redshift
start_operator >> stage_vehicle_plate_types_to_redshift
start_operator >> stage_vehicle_colors_to_redshift
start_operator >> stage_violations_to_redshift
start_operator >> stage_counties_to_redshift
start_operator >> stage_issuing_agencies_to_redshift

stage_states_to_redshift                >> end_operator
stage_violation_codes_to_redshift       >> end_operator
stage_vehicle_body_types_to_redshift    >> end_operator
stage_vehicle_plate_types_to_redshift   >> end_operator
stage_vehicle_colors_to_redshift        >> end_operator
stage_violations_to_redshift            >> end_operator
stage_counties_to_redshift              >> end_operator
stage_issuing_agencies_to_redshift      >> end_operator
