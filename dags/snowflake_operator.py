# -*- coding: utf-8 -*-
# Copyright 2019 Dineshkarthik Raveendran

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Example DAG demonstrating the usage of the SnowflakeOperator & Hook."""
import logging

import airflow
import json
from pprint import pprint
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Shameem Gafoor", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="snowflake_connector", 
    default_args=args, 
    schedule_interval="0 0 * * *"
)

create_insert_query = [
    """create table if not exists public.test_table_time (operation varchar(16), tstamp timestamp, context variant);""",
    "insert into public.test_table_time(operation, tstamp, context) select 'START', current_timestamp::timestamp_NTZ, parse_json(' " + json.dumps( {} ) + " ');",
]

def insert_row(timestamp, **context):
    pprint(context)
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    result = dwh_hook.get_first("insert into public.test_table_time select 'INSERT', " + timestamp + ", parse_json(' " + context  + " ');")
    logging.info("Added row with timestamp : %s, and result is $s", timestamp, result)

def row_count(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    result = dwh_hook.get_first("select count(*) from public.test_table")
    logging.info("Number of rows in `public.test_table_time`  - %s", result[0])


with dag:
    create_insert = SnowflakeOperator(
        task_id="snowfalke_create",
        sql=create_insert_query,
        snowflake_conn_id="snowflake_conn",
    )
    for i in range(5):
        insert_task = PythonOperator(
        task_id='insert_row' + str(i),
        python_callable=insert_row,
        provide_context=True,
        op_kwargs={'timestamp': datetime.utcnow},
        dag=dag,
        )


    get_count = PythonOperator(task_id="get_count", python_callable=row_count)
    create_insert >> insert_task >> get_count
