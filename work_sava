import os
import requests
import pandas as pd
import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "page_speed_update_clickhouse"

ch_hook = ClickHouseHook(clickhouse_conn_id="clickhouse") # Если создан коннектер, если нет, то можно подключиться через clickhouse driver

# Функция сбора информации и формирования таблицы
def pagespeed():
    urls_get = requests.get("https://raw.githubusercontent.com/grimlyrosen/tests/e04d9b18d0d2e087045e01b75d18aa0c21ce504a/urllist.csv")
    urls_list = urls_get.text.split()
    delete_url = urls_list.pop(0)
    df_table = pd.DataFrame(columns=['Date','URL','TBT','LCP'])
    url = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    tbt = ''
    lcp = ''
    for i in urls_list:
        params = {
            "url":i,
            "key":"AIzaSyBbTAmALTaWjjAXeNz7exCa2-fjqIP8jfk",
            "strategy":"desktop", #Возможно нужно добавить еще мобильную версию 
            "category":["accessibility","performance","pwa","seo"]  # Не приходилось раньше проводить подобный анализ, так что не уверен, что все эти категории нужны, взял с запасом 
        }
        res = requests.get(url, params=params)
        url_string = []
        tbt = res.json()['lighthouseResult']['audits']['total-blocking-time']['score']
        lcp = res.json()['loadingExperience']['metrics']['LARGEST_CONTENTFUL_PAINT_MS']['percentile']
        url_string.append(dt.datetime.now().strftime("%Y-%m-%d"))
        url_string.append(i)
        url_string.append(tbt)
        url_string.append(lcp)
        df_table.loc[len(df_table.index)] = url_string
    return df_table
# Функция записи в Clickhouse
def load_to_clickhouse():
    records = pagespeed()
    ch_hook.run(""" Create if not exists table dbo.table1 ("Date" Date,"URL" String,"TBT" Float64,"LCP" Float64) engine = MergeTree order by "Date" """)
    ch_hook.execute('INSERT INTO dbo.table1 VALUES', records)

default_args = {
    'owner': 'sava_v',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}
with DAG(
    dag_id=DAG_ID,
    schedule_interval='0 1 * * *',  # каждый день в час ночи по времени сервера, по умолчанию UTC
    start_date=days_ago(1),
    tags=['test','daily','tbt','lcp','update','clickhouse'],
    default_args=default_args,
    catchup=False,
) as dag:
    page_speed_data_update_clickhouse = PythonOperator(
        task_id='page_speed_data_update_in_clickhouse',
        python_callable=load_to_clickhouse,
    )
    ( 
    page_speed_data_update_clickhouse 
    )
