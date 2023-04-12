import os

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import pandas as pd
from sodapy import Socrata

from user_definition import *


def retrieve_yesterday_data(url, app_token, sub_uri, data_limit, yesterday):
    '''
    Access clients using url and app_token and
    retrieve data from sub_uri where limit is data_limit.
    Return a dataframe where 'received_datetime' is yesterday.
    '''
    # app_token is not needed for public data
    client = Socrata(url, app_token=app_token)
    results = client.get(sub_uri, limit=data_limit)
    results_df = pd.DataFrame.from_records(results)
    return results_df[results_df['received_datetime'].
                      str.startswith(yesterday)]


def write_dataframe_to_csv(parent_dir, directory, file_name, df):
    '''
    Write a data frame (df) to
    {parent_dir}/{directory}/{file_name} in csv format.
    '''
    file_path = f'{parent_dir}/{directory}/{file_name}'
    df.to_csv(file_path, index=False)


def download_sf_law_enforcement_data(**kwargs):
    '''
    Write data frame from retrieve_yesterday_data()
    using write_dataframe_to_csv.
    '''
    df = retrieve_yesterday_data(kwargs['url'],
                                 kwargs['app_token'],
                                 kwargs['sub_uri'],
                                 kwargs['data_limit'],
                                 kwargs['yesterday'])
    write_dataframe_to_csv(kwargs['data_dir'],
                           kwargs['yesterday'],
                           f"{kwargs['sub_uri']}.csv", df)


with DAG(dag_id="download_data_step1",
         start_date=datetime(2023, 1, 14),
         schedule_interval='@daily') as dag:

    # https://github.com/apache/airflow/discussions/24463
    os.environ["no_proxy"] = "*"  # set this for airflow errors.

    create_dirs_op = BashOperator(task_id="create_dirs",
                                  bash_command=f"mkdir -p {data_dir}/{yesterday}")

    download_sf_law_enforcement = PythonOperator(task_id="download_sf_law_enforcement",
                                                 python_callable=download_sf_law_enforcement_data,
                                                 op_kwargs={'url': sf_data_url,
                                                            'app_token': sf_data_app_token,
                                                            'data_dir': data_dir,
                                                            'sub_uri': sf_data_sub_uri,
                                                            'data_limit': data_limit,
                                                            'yesterday': yesterday})

    create_dirs_op >> download_sf_law_enforcement
