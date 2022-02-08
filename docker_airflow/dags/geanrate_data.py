from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Variable
from time import sleep
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from data_cleaning import split_df
import pandas as pd
import os

mydir=os.path.join(os.getcwd(),"dags/")
def generate_data():
    conn = create_engine("mysql://mysqluser:mysqlpw@3.111.57.203:3306/inventory?ssl=%7B%22cert%22%3A+%22%2Ftmp%2Fclient-cert.pem%22%2C+%22ca%22%3A+%22%2Ftmp%2Fserver-ca.pem%22%2C+%22key%22%3A+%22%2Ftmp%2Fclient-key.pem%22%7D") # connect to server
    engine = create_engine('sqlite:///telecom.db', echo = True)
    
    m = 0
    while m<10:
        dataset_name = mydir+"raw_cdr_data.csv" #C:\Users\DELL\Downloads\kafka-telecom-project\docker_airflow\dags\raw_cdr_data.csv
        dataset_header_name = mydir+"raw_cdr_data_header.csv"
        #n = int(Variable.get('my_iterator'))
        
        raw_cdr_data_header= pd.read_csv(dataset_header_name,low_memory=False)
        raw_cdr_data = pd.read_csv(dataset_name, header=None, low_memory=False)
        
        df=raw_cdr_data_header.sample(n=1)
        n=df.index[0]
        print("n=",n)
        raw_cdr_data=raw_cdr_data.iloc[n:(n+1),:]
        
        #idx = raw_cdr_data.columns.tolist()
        #new_df = pd.DataFrame(columns=idx)

        #new_df.loc[n] = raw_cdr_data.iloc[n]
        call_dataset,service_dataset,device_dataset=split_df(raw_cdr_data)

        df.to_sql('raw_telecom_data',conn, if_exists='append')
        call_dataset.to_sql('call_dataset_mysql',engine, if_exists='append')
        service_dataset.to_sql('service_dataset_mysql',engine, if_exists='append')
        device_dataset.to_sql('device_dataset_mysql',engine, if_exists='append')
        sleep(10)
        m = m+1
   

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=120)
}

dag=DAG('generate_data',
         start_date=datetime(2022, 1, 4),
         max_active_runs=2,
         schedule_interval= "@daily",
         default_args=default_args,
         catchup=False
         ) 


start_dummy = DummyOperator(
    task_id='start',
    dag=dag,
    )

generate_data = PythonOperator(
  task_id='generate_data',
  python_callable=generate_data, #Registered method
  provide_context=True,
  dag=dag
)


end_dummy = DummyOperator(
    task_id='end',
    dag=dag,
    )

start_dummy >> generate_data >> end_dummy


