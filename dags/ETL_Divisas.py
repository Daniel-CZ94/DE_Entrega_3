import json
import pandas as pnd
import requests
from datetime import date,timedelta,datetime
import psycopg2
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os

BASE_URL_API = None
BASE_CURRENCY = None

BASE_BD_HOST = None
BASE_BD_NAME = None
BASE_BD_USER = None
BASE_BD_PASS = None
BASE_BD_PORT = None

dag_path = os.getcwd()

with open(dag_path+'/keys/config.json','r') as file:
    config = json.load(file)

#Obtenemos los datos correspondientes a la base de datos y a la API de consulta,
#del archivo config.json
BASE_URL_API = config['DATA_API']['URL_BASE']
BASE_CURRENCY = config['DATA_API']['FROM_CURRENCY']
        
BASE_BD_HOST = config['DATA_BD']['BD_HOST']
BASE_BD_NAME = config['DATA_BD']['BD_NAME']
BASE_BD_USER = config['DATA_BD']['BD_USER']
BASE_BD_PASS = config['DATA_BD']['BD_PASS']
BASE_BD_PORT = config['DATA_BD']['BD_PORT']

TABLE_NAME = config['DATA_BD']['DATA_TABLE']['NAME_TABLE']
TABLE_COLUMNS = config['DATA_BD']['DATA_TABLE']['COLUMNS']

CARGA_ACTUAL_REALIZADA = None

default_args = {
    'owner':'DanielCZ',
    'start_date': datetime(2023,6,16),
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}
BC_dag = DAG(
    dag_id='Divisas_ETL',
    default_args=default_args,
    description='Obtiene el tipo de cambio del dolar a varias divisas de Lunes a Viernes',
    schedule_interval='@daily',
    catchup=False
)

dag_path = os.getcwd()

def validar_dia_habil(date_execution):
    try:
        print("Validamos que el dia actual sea un dia habil")
        fecha = datetime.strptime(date_execution, '%Y-%m-%d')
        dia_semana = fecha.weekday()
        dia_habil = None
        if dia_semana < 5:
            dia_habil = True
        else:
            dia_habil = False
        with open(dag_path+"/keys/dia_habil.txt","w") as txt_file:
            txt_file.write(str(dia_habil))
        
    except Exception as error:
        print("Surgio un error al validar el dia habil: "+error)

def obtener_tipo_cambio_actual(date_execution):
    try:
        print("Obteniendo el tipo de cambio actual")
        es_dia_habil = None
        with open(dag_path+"/keys/dia_habil.txt","r") as txtDiaHabil:
            es_dia_habil = txtDiaHabil.read()
        if(es_dia_habil):
            url_request = f"{BASE_URL_API}/{date_execution}?from={BASE_CURRENCY}"
            #print("url_request: "+url_request)
            request_currencies = None
            data_currencies = None
            request_currencies = requests.get(url_request)
            if request_currencies.status_code == 200:
                data_currencies = request_currencies.json()
                with open(dag_path+"/raw_data/"+"data_"+str(date.year)+"-"+str(date.month)+"-"+str(date.day)+"-"+str(datetime.hour)+".json","w") as json_file:
                    json.dump(data_currencies,json_file)
            else:
                print("La peticion al servidor de los tipos de cambio ha fallado")
        else:
            print("El dia actual es un dia inhabil. Por lo tanto no se realizara la carga")
    except Exception as ex:
        print("Surgio un error al obtener la informacion desde el servidor de tipos de cambio: "+ex)


def crear_tabla_carga_divisas():
    try:
        print("Creamos la tabla de destino")
        es_dia_habil = None
        with open(dag_path+"/keys/dia_habil.txt","r") as txtDiaHabil:
            es_dia_habil = txtDiaHabil.read()
        if(es_dia_habil):
            conn = psycopg2.connect(
                host = BASE_BD_HOST,
                dbname = BASE_BD_NAME,
                user = BASE_BD_USER,
                password = BASE_BD_PASS,
                port = BASE_BD_PORT
            )
            sql_table = f"""
            create table if not exists {TABLE_NAME}(
                currency varchar(3),
                operation_date date,
                ammount float,
                base_currency varchar(3),
                rates float,
                primary key(currency,operation_date),
                unique(currency,operation_date)
            )
            """
            cur = conn.cursor()
            cur.execute(sql_table)
            conn.commit()
        else:
            print("El dia actual es un dia inhabil. Por lo tanto no se realizara la carga")
    except Exception as error:
        print("Surgio un error al crear la tabla de carga: "+error)

def cargar_tipos_cambio():
    try:
        print("Cargando informacion en la BD")
        es_dia_habil = None
        with open(dag_path+"/keys/dia_habil.txt","r") as txtDiaHabil:
            es_dia_habil = txtDiaHabil.read()
        if(es_dia_habil):
            registros_divisas = pnd.read_json(dag_path+"/raw_data/"+"data_"+str(date.year)+"-"+str(date.month)+"-"+str(date.day)+"-"+str(datetime.hour)+".json")
            frame_currencies = pnd.DataFrame(registros_divisas)
            frame_currencies.insert(0,'currency',frame_currencies.index)
            conn = psycopg2.connect(
                host = BASE_BD_HOST,
                dbname = BASE_BD_NAME,
                user = BASE_BD_USER,
                password = BASE_BD_PASS,
                port = BASE_BD_PORT
            )
            cols = ','.join(TABLE_COLUMNS)

            insert_sql = f"insert into {TABLE_NAME} ({cols}) values %s"
            values = [tuple(x) for x in registros_divisas.to_numpy()]
            cur = conn.cursor()
            cur.execute("BEGIN")
            execute_values(cur,insert_sql,values)
            conn.commit()
            print("Se realizo la carga de informacion de forma correcta")
        else:
            print("El dia actual es un dia inhabil. Por lo tanto no se realizara la carga")
    except Exception as error:
        print("Surgio un error al cargar la informacion en la BD: "+error)

task_1 = PythonOperator(
    task_id='validar_dia_habil',
    python_callable=validar_dia_habil,
    op_args=["{{ ds }}"],
    dag=BC_dag
)
task_2 = PythonOperator(
    task_id='obtener_tipoCambioActual',
    python_callable=obtener_tipo_cambio_actual,
    op_args=["{{ ds }}"],
    dag=BC_dag
)
task_3 = PythonOperator(
    task_id='crear_tabla_carga_divisas',
    python_callable=crear_tabla_carga_divisas,
    dag=BC_dag
)
task_4 = PythonOperator(
    task_id='cargar_tipos_cambio',
    python_callable=cargar_tipos_cambio,
    dag=BC_dag
)
task_1 >> task_2 >> task_3 >> task_4