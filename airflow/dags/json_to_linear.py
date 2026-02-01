from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def read_json_file():
    """Читает JSON файл и возвращает данные"""
    try:
        with open('/opt/airflow/data/pets.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Successfully read JSON file. Found {len(data.get('pets', []))} pets")
        return data
    except Exception as e:
        logger.error(f"Error reading JSON file: {str(e)}")
        raise

def transform_to_linear(**context):
    """Преобразует вложенную JSON структуру в линейную"""
    ti = context['ti']
    json_data = ti.xcom_pull(task_ids='read_json')
    
    linear_data = []
    
    for pet in json_data.get('pets', []):
        name = pet.get('name')
        species = pet.get('species')
        birth_year = pet.get('birthYear')
        photo = pet.get('photo')
        fav_foods = pet.get('favFoods', [])
        
        if fav_foods:
            for idx, food in enumerate(fav_foods, 1):
                linear_data.append({
                    'pet_name': name,
                    'species': species,
                    'birth_year': birth_year,
                    'photo_url': photo,
                    'fav_food': food,
                    'food_order': idx
                })
        else:
            linear_data.append({
                'pet_name': name,
                'species': species,
                'birth_year': birth_year,
                'photo_url': photo,
                'fav_food': None,
                'food_order': None
            })
    
    logger.info(f"Transformed {len(json_data.get('pets', []))} pets into {len(linear_data)} linear records")
    return linear_data

def load_to_postgres(**context):
    """Загружает линейные данные в PostgreSQL"""
    ti = context['ti']
    linear_data = ti.xcom_pull(task_ids='transform_to_linear')
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("TRUNCATE TABLE pets_linear RESTART IDENTITY;")
        logger.info("Table pets_linear truncated")
        
        insert_query = """
            INSERT INTO pets_linear (pet_name, species, birth_year, photo_url, fav_food, food_order)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        for record in linear_data:
            cursor.execute(insert_query, (
                record['pet_name'],
                record['species'],
                record['birth_year'],
                record['photo_url'],
                record['fav_food'],
                record['food_order']
            ))
        
        conn.commit()
        logger.info(f"Successfully inserted {len(linear_data)} records into pets_linear table")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading data to PostgreSQL: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

def verify_data(**context):
    """Проверяет загруженные данные и выводит статистику"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT COUNT(*) FROM pets_linear;")
        total_records = cursor.fetchone()[0]
        
        # Статистика по видам
        cursor.execute("""
            SELECT species, COUNT(*) as count 
            FROM pets_linear 
            GROUP BY species;
        """)
        species_stats = cursor.fetchall()
        
        # Вывод некоторых записей
        cursor.execute("SELECT * FROM pets_linear LIMIT 5;")
        sample_records = cursor.fetchall()
        
        logger.info(f"Total records in database: {total_records}")
        logger.info(f"Species statistics: {species_stats}")
        logger.info(f"Sample records: {sample_records}")
        
        print("\n" + "="*80)
        print("DATA VERIFICATION REPORT")
        print("="*80)
        print(f"\nTotal records loaded: {total_records}")
        print("\nSpecies distribution:")
        for species, count in species_stats:
            print(f"  - {species}: {count} records")
        print("\nSample records (first 5):")
        for record in sample_records:
            print(f"  {record}")
        print("="*80 + "\n")
        
    except Exception as e:
        logger.error(f"Error verifying data: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

with DAG(
    'json_to_linear_structure',
    default_args=default_args,
    description='Преобразование JSON в линейную структуру и загрузка в PostgreSQL',
    schedule_interval=None,
    catchup=False,
    tags=['json', 'etl', 'transformation'],
) as dag:
    
    # Задача 1: Чтение json файла
    read_json_task = PythonOperator(
        task_id='read_json',
        python_callable=read_json_file,
    )
    
    # Задача 2: Преобразование в линейную структуру
    transform_task = PythonOperator(
        task_id='transform_to_linear',
        python_callable=transform_to_linear,
        provide_context=True,
    )
    
    # Задача 3: Загрузка в postgres
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True,
    )
    
    # Задача 4: Проверка данных
    verify_task = PythonOperator(
        task_id='verify_data',
        python_callable=verify_data,
        provide_context=True,
    )
    
    # Определение порядка выполнения задач
    read_json_task >> transform_task >> load_task >> verify_task
