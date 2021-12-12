# import the libraries

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Deepak',
    'start_date': days_ago(0),
    'email': 'dipakgibm@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retries_delay': timedelta(minutes=5),

}

dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='ETL_toll_data',
    schedule_interval=timedelta(days=1),
)

# define the task 'unzip_data'

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='wget -c https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -O - | tar -xz',
    dag=dag,
)

# define the task 'extract_data_from_csv'


extract_data_from_csv = BashOperator(
    task_id='extract',
    bash_command='cut -d"," -f1,2,3,4 vehicle-data.csv  > csv_data.csv',
    dag=dag,
)

# define the task 'extract_data_from_tsv'

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut  -f5,6,7,8 tollplaza-data.tsv > tsv_data.csv',
    dag=dag,
)


# define the task 'extract_data_from_tsv'

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -b 59- payment-data.txt > fixed_width_data.csv',
    dag=dag,
)

# define the task 'extract_data_from_tsv'

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv>extracted_data.csv,
    dag=dag,
)

# define the task 'transform_data'

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk '$5 = toupper($5)' extracted_data1.csv>transformed_data.csv,
    dag=dag,
)

extract_data_from_csv>>extract_data_from_tsv>>extract_data_from_fixed_width>>extract_data_from_fixed_width>>consolidate_data>>transform_data
