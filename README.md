# Data-Engineer
Simple ETL job to pull the log from servers and do the transform and load it.
To submit the DAG to Apache Airflow copy the file to cp  ETL_Server_Access_Log_Processing.py $AIRFLOW_HOME/dags
Verify if the DAG is submitted.run the cmd "airflow dags list" and Verify that the DAG etl-log-processsing-dag is listed.
