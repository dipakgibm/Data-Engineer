# Data-Engineer
Simple ETL job to pull the log from servers and do the transform and load it.

To submit the DAG to Apache Airflow copy the file to cp  ETL_Server_Access_Log_Processing.py $AIRFLOW_HOME/dags

Verify if the DAG is submitted.run the cmd "airflow dags list" and Verify that the DAG etl-log-processsing-dag is listed.

ETL python file will collect the data from multiple file for example csv, xml, json and do the transaction and store to target file.
