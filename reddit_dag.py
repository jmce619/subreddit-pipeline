from datetime import datetime
import reddit_scrape

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator

default_args={"start_date":datetime(2020,1,1),"owner":"airflow"}

with DAG(dag_id="subreddit_dag",schedule_interval="@daily",default_args=default_args,catchup=False) as dag:

	subreddit_scrape=PythonOperator(task_id="subreddit_scrape",python_callable=reddit_scrape.main)
	data_clean=PythonOperator(task_id="data_clean",python_callable=clean_data.main)
	storing_posts=BashOperator(task_id="storing_posts",bash_command='hadoop fs -f -put -f /tmp/subreddit_clean.csv /tmp/')
	loading_posts=HiveOperator(task_id="loading_posts",hql='LOAD DATA INPATH "/tmp/subreddit_clean.csv" INTO TABLE subreddit')

	subreddit_scrape>>data_clean_storing>>storing_posts>>loading_posts
	





