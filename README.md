# Airflow Setup
### Tutorial from DataStack.tv

1. Create a working directory and cd in. Used for saving DAGs and plugins.
2. Setup the Airflow home var environment to your PATH (the pwd). Do this step for all terminal sessions: `export AIRFLOW_HOME=/Users/rafaellopez/Development/airflow/airflow-tutorial`
3. Make sure its working: `echo $AIRFLOW_HOME`
4. Create virtual env with Conda and packages: `conda create --name airflow-tutorial python=3.7` (doesnt work with python 3.8)
5. Activate the venv: `conda activate airflow-tutorial`

## Install Airflow and other packages 
1. Airflow with extra packages (GCP (for interaction with GCP), Statsd(for monitoring), and Sentry(for error tracking)): `pip install apache-airflow[gcp,statsd,sentry]==1.10.10`
2. Cryptography (to encrypt Airflow conection passwords): `pip install cryptography==2.9.2`
3. PySpark (to create and schedule PySpark jobs): `pip install pyspark==2.4.5`

## Validate Airflow's installation
1. Run `airflow version`

## Running Airflow
1. Initiatilize Airflow's SQLite database to create the tables necessary to store Dags: `airflow initdb`
2.  Run the webserver: `airflow webserver`
3. Open new terminal tab/session and add your home var again: `export AIRFLOW_HOME=/Users/rafaellopez/Development/airflow/airflow-tutorial`
4. Activate your venv again: `conda activate airflow-tutorial`
5. Run Airflow scheduler: `airflow scheduler`
6. Now both, the webserver and the scheduler are runnign at the same time in 2 terminal sessions.
7. In the webserver session you can pick your local url: `0.0.0.0:8080`