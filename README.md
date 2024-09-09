# airflow-docker

Ref: https://blog.devgenius.io/how-to-install-apache-airflow-apache-spark-in-a-single-docker-container-1fa4f6fba3c7

This project sets up Apache Airflow and Apache Spark in a single Docker container. It includes example DAGs to demonstrate basic functionalities.

## Prerequisites

- Docker
- Docker Compose
- 
## Directory Structure

- `dags/`: Contains example DAGs.
  - `example_bash_operator.py`: Example DAG using BashOperator.
  - `sample_dag.py`: Sample DAG with basic tasks.
  - `xcom_dag.py`: Example DAG demonstrating XCom usage.
- `docker-compose.yaml`: Docker Compose configuration.
- `Dockerfile`: Dockerfile to build the Docker image.
- `requirements.txt`: Python dependencies.
