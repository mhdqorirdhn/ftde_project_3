# airflow-docker

Ref: https://blog.devgenius.io/how-to-install-apache-airflow-apache-spark-in-a-single-docker-container-1fa4f6fba3c7

This project sets up Apache Airflow and Apache Spark in a single Docker container. It includes example DAGs to demonstrate basic functionalities.

## Prerequisites

- Docker
- Docker Compose

## Setup

1. Clone the repository:

   ```sh
   git clone https://github.com/hotspoon/project_3
   cd project_3
   ```

2. Build and start the Docker container:

   ```sh
   docker-compose up --build
   ```

3. Access the Airflow web interface at `http://localhost:8080`.

## Directory Structure

- `dags/`: Contains example DAGs.
  - `example_bash_operator.py`: Example DAG using BashOperator.
  - `sample_dag.py`: Sample DAG with basic tasks.
  - `xcom_dag.py`: Example DAG demonstrating XCom usage.
- `docker-compose.yaml`: Docker Compose configuration.
- `Dockerfile`: Dockerfile to build the Docker image.
- `requirements.txt`: Python dependencies.
