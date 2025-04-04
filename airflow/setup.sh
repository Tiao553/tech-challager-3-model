mkdir ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

docker build -t custom-airflow:2.10.5 .

docker-compose up -d