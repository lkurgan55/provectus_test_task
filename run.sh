#! /bin/bash
mkdir ./minio
sudo chmod 777 minio
docker-compose build
docker-compose up -d