#!/bin/bash 

docker exec namenode hdfs dfs -rm -r /input

docker exec namenode hdfs dfs -put -f /input /input