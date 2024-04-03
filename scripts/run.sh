#!/bin/bash

# docker exec -it spark-master /spark/bin/spark-submit --driver-class-path /jars/postgresql-42.2.10.jar --jars /jars/postgresql-42.2.10.jar,/jars/hadoop-client-2.10.2.jar,/jars/hadoop-aws-2.10.2.jar --exclude-packages com.google.guava:guava /home/$1

docker exec -it spark-master /spark/bin/spark-submit --driver-class-path /jars/postgresql-42.2.10.jar --jars /jars/postgresql-42.2.10.jar  --packages org.apache.hadoop:hadoop-aws:2.10.2,org.apache.hadoop:hadoop-client:2.10.2 --exclude-packages com.google.guava:guava /home/$1
