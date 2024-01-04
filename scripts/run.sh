#!/bin/bash

docker exec -it spark-master /spark/bin/spark-submit --driver-class-path /jars/postgresql-42.2.10.jar --jars /jars/postgresql-42.2.10.jar /home/$1