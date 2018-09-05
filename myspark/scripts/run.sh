#!/usr/bin/bash

java -jar target/myspark-1.0-SNAPSHOT.jar -sparkConfFile src/main/resources/spark_conf/spark-local.json  -inputDir src/main/resources/input_dir -outputDir src/main/resources/output_dir
