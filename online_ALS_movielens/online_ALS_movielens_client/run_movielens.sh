#!/bin/bash

echo "Run the script.................."
echo '--------------------we do-----------------------'
echo "Run the movieLensTuiJianDataClient.jar code....."

/home/zhangyuming/app/spark/bin/spark-submit \
  --class "onlineALSMovieLens" \
  --jars "$SPARK_EXAMPLES_JAR" \
  --master local[2]   onlineALSMovieLens.jar \
  --timeIntertoGetData 30

echo "------------------we do end-----------------------"
