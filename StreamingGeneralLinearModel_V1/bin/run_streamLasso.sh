#!/bin/bash

#if [ -f "streaming_log.txt" ]; then
#	rm rf streaming_log.txt
#	echo "rm streaming_log.txt"
#fi
echo "Run the spark code....."
 /home/zhangyuming/spark_kaiyuan/spark-1.0.0-bin-hadoop1/bin/spark-submit \
  --class "doStreamLasso" \
  --master local[2] \
  --jars "$SPARK_EXAMPLES_JAR" \
  doStreamLasso.jar \
  --step 0.05 --numDataInBatch 50 --dimenOfData 9 --regPara 0.1 \
  --testFileName breast-test.txt \
  parameters.txt
