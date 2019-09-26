from pyspark.sql import SparkSession

logFile = "/usr/local/Cellar/spark/README.md"
spark = SparkSession.builder.appName(
    "SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()

numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a:%i, lins with b:%i" % (numAs, numBs))

spark.stop()
