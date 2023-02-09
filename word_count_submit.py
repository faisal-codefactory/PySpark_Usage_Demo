from pyspark.sql import SparkSession
import pyspark.sql.functions as F
spark = (SparkSession.builder
         .appName ("Analyzing Vocabulary of Pride and Prejudice Batch")
         .getOrCreate()
)
results2 = (
spark.read.text("./work/1342-0.txt")
#spark.read.text("./work/1342-0.txt")
.select(F.split(F.col("value"), " ").alias("line"))
.select(F.explode(F.col("line")).alias("word"))
.select(F.lower(F.col("word")).alias("word"))
.select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
.where(F.col("word") != "")
.groupby("word")
.count()
.orderBy(F.col("count").desc())
)
results2.coalesce(1).write.csv("./simple_count_res.csv")
results2.show()