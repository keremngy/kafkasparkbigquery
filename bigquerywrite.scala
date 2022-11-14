import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

val bucket = "keremkafkabucket"
spark.conf.set("temporaryGcsBucket", bucket)
spark.conf.set("parentProject", "keremkafka")

val kafkadf = spark.readStream.format("kafka"). option("kafka.bootstrap.servers", "localhost:9092"). option("subscribe", "kafkaprojesi").load

val dfschema = StructType( List( StructField("id", IntegerType),StructField("tarih",StringType), StructField("cins",StringType),StructField("bolge",StringType)))

val basedf = kafkadf.select(from_json($"value".cast("string"), dfschema).alias("dfalias"))

val countdf = basedf. groupBy($"dfalias"("tarih"),$"dfalias"("bolge"),$"dfalias"("cins")).count. sort($"count".desc)

val dfcountquery = countdf.writeStream.outputMode("complete"). format("bigquery"). option("credentialsFile", "/home/austinjse/keremkafka-f1134fd1ac78.json"). option("table","keremkafka.kopeksaldiri"). option("checkpointLocation", "path/to/checkpoint/dir/in/hdfs"). option("failOnDataLoss",false). option("truncate",false).start().awaitTermination()