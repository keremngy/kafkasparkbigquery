
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

val kafkadf = spark.readStream.format("kafka"). option("kafka.bootstrap.servers", "localhost:9092"). option("subscribe", "kafkaprojesi").load

val dfschema = StructType( List( StructField("id", IntegerType),StructField("tarih",StringType), StructField("cins",StringType),StructField("bolge",StringType)))

val basedf = kafkadf.select(from_json($"value".cast("string"), dfschema).alias("dfalias"))

val countdf = basedf. groupBy($"dfalias"("tarih"),$"dfalias"("bolge"),$"dfalias"("cins")).count. sort($"count".desc)

val streamcountdf = countdf. writeStream.outputMode("complete").format("console"). option("truncate","false"). trigger(ProcessingTime("5 seconds")).start


