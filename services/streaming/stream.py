import os
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("ticks-stream").getOrCreate()

schema = T.StructType([
    T.StructField("symbol", T.StringType()),
    T.StructField("ts", T.StringType()),
    T.StructField("price", T.DoubleType()),
    T.StructField("volume", T.LongType())
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP","kafka:9092")) \
    .option("subscribe", os.getenv("KAFKA_TOPIC","nse_ticks")) \
    .option("startingOffsets","latest").load()

parsed = df.select(F.from_json(F.col("value").cast("string"), schema).alias("j")).select("j.*")

SUPABASE_URL = f"jdbc:postgresql://{os.getenv('SUPABASE_HOST')}:{os.getenv('SUPABASE_PORT')}/{os.getenv('SUPABASE_DB')}?sslmode={os.getenv('SUPABASE_SSLMODE','require')}"
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")

def write_to_supabase(batch_df, batch_id):
    (batch_df
      .withColumn("ts", F.to_timestamp("ts"))
      .write
      .format("jdbc")
      .option("url", SUPABASE_URL)
      .option("dbtable", "public.ticks")
      .option("user", SUPABASE_USER)
      .option("password", SUPABASE_PASSWORD)
      .option("driver", "org.postgresql.Driver")
      .mode("append")
      .save())

parsed.writeStream.foreachBatch(write_to_supabase).outputMode("append").start().awaitTermination()