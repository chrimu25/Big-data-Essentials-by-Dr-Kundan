import os

# 1. BULLETPROOF ENVIRONMENT FIXES FOR MAC
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler

# 2. START SPARK SESSION (NOW INCLUDES POSTGRESQL DRIVER)
# Notice we added org.postgresql:postgresql:42.6.0 to the packages string
spark = SparkSession.builder \
    .appName("StockStreamPredictor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. LOAD THE TRAINED AI MODEL
print("🧠 Loading Trained AI Model...")
model_path = "stock_lr_model" 
try:
    lr_model = LinearRegressionModel.load(model_path)
    print("✅ Model Loaded Successfully!")
except Exception as e:
    print(f"❌ Error loading model. Run model_trainer.py first.")
    raise e

# 4. DEFINE THE EXACT DATA SCHEMA
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", StringType(), True),
    StructField("High", StringType(), True),
    StructField("Low", StringType(), True),
    StructField("Close", StringType(), True),
    StructField("Adj Close", StringType(), True),
    StructField("Volume", StringType(), True),
    StructField("Symbol", StringType(), True)
])

print("⏳ Waiting for Kafka stream to start (Reading from the earliest offset)...")

# 5. READ THE LIVE STREAM FROM KAFKA
raw_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_stream") \
    .option("startingOffsets", "earliest") \
    .load()

# 6. PARSE THE JSON DATA
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 6.5 CAST STRINGS TO NUMBERS (Fixes the Decimal/Integer crashes)
parsed_stream = parsed_stream \
    .withColumn("Open", col("Open").cast(DoubleType())) \
    .withColumn("High", col("High").cast(DoubleType())) \
    .withColumn("Low", col("Low").cast(DoubleType())) \
    .withColumn("Close", col("Close").cast(DoubleType())) \
    .withColumn("Volume", col("Volume").cast(DoubleType()))

# 7. CLEAN THE DATA
clean_stream = parsed_stream.dropna(subset=["Open", "High", "Low", "Volume"])

# 8. ASSEMBLE FEATURES
assembler = VectorAssembler(
    inputCols=["Open", "High", "Low", "Volume"],
    outputCol="features"
)
feature_stream = assembler.transform(clean_stream)

# 9. MAKE LIVE PREDICTIONS
prediction_stream = lr_model.transform(feature_stream)

# 10. FORMAT THE FINAL OUTPUT
final_output = prediction_stream.select("Date", "Symbol", "Close", "prediction")

# ==============================================================================
# 11. AWS RDS POSTGRESQL CONFIGURATION
# ==============================================================================

DB_URL = "jdbc:postgresql://dbessentialexam.c9kaassq2icm.eu-north-1.rds.amazonaws.com:5432/postgres"
DB_PROPERTIES = {
    "user": "mugabo",
    "password": "dbessentialexam",
    "driver": "org.postgresql.Driver"
}
TABLE_NAME = "live_stock_predictions"

# This function writes each micro-batch directly to AWS
def write_to_postgres(df, epoch_id):
    if not df.isEmpty():
        try:
            df.write.jdbc(
                url=DB_URL,
                table=TABLE_NAME,
                mode="append", # Appends new rows as they stream in
                properties=DB_PROPERTIES
            )
            print(f"☁️ Successfully wrote Batch {epoch_id} to AWS RDS!")
        except Exception as e:
            print(f"❌ Failed to write Batch {epoch_id} to RDS. Error: {e}")

# 12. OUTPUT TO POSTGRESQL INSTEAD OF CONSOLE
query = final_output \
    .writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "rds_checkpoint_dir") \
    .start()

query.awaitTermination()