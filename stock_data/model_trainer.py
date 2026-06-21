import os

# Bulletproof Java path for Mac
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# 1. Start Spark Session
spark = SparkSession.builder \
    .appName("StockModelTrainer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("⏳ Loading historical data...")

# 2. Load the data (Using your local folder to bypass Hadoop network configs)
df = spark.read.csv("data/AAPL.csv", header=True, inferSchema=True)

# Drop any rows with missing data to prevent math errors
df = df.dropna()

# 3. Prepare the Features
# We will use Open, High, Low, and Volume to predict the Close price.
assembler = VectorAssembler(
    inputCols=["Open", "High", "Low", "Volume"],
    outputCol="features"
)

# Transform the dataframe to include the new 'features' column
ml_data = assembler.transform(df)

# 4. Define and Train the Model
print("🧠 Training the Linear Regression Model...")
lr = LinearRegression(featuresCol="features", labelCol="Close")

# Fit the model to our historical data
model = lr.fit(ml_data)

# Print out the model's accuracy metrics
trainingSummary = model.summary
print(f"✅ Training Complete!")
print(f"📊 Model Error (RMSE): {trainingSummary.rootMeanSquaredError}")

# 5. Save the Model
model_path = "stock_lr_model"
# Overwrite if it already exists
model.write().overwrite().save(model_path)
print(f"💾 Model successfully saved to {model_path}/")

spark.stop()