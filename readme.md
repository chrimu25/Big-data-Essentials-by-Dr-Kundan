# **Real-Time AI Stock Predictions Pipeline 📈**

## **Overview**

This project is a comprehensive, end-to-end Big Data architecture built for real-time stock market prediction. It simulates a live financial market by extracting historical stock data, storing it in a distributed file system, streaming it concurrently, processing it in real-time to predict future closing prices using Machine Learning, and visualizing the pipeline on a live cloud-connected dashboard.

**Complete Pipeline Architecture:**

Kaggle Dataset ➔ Hadoop (HDFS) ➔ Apache Kafka ➔ PySpark (Streaming & MLlib) ➔ AWS RDS (PostgreSQL) ➔ Streamlit

## **🎯 What Are We Predicting & How to Read It**

The core objective of this AI pipeline is to predict the exact **Closing Price** of a given stock.

### **The Machine Learning Logic**

We use a **Linear Regression** model powered by PySpark MLlib.

* **The Input (Features):** As the live market data streams in from Kafka, the AI looks at four key metrics for that specific row of data: Open (starting price), High (peak price), Low (bottom price), and Volume (total shares traded).  
* **The Output (Prediction):** Using the historical patterns it learned during the Hadoop training phase, the AI instantly calculates a prediction for what the final Close price should be based on those four features.

### **How to Read the Dashboard**

When you launch the Streamlit web application, you will see a live-updating interactive chart:

* **The Lines:** The chart plots two lines. One represents the **Actual Close Price** (the true historical price), and the other represents the **AI Predicted Price**.  
* **Interpretation:** If the AI's predicted line tightly hugs the actual line, the model is highly accurate\! If there are massive gaps, the model struggled to predict that day's volatility.  
* **Interactivity:** You can use the sidebar dropdown to switch between different company tickers (e.g., AAPL, TSLA, MSFT) to see how the AI's accuracy shifts depending on the stock. The KPI metrics below the chart will show you the exact dollar amounts of the absolute latest prediction to hit the AWS database.

## **🛠️ Tech Stack, Packages & Plugins**

### **Core Infrastructure**

* **Hadoop (HDFS):** Distributed storage system used to house the massive raw dataset.  
* **Apache Kafka:** Distributed event streaming platform used to handle real-time data feeds.  
* **Apache Spark (PySpark):** Version 4.1.2 (built on Scala 2.13). Used for structured streaming and distributed machine learning computations.  
* **AWS RDS (PostgreSQL):** Cloud relational database used as the final data sink for live predictions.

### **Python Packages (pip install)**

* pyspark: The Python API for Apache Spark.  
* kafka-python: Python client to publish data to the Apache Kafka cluster.  
* pandas: Used to parse, combine, and chronologically sort the dataset before streaming.  
* streamlit: The web framework used to build the real-time visualization dashboard.  
* psycopg2-binary: PostgreSQL database adapter for Python to connect Streamlit to AWS RDS.

### **Spark Extensions/Plugins (Maven Coordinates)**

These packages are injected directly into the PySpark session at runtime:

* org.apache.spark:spark-sql-kafka-0-10\_2.13:4.1.2: Allows PySpark to consume data directly from Kafka topics.  
* org.postgresql:postgresql:42.6.0: The JDBC driver that allows PySpark to write micro-batches directly to the AWS RDS Postgres database.

## **🚀 Complete Step-by-Step Guide**

### **Phase 1: Data Acquisition & Hadoop Ingestion**

1. **Download the Data:** The raw data was sourced from the [Kaggle Stock Market Dataset by Jackson Crow](https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset).  
2. **Start the Hadoop Cluster:**  
   start-dfs.sh  
   start-yarn.sh

3. **Load Data into HDFS:** We created a directory in the Hadoop Distributed File System and loaded the CSV files into it for distributed storage.  
   hdfs dfs \-mkdir \-p /user/stock\_data/stocks  
   hdfs dfs \-put stocks/\*.csv /user/stock\_data/stocks/

### **Phase 2: Train the Machine Learning Model**

Before streaming, the AI needs to learn the mathematical relationships to predict the Close price.

1. Activate your Python virtual environment (source venv/bin/activate).  
2. Run the trainer script:  
   python3 model\_trainer.py

*This script trains a Linear Regression model using pyspark.ml and saves the trained weights to the local stock\_lr\_model/ directory.*

### **Phase 3: Start the Kafka Producer**

1. Start your local **Zookeeper** and **Kafka Broker**.  
2. Create a Kafka topic named stock\_stream.  
3. Launch the producer. The producer fetches the data, combines the files, sorts them chronologically by Date, and streams them row-by-row into the Kafka topic.  
   \# Open a new terminal window  
   python3 producer.py

### **Phase 4: Start the PySpark Consumer**

The consumer acts as the core processing engine. It subscribes to the Kafka topic, parses the JSON, casts the data types, applies the pre-trained ML model to generate a prediction, and writes the output micro-batches to AWS RDS.

\# Open a new terminal window  
python3 consumer.py

### **Phase 5: Launch the Live Dashboard**

The Streamlit dashboard queries the AWS database and plots the actual vs. predicted prices in real-time.

\# Open a new terminal window  
streamlit run dashboard.py

## **🧗‍♂️ Development Journey & Problem Solving**

Building this pipeline required solving several complex data engineering and environmental challenges:

1. **Java & Scala Architecture Conflicts on Mac:** Spark requires specific Java versions. We configured the environment variables (JAVA\_HOME) to explicitly target Homebrew's openjdk@17. Furthermore, upgrading to Spark 4.1.2 required updating our Kafka connector dependencies from Scala 2.12 to 2.13.  
2. **Null VectorAssembler Crashes:** Machine Learning models in PySpark crash if fed null values. When Kafka streamed data, slight schema formatting mismatches caused fields to drop. We implemented strict schema definitions and a robust .dropna() filter to protect the ML pipeline.  
3. **Strict Type Casting:** Pandas exported numeric volumes with trailing decimals (e.g., 387200.0), which crashed PySpark when it expected a BIGINT (LongType). We solved this by dynamically casting all incoming string data to DoubleType() before feeding it to the assembler.  
4. **Kafka Offset & Checkpoint Management:** To prevent the consumer from missing data during its boot sequence, we configured PySpark's Kafka reader with .option("startingOffsets", "earliest"). We also used .option("failOnDataLoss", "false") and cleared the rds\_checkpoint\_dir to recover from NullPointerException errors when PySpark checkpoints became out of sync with Kafka's state.  
5. **Cloud Integration (AWS RDS):** Integrated local processing with cloud storage by attaching the PostgreSQL JDBC driver to PySpark, utilizing .foreachBatch() to execute custom insertion logic. We successfully overcame AWS network blocks by modifying the VPC Security Groups to allow inbound Postgres traffic (0.0.0.0/0) on Port 5432\.