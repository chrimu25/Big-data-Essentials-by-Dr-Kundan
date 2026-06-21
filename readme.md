# **Real-Time Hybrid AI Stock Predictions Pipeline 📈**

## **Overview**

This project implements a **Hybrid (Lambda) Big Data Architecture** for real-time stock market prediction. It combines massive historical batch processing for Machine Learning training with live API stream processing for real-time inference.

The system extracts historical Kaggle data from a distributed Hadoop file system to train the AI. Then, it utilizes the Yahoo Finance API to stream live, to-the-minute market data through Apache Kafka. PySpark processes this live stream, applies the trained model, and outputs predictions to an AWS RDS database, which is visualized in real-time on a Streamlit dashboard.

**Complete Hybrid Pipeline Architecture:**

* **Batch/Training Layer:** Kaggle Dataset ➔ Hadoop (HDFS) ➔ PySpark MLlib (Model Training)  
* **Stream/Speed Layer:** Yahoo Finance API ➔ Apache Kafka ➔ PySpark (Live Inference) ➔ AWS RDS (PostgreSQL) ➔ Streamlit Dashboard

## **🎯 What Are We Predicting & How to Read It**

The core objective of this AI pipeline is to predict the exact **Closing Price** of a given stock in real-time.

### **The Machine Learning Logic**

We use a **Linear Regression** model powered by PySpark MLlib.

* **The Input (Features):** As the live market data streams in from the Yahoo Finance API via Kafka, the AI looks at four key metrics for that specific minute: Open (starting price), High (peak price), Low (bottom price), and Volume (total shares traded).  
* **The Output (Prediction):** Using the historical patterns it learned during the Hadoop training phase, the AI instantly calculates a prediction for what the final Close price should be based on those four features.

### **How to Read the Dashboard**

When you launch the Streamlit web application, you will see a live-updating interactive chart:

* **The Lines:** The chart plots two lines. One represents the **Actual Close Price** (the true live price), and the other represents the **AI Predicted Price**.  
* **Interpretation:** If the AI's predicted line tightly hugs the actual line, the model is highly accurate\! If there are massive gaps, the model struggled to predict that minute's volatility.  
* **Interactivity:** You can use the sidebar dropdown to switch between different company tickers (e.g., AAPL, TSLA, MSFT) to see how the AI's accuracy shifts depending on the stock.

## **🛠️ Tech Stack, Packages & Plugins**

### **Core Infrastructure**

* **Hadoop (HDFS):** Distributed storage system used to house the massive historical dataset.  
* **Apache Kafka:** Distributed event streaming platform used to handle real-time API feeds.  
* **Apache Spark (PySpark):** Version 4.1.2 (built on Scala 2.13). Used for structured streaming and distributed machine learning computations.  
* **AWS RDS (PostgreSQL):** Cloud relational database used as the final data sink for live predictions.

### **Python Packages (pip install)**

* pyspark: The Python API for Apache Spark.  
* kafka-python: Python client to publish data to the Apache Kafka cluster.  
* yfinance: The Yahoo Finance API library used to fetch live, 1-minute interval market data.  
* pandas: Data manipulation library used across the pipeline.  
* streamlit: The web framework used to build the real-time visualization dashboard.  
* psycopg2-binary: PostgreSQL database adapter for Python to connect Streamlit to AWS RDS.

### **Spark Extensions/Plugins (Maven Coordinates)**

These packages are injected directly into the PySpark session at runtime:

* org.apache.spark:spark-sql-kafka-0-10\_2.13:4.1.2: Allows PySpark to consume data directly from Kafka topics.  
* org.postgresql:postgresql:42.6.0: The JDBC driver that allows PySpark to write micro-batches directly to the AWS RDS Postgres database.

## **🚀 Complete Step-by-Step Guide**

### **Phase 1: Data Acquisition & Hadoop Ingestion (The Batch Layer)**

1. **Download the Data:** The historical training data was sourced from the [Kaggle Stock Market Dataset](https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset).  
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

### **Phase 3: Start the Hybrid Kafka Producer (The Stream Layer)**

1. Start your local **Zookeeper** and **Kafka Broker**.  
2. Create a Kafka topic named stock\_stream.  
3. Launch the hybrid producer. Instead of reading local files, this script polls the Yahoo Finance API (yfinance) every 60 seconds for the absolute latest market data and streams it to Kafka.  
   \# Open a new terminal window  
   python3 hybrid\_producer.py

### **Phase 4: Start the PySpark Consumer**

The consumer acts as the core processing engine. It subscribes to the Kafka topic, parses the JSON, casts the data types, applies the pre-trained ML model to generate a prediction, and writes the output micro-batches to AWS RDS.

\# Open a new terminal window  
python3 consumer.py

### **Phase 5: Launch the Live Dashboard**

The Streamlit dashboard queries the AWS database and plots the actual vs. predicted prices in real-time.

\# Open a new terminal window  
streamlit run dashboard.py

## **🧗‍♂️ Development Journey & Problem Solving**

Building this pipeline required solving several complex data engineering challenges:

1. **Java & Scala Architecture Conflicts:** Spark requires specific Java versions. We configured the environment variables (JAVA\_HOME) to explicitly target Homebrew's openjdk@17. Furthermore, upgrading to Spark 4.1.2 required updating our Kafka connector dependencies from Scala 2.12 to 2.13.  
2. **Null VectorAssembler Crashes:** Machine Learning models in PySpark crash if fed null values. When Kafka streamed data, slight schema formatting mismatches caused fields to drop. We implemented strict schema definitions and a robust .dropna() filter to protect the ML pipeline.  
3. **Strict Type Casting:** Data coming from APIs often contains numeric values with trailing decimals (e.g., 387200.0), which crashed PySpark when it expected a BIGINT (LongType). We solved this by dynamically casting all incoming string data to DoubleType() before feeding it to the assembler.  
4. **Kafka Offset & Checkpoint Management:** To prevent the consumer from missing data during its boot sequence, we configured PySpark's Kafka reader with .option("startingOffsets", "earliest"). We also used .option("failOnDataLoss", "false") and cleared the rds\_checkpoint\_dir to recover from NullPointerException errors when PySpark checkpoints became out of sync with Kafka's state.  
5. **Cloud Integration (AWS RDS):** Integrated local processing with cloud storage by attaching the PostgreSQL JDBC driver to PySpark, utilizing .foreachBatch() to execute custom insertion logic. We successfully overcame AWS network blocks by modifying the VPC Security Groups to allow inbound Postgres traffic (0.0.0.0/0) on Port 5432\.