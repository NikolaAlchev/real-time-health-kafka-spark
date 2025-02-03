from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, from_json, struct

spark = SparkSession.builder \
    .appName("HealthDataStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .master("local[4]") \
    .getOrCreate()


classification_model = DecisionTreeClassificationModel.load('../best-model')
transformation_pipeline = PipelineModel.load('../pipeline_model')


schema = StructType([
    StructField("HighBP", FloatType(), True),
    StructField("HighChol", FloatType(), True),
    StructField("CholCheck", FloatType(), True),
    StructField("BMI", FloatType(), True),
    StructField("Smoker", FloatType(), True),
    StructField("Stroke", FloatType(), True),
    StructField("HeartDiseaseorAttack", FloatType(), True),
    StructField("PhysActivity", FloatType(), True),
    StructField("Fruits", FloatType(), True),
    StructField("Veggies", FloatType(), True),
    StructField("HvyAlcoholConsump", FloatType(), True),
    StructField("AnyHealthcare", FloatType(), True),
    StructField("NoDocbcCost", FloatType(), True),
    StructField("GenHlth", FloatType(), True),
    StructField("MentHlth", FloatType(), True),
    StructField("PhysHlth", FloatType(), True),
    StructField("DiffWalk", FloatType(), True),
    StructField("Sex", FloatType(), True),
    StructField("Age", FloatType(), True),
    StructField("Education", FloatType(), True),
    StructField("Income", FloatType(), True)
])

bootstrap_servers = 'localhost:9092'
input_topic = "health_data"
output_topic = "health_data_predicted"

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap_servers)
      .option("subscribe", input_topic)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .withColumn("json_data", from_json(col("value"), schema))
      .select("json_data.*"))

df = df.na.drop()

df_transformed = transformation_pipeline.transform(df)
streaming_features = df_transformed.select("scaledFeatures")

predictions = classification_model.transform(streaming_features)

output = predictions.select(
    struct(
        col("scaledFeatures"),
        col("prediction").alias("label")
    ).alias("value")
).selectExpr("to_json(value) AS value")

debug_query = predictions\
    .writeStream\
    .format("console")\
    .outputMode("append")\
    .start()

query = (output.writeStream
         .format("kafka")
         .option("kafka.bootstrap.servers", bootstrap_servers)
         .option("topic", output_topic)
         .option("checkpointLocation", "./checkpoint_kafka_predictions")
         .option("failOnDataLoss", "false") 
         .start())

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    spark.stop()
