from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, MinMaxScaler


def transform(data):
    input_cols = [
        "HighBP", "HighChol", "CholCheck", "BMI", "Smoker", "Stroke",
        "HeartDiseaseorAttack", "PhysActivity", "Fruits", "Veggies",
        "HvyAlcoholConsump", "AnyHealthcare", "NoDocbcCost", "GenHlth",
        "MentHlth", "PhysHlth", "DiffWalk", "Sex", "Age", "Education", "Income"
    ]

    assembler = VectorAssembler(inputCols=input_cols, outputCol="features", handleInvalid="keep")
    scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")

    pipeline = Pipeline(stages=[assembler, scaler])
    pipeline_model = pipeline.fit(data)

    if "Diabetes_012" in data.columns:
        pipeline_model.write().overwrite().save("pipeline_model")

    transformed_data = pipeline_model.transform(data)

    if "Diabetes_012" in data.columns:
        return transformed_data.select("scaledFeatures", "Diabetes_012")
    else:
        return transformed_data.select("scaledFeatures")