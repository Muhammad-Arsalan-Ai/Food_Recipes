from fastapi import FastAPI
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, ArrayType
import json
import pandas as pd

app = FastAPI()

@app.get("/fetch-data")
def fetch_data():
    url = "https://tasty.p.rapidapi.com/recipes/list"
    headers = {
        "X-RapidAPI-Key": "9a01d18606msh72790d85acbf34dp1267cejsnd8151080a282",
        "X-RapidAPI-Host": "tasty.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    spark = SparkSession.builder.appName("FastAPI to PySpark").getOrCreate()
    # Define the schema for the data
    schema = StructType([
        StructField("count", IntegerType(), True),
        StructField("results", ArrayType(StructType([
            StructField("cook_time_minutes", IntegerType(), True),
            StructField("raw_text", StringType(), True),
            StructField("name", StringType(), True),
            StructField("num_servings", IntegerType(), True),
            StructField("description", StringType(), True),
            StructField("total_time_minutes", IntegerType(), True),
            StructField("country", StringType(), True),
            StructField("yields", StringType(), True),
            StructField("language", StringType(), True)
        ])), True),
    ])

    count = data.get("count")
    results = data.get("results")

    # Create the PySpark DataFrame
    rows = [(count, results)]
    df = spark.createDataFrame(rows, schema)
    
    result_data1 = df.select("results.name").first()

    # Extract the dish names as a list
    dish_names = result_data1["name"]

    # Calculate the total number of dishes
    total_dishes = len(dish_names)


    # Select the desired columns from the "results" column
    result_data = df.select("results.cook_time_minutes", "results.raw_text", "results.name", "results.num_servings",
                            "results.description", "results.total_time_minutes", "results.country", "results.yields",
                            "results.language").first()
    result_list = {
        "cook_time_minutes": result_data["cook_time_minutes"][:5],
        "raw_text": result_data["raw_text"][:5],
        "name": result_data["name"][:5],
        "num_servings": result_data["num_servings"][:5],
        "description": result_data["description"][:5],
        "total_time_minutes": result_data["total_time_minutes"][:5],
        "country": result_data["country"][:5],
        "yields": result_data["yields"][:5],
        "language": result_data["language"][:5]
    }
 
 
    filtered_data = [
        {
            "cook_time_minutes": cook_time,
            "raw_text": raw_text,
            "name": name,
            "num_servings": num_servings,
            "description": description,
            "total_time_minutes": total_time_minutes,
            "country": country,
            "yields": yields,
            "language": language
        }
        for cook_time, raw_text, name, num_servings, description, total_time_minutes, country, yields, language in zip(
            result_data["cook_time_minutes"],
            result_data["raw_text"],
            result_data["name"],
            result_data["num_servings"],
            result_data["description"],
            result_data["total_time_minutes"],
            result_data["country"],
            result_data["yields"],
            result_data["language"]
        )
        if cook_time is not None and cook_time > 10
    ]
    # Stop the Spark session
    spark.stop()
    # print(result_list)
 
    df = pd.DataFrame(result_list)
    print(df)
 
    # df = spark.createDataFrame(result_list)
    # print(df)
 
    return result_list,{"Filtered Data which time is greater than 10 mins":filtered_data},{"total_Types_of_dishes": total_dishes}

# Run the FastAPI application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)