# pipeline.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, coalesce, lit, concat_ws,when,avg
from utils import clean_data
import os
import shutil

def run_pipeline(input_folder: str, output_folder: str):
    spark = SparkSession.builder \
        .appName("Formula1Pipeline") \
        .master("local[*]") \
        .getOrCreate()
    
    # Read RACES dataset
    df_races = spark.read\
            .option("header",True)\
            .option("inferSchema", True)\
            .csv(f"{input_folder}/races.csv")
    
    df_races_clean = clean_data(df_races, 'races')

    #cleans the time column
    df_races_clean = df_races_clean.withColumn("time_fixed",
        when(col("time").isin("null", ""), "00:00:00").otherwise(col("time"))
    )
    
    # Create column Race Datetime
    df_races_clean = df_races_clean.withColumn("Race Datetime",
        to_timestamp(concat_ws("T", col("date"), coalesce(col("time_fixed"), lit("00:00:00"))))
    )
    
    # Read RESULTS dataset
    df_results = spark.read\
        .option("header",True)\
        .option("inferSchema",True)\
        .csv(f"{input_folder}/results.csv")
    
    df_results_clean = clean_data(df_results, 'results')
    
    # Filtrează câștigătorul: position == 1
    df_winners = df_results_clean.filter(col("position") == 1)\
        .select("raceid", 
            col("driverid").alias("Race Winning driverId"), 
            col("fastestlaptime").alias("Race Fastest Lap")
    )
    #df_winners.show()
    # Join RACES + winners
    df_final = df_races_clean.join(df_winners, on="raceid", how="left") \
        .select(
            col("year"),
            col("name").alias("Race Name"),
            col("round").alias("Race Round"),
            col("Race Datetime"),
            col("Race Winning driverId"),
            col("Race Fastest Lap")
        )
    #df_final.show()
    
    # create yearly JSON
    years = [row['year'] for row in df_races_clean.select('year').distinct().collect()]
    
    for year in years:
        temp_path = f"{output_folder}/temp_{year}"
        df_year = df_final.filter(col("year") == year).drop("year")
        df_year.coalesce(1).write.mode("overwrite").json(temp_path)

        #Because park writes output as distributed files in a folder
        #Since the assignment required a single JSON file per year
        #I wrote the output to a temporary folder using coalesce(1) and then renamed the generated part file.
        for file in os.listdir(temp_path):
            if file.startswith("part-") and file.endswith(".json"):
                shutil.move(os.path.join(temp_path, file),f"{output_folder}/stats_{year}.json")
        
        # we delete the temporary file
        shutil.rmtree(temp_path)

    spark.stop()
    print("Pipeline finished successfully!")

    