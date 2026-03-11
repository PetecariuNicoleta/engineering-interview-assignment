# In this file I have defined some functions that are used in the pipeline. 
# I think this method makes the code much easier to understand and read even for someone non-technical.

from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, DateType

def clean_data(df, dataset_name):   
    if dataset_name == 'races':
        df = df.dropDuplicates(['raceid'])
        df = df.dropna(subset=['raceid', 'year', 'name'])
        
        if 'date' in df.columns:
            df = df.withColumn('date', col('date').cast(DateType()))
            df = df.dropna(subset=['date'])
    
    elif dataset_name == 'results':
        df = df.dropDuplicates(['resultid'])
        df = df.dropna(subset=['resultid', 'raceid', 'driverid'])
        
        if 'position' in df.columns:
            df = df.withColumn("position",
                when(col("position").isin("null", ""), None)\
                    .otherwise(col("position").cast(IntegerType()))
            )
            df = df.dropna(subset=['position'])  
    
    return df