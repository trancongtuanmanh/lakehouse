import yaml
from pyspark.sql import SparkSession
from delta import *
from settings import jar_path, key_path, sources

builder = SparkSession.builder\
    .appName('spark-bigquery-demo') \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", "gs://lakehouse-prod/spark-warehouse/")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

def python_read_yml(sources):
    '''
    This function reads the schema name from the config file
    '''
    # Open the YAML file
    with open(sources) as file:
        # Load the YAML data
        data = yaml.load(file, Loader=yaml.FullLoader)

    # Access the schema and table data
    table_data = data['table']

    return table_data

def create_delta_table(table_data):
    '''
    This function creates a delta table
    '''
    # spark.sql('create database if not exists bronze')
    for table in table_data:
        table_name = table['name']
        table_schema = table['schema']
        table_source = table['source']
        for path in table['path']:
            table_path = path['path']
            try:
                spark.read.json(f'{table_source}').write.format("delta").mode("overwrite").save(f'{table_path}')
                spark.sql(f"CREATE TABLE IF NOT EXISTS {table_schema}.{table_name} USING DELTA LOCATION '{table_path}'")
                print(f"Delta table: {table_schema}.{table_name} created")
            except Exception as error:
                print(f"Error creating delta table {table_schema}.{table_name}")
                raise error

def main():
    '''
    This function is the main function
    '''
    table_data = python_read_yml(sources)
    create_delta_table(table_data)

if __name__ == "__main__":
    main()
