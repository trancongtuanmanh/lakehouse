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
    schema_data = data['schema']

    return schema_data

def create_schema_gcs(schema_data):
    '''
    This function creates the schema for the spark dataframe

    Parameters
    ----------
    schema_data : bronze, silver, gold
    '''
        # Get the schema data
    for schema in schema_data:
        schema_name = schema['name']
        for path in schema['path']:
            gcs_path = path['path']
            try:
                spark.sql(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
                print(f"Schema: {schema_name} dropped")
            except Exception as error:
                raise error
    spark.sql("show schemas").show()

def main():
    '''
    This function is the main function
    '''
    schema_data = python_read_yml(sources)
    create_schema_gcs(schema_data)

if __name__ == "__main__":
    main()
