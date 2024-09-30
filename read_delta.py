try:
    import pyspark
    from delta import *
    from pyspark.sql.functions import parse_json
    from pyspark.sql.functions import try_variant_get
except Exception as e:
    print("Error ", e)

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Read data from the Delta table
delta_table_path = "/Users/sshah/IdeaProjects/poc-projects/lakehouse/data/delta-table"
delta_df = spark.read.format("delta").load(delta_table_path)

# Extracting values from the JSON variant using try_variant_get
delta_df.select(
    try_variant_get("json_var", "$.state", "STRING").alias("state"),
    try_variant_get("json_var", "$.employee_name", "STRING").alias("employee_name")
).show()


delta_df.count()

