from pyspark.sql import SparkSession
from pyspark.sql.types import *
import csv

# Step 1: Start Spark session
spark = SparkSession.builder \
    .appName("SparkToMonetDBSchema") \
    .enableHiveSupport() \
    .getOrCreate()

# Step 2: Read table names from CSV file (one table name per line)
csv_path = "/path/to/tables.csv"  # ← Replace with your actual path
with open(csv_path, "r") as f:
    reader = csv.reader(f)
    table_names = [row[0].strip() for row in reader if row]

# Step 3: Spark-to-MonetDB type conversion
def spark_to_monetdb_type(spark_type):
    if isinstance(spark_type, StringType):
        return "VARCHAR"
    elif isinstance(spark_type, IntegerType):
        return "INT"
    elif isinstance(spark_type, LongType):
        return "BIGINT"
    elif isinstance(spark_type, FloatType):
        return "FLOAT"
    elif isinstance(spark_type, DoubleType):
        return "DOUBLE"
    elif isinstance(spark_type, BooleanType):
        return "BOOLEAN"
    elif isinstance(spark_type, TimestampType):
        return "TIMESTAMP"
    elif isinstance(spark_type, DateType):
        return "DATE"
    elif isinstance(spark_type, DecimalType):
        return f"DECIMAL({spark_type.precision}, {spark_type.scale})"
    elif isinstance(spark_type, BinaryType):
        return "BLOB"
    elif isinstance(spark_type, ByteType):
        return "TINYINT"
    else:
        return "VARCHAR"  # Fallback

# Step 4: Generate CREATE TABLE statements
ddl_statements = []

for table_name in table_names:
    try:
        df = spark.table(table_name)
        schema = df.schema
        columns_def = ",\n  ".join([
            f"{field.name} {spark_to_monetdb_type(field.dataType)}" for field in schema
        ])
        create_stmt = f"CREATE TABLE {table_name} (\n  {columns_def}\n);"
        ddl_statements.append(create_stmt)
        print(f"\n-- DDL for {table_name} --\n{create_stmt}")
    except Exception as e:
        print(f"⚠️ Error processing table '{table_name}': {e}")

# Step 5: Write all DDLs to a .sql file
output_path = "/tmp/monetdb_create_statements.sql"  # Change path if needed
with open(output_path, "w") as f:
    f.write("\n\n".join(ddl_statements))

print(f"\n✅ DDLs written to: {output_path}")
