import csv

# Read table names from CSV file (one table per line)
csv_path = "/path/to/tables.csv"
with open(csv_path, "r") as f:
    reader = csv.reader(f)
    table_names = [row[0].strip() for row in reader if row]

ddl_statements = []

# Type mapper from Spark to MonetDB
from pyspark.sql.types import *

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

# Process each table
for table_name in table_names:
    try:
        df = spark.table(table_name)
        schema = df.schema
        columns_def = ",\n  ".join([f"{field.name} {spark_to_monetdb_type(field.dataType)}" for field in schema])
        create_stmt = f"CREATE TABLE {table_name} (\n  {columns_def}\n);"
        ddl_statements.append(create_stmt)
        print(f"\n-- DDL for {table_name} --\n{create_stmt}")
    except Exception as e:
        print(f"⚠️ Failed to process {table_name}: {e}")

# Optional: Write to file
output_file = "/tmp/monetdb_create_statements.sql"
with open(output_file, "w") as f:
    f.write("\n\n".join(ddl_statements))

print(f"\n✅ All CREATE TABLE statements written to: {output_file}")
