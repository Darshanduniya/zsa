# List of Spark SQL table names you want to convert
table_names = ["table1", "table2", "table3"]  # Replace with your actual table names

ddl_statements = []

for table_name in table_names:
    df = spark.table(table_name)
    schema = df.schema

    def spark_to_monetdb_type(spark_type):
        from pyspark.sql.types import *
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
            return "VARCHAR"

    columns_def = ",\n  ".join([f"{field.name} {spark_to_monetdb_type(field.dataType)}" for field in schema])
    create_stmt = f"CREATE TABLE {table_name} (\n  {columns_def}\n);"
    
    ddl_statements.append(create_stmt)
    print(f"\n-- DDL for {table_name} --\n{create_stmt}")

# Optional: Write to file
with open("/tmp/monetdb_create_statements.sql", "w") as f:
    f.write("\n\n".join(ddl_statements))
