# Databricks notebook source
# MAGIC %md
# MAGIC # Spark SQL TPC-DS
# MAGIC ## Testing decision support queries on a Spark Databricks deployment.
# MAGIC 
# MAGIC The purpose of this notebook is to execute the TPC-DS benchmark on a Spark environment in the cloud. Modern implementations of data warehouses are almost certainly on the cloud. Let's evaluate how they behave assuming a small system (for testing and cost purposes). This testing framework works with scale factors of 1, 2, 3 and 4GB sizes.
# MAGIC 
# MAGIC For the written report, we will run tests on the Databricks basic cluster with:
# MAGIC - Databricks 11.0
# MAGIC - Spark 3.3.0
# MAGIC - Scala 2.1.12
# MAGIC - Driver size m5d.large
# MAGIC - 8GB Memory
# MAGIC - 2 Cores
# MAGIC 
# MAGIC Feel free to create your own clusters and run the notebook on a custom cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Variables declaration
# MAGIC Please run all the cells in order to perform the experiments. Change the data size when needed.

# COMMAND ----------

# MAGIC %pip install joblib

# COMMAND ----------

# Import statements
import pyspark
import boto3
from io import StringIO
import os
import pandas as pd
import logging
from pyspark import SparkContext
from pyspark.sql import Row, SQLContext, SparkSession, types
import time
from pyspark.sql.functions import col, to_date, to_timestamp, coalesce
from pyspark.sql.types import IntegerType, DoubleType, StringType, BooleanType, DateType, TimestampType, DecimalType
import numpy as np

spark.conf.set("fs.s3a.access.key", "AKIATWBJZ4QMRIKK377C")
spark.conf.set("fs.s3a.secret.key", "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")
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

#New Keys
#AKIAZ3MGMZ6KVSM4HNNG
#SLzF2m2TOKnosyKTNvCJTMfAsGHrN/PtxEouQkjQ

# Variable definition
tables = ["call_center", "catalog_page", "catalog_returns", "catalog_sales",
             "customer_address", "customer_demographics", "customer", "date_dim",
              "household_demographics", "income_band", "inventory", "item",
             "promotion", "reason", "ship_mode", "store_returns", "store_sales", "store",
             "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site"
            ]

data_size = "1G"  # 2GB 4GB
s3_bucket = "s3a://tpcds-spark/"
db_name = "tpcds2"
schemas_location = "scripts/queries/table/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the schema and loading the tables
# MAGIC 
# MAGIC The TPCDS schema has been defined under the /scripts/queries/table/ of the repo. These sql templates will create a Hive Metastore table inside of the Databricks cluster. Once created, we are telling Spark to pull the data (stored in parquet format) from the corresponding s3 bucket. The data was generated using the dbsdgen tooling provided by TPCDS. For this experiment, we created samples for 1GB, 2GB and 4GB scale factors.  
# MAGIC Once we have created the metastore, we can test Spark SQL decision support capabilities with the tpcds queries.

# COMMAND ----------

def validate_s3_file(data_path):
    try:
        file_status = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).getContentSummary(spark._jvm.org.apache.hadoop.fs.Path(data_path))
        if file_status.getLength() > 0:
            print(f"File exists and has data: {data_path}")
            return True
        else:
            print(f"File exists but is empty: {data_path}")
            return False
    except Exception as e:
        print(f"Failed to access file {data_path}: {e}")
        return False

# Create database and tables

def impose_schema(df1, df2):
    schema_df2 = {field.name: field.dataType for field in df2.schema.fields}

    # Para cada columna de df1, aplicamos la conversión al tipo de df2
    for col_num, column_df2 in enumerate(df2.columns):
        # Comprobar si la columna existe en df1
        col_name = df1.columns[col_num]
        data_type = schema_df2[column_df2]

        # Dependiendo del tipo de datos, realizar la conversión adecuada
        if isinstance(data_type, IntegerType):
            df1 = df1.withColumn(col_name, col(col_name).cast(IntegerType()))
        elif isinstance(data_type, DoubleType):
            df1 = df1.withColumn(col_name, col(col_name).cast(DoubleType()))
        elif isinstance(data_type, DecimalType):
            precision = data_type.precision
            scale = data_type.scale
            # Convertir la columna a DecimalType con la precisión y escala correctas
            df1 = df1.withColumn(col_name, col(col_name).cast(DecimalType(precision, scale)))
        elif isinstance(data_type, DateType):
            # Para DateType, usamos to_date() con el formato proporcionado
            df1 = df1.withColumn(col_name, to_date(col(col_name), "yyyy-MM-dd"))
        elif isinstance(data_type, TimestampType):
            # Para TimestampType, usamos to_timestamp() con el formato proporcionado
            df1 = df1.withColumn(col_name, to_timestamp(col(col_name), "yyyy-MM-dd HH:mm:ss"))
        elif isinstance(data_type, BooleanType):
            df1 = df1.withColumn(col_name, col(col_name).cast(BooleanType()))
        elif isinstance(data_type, StringType):
            # Si es StringType, no necesitamos hacer nada, ya es String
            pass
        # Puedes agregar más tipos según los necesites
    return df1



def insert_data(s3_path, relation):
    df1 = spark.createDataFrame(spark.read.csv(s3_path, sep="+").toPandas().iloc[:, :-1])
    df2 = spark.table(relation)
    df1 = impose_schema(df1, df2)
    df2 = df2.union(df1)
    df2.write.mode("overwrite").saveAsTable(relation)

def create_database(name=db_name):
    pass
    #spark.sql(f"DROP DATABASE IF EXISTS {name} CASCADE")
    #spark.sql(f"CREATE DATABASE {name}")
    #spark.sql(f"USE {name}")
    
def create_table(relation, s3_bucket=s3_bucket, db_name=db_name, schemas_location=schemas_location, data_size=data_size, spark=spark):
    use_database = f"USE `tpcds2`.`{data_size.lower()}`"
    spark.sql(use_database)
    schema_path = f"{schemas_location}{relation}.sql"
    data_path = f"{s3_bucket}/csv_data/{data_size}/{relation}.csv"

#     if not validate_s3_file(data_path):
#         raise Exception(f"S3 file for {relation} does not exist or is empty.")

    with open(schema_path) as schema_file:
        queries = schema_file.read().strip("\n").replace(f"create table {relation}", f'create table `tpcds2`.`{data_size.lower()}`.`{relation}`').replace(f"exists {relation}", f"exists `tpcds2`.`{data_size.lower()}`.`{relation}`").lower().replace("parquet", "delta").split(";")
    for query in queries:
        print(query)
        spark.sql(query)
        if "drop" not in query:
            table_name = f'`tpcds2`.`{data_size.lower()}`.`{relation}`'
            insert_data(data_path, table_name)
        

def create_tables(relations, s3_bucket, db_name, schemas_location, data_size, spark):
    for relation in relations:
        create_table(relation, 
                     s3_bucket=s3_bucket, 
                     db_name=db_name, 
                     schemas_location=schemas_location, 
                     data_size=data_size, 
                     spark=spark)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Executing the queries and recording performance metrics
# MAGIC In this section we will execute the TPC-DS queries provided to us. First, we parse the queries from generated queries file from the templates. For each data size we will run each query and save the result to csv file. We will also collect statistical data regarding the execution time of each query.

# COMMAND ----------

import csv

def save_list_results(url, data):
    data_frame = spark.createDataFrame(Row(**x) for x in data)
    data_frame.write.partitionBy('run_id').format("csv").mode("overwrite").option("header", "true").save(url)
    
def save_stats(url, data):
    data_frame = spark.createDataFrame(Row(**x) for x in data)
    data_frame.write.format("csv").mode("overwrite").option("header", "true").save(url)

# COMMAND ----------

def save_execution_plan(query, data_size, filename):
    try:
        execution_plan_path = f"s3a://tpcds-spark/execution_plan/{data_size}/{filename}"
        df = spark.sql(query)
        execution_plan = df._jdf.queryExecution().executedPlan().toString()
        execution_plan_rdd = spark.sparkContext.parallelize([execution_plan])
        execution_plan_rdd.saveAsTextFile(execution_plan_path)
    except Exception as e:
        print(f"An error occurred: {e}")

# COMMAND ----------

from joblib import Parallel, delayed
from multiprocessing.pool import Pool
import traceback

NUM_THREADS = 5
NUM_POOLS = 10

def load_queries(path_to_queries, data_size) -> list:
    tables = ["call_center", "catalog_page", "catalog_returns", "catalog_sales",
             "customer_address", "customer_demographics", "customer", "date_dim",
              "household_demographics", "income_band", "inventory", "item",
             "promotion", "reason", "ship_mode", "store_returns", "store_sales", "store",
             "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site"
            ]

    with open(path_to_queries) as file_obj:
        comment_count = 0
        queries = []
        query_lines = []
        for line in file_obj:
            if comment_count == 0 and "--" in line:  # it is a comment and therefore the beginning or end of a query
                comment_count += 1
                query_lines = []
                continue
            elif comment_count == 1 and "--" not in line:  # we are reading part of the query
                query_lines.append(line)
            elif comment_count == 1 and "--" in line:  # it is the second comment indicating this is the end of the query
                query = "".join(query_lines)
                for table in tables:
                    query = query.replace(f"${table}$", f"`tpcds2`.`{data_size}`.`{table}`")
                queries.append(query)
                comment_count = 0


    return queries

def run_query(run_id, query_number, queries, path_to_save_results, data_size, specific_queries=[], print_result=False):
    
    try:
        print(f"Running query {query_number} for scale factor {data_size}, saving results at {path_to_save_results}")
        
        # the extra query here should also remove cache
        #execution_plan_filename = f"{query_number}.txt"
        #save_execution_plan(queries[query_number-1], data_size, execution_plan_filename)
        print(queries[query_number-1])
        result = spark.sql(queries[query_number-1])
        count = result.count()

        execution_times = []
                
        for i in range(30):
            start = time.time()
            result = spark.sql(queries[query_number-1])
            count = result.count()
            end = time.time()
            elapsed_time = float(end - start)
            execution_times.append(elapsed_time)
        
        elapsed_time = float(np.median(execution_times))

        result.write.format("csv").mode("overwrite").option("header", "true").save(path_to_save_results.format(size=data_size, query_number=query_number))
        
        stats = {
            "run_id": run_id,
            "query_id": query_number,
            "start_time": start,
            "end_time": end,
            "elapsed_time": elapsed_time,
            "runtimes": execution_times,
            "row_count": count,
            'error': False
        }
        if (print_result is True):
            print(stats)
            print(result.show())
        return stats
    except Exception as e:
        print(e)
        return {
            "run_id": run_id,
            "query_id": query_number,
            "start_time": time.time(),
            "end_time": time.time(),
            "elapsed_time": 0.0,
            "runtimes":np.zeros(30),
            "row_count": 0,
            "error": True
        }

def run_queries(run_id, queries, path_to_save_results, path_to_save_stats, data_size, specific_queries=[], print_result=False, get_distributions=False):
#     with Pool(processes=NUM_POOLS) as pool:
#         stats = pool.starmap(run_query, [(run_id, i+1, queries, path_to_save_results, data_size, print_result) for i in range(len(queries))])
    
    stats = Parallel(n_jobs=NUM_THREADS, prefer="threads")(delayed(run_query)(run_id, i+1, queries, path_to_save_results, data_size, specific_queries, print_result) for i in range(len(queries)))
    
    if get_distributions:
        s3 = boto3.client('s3',
                    aws_access_key_id='AKIATWBJZ4QMRIKK377C',
                    aws_secret_access_key='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')
        bucket_name = 'tpcds-spark'
        s3_file_key = f"csv_data/{data_size}/runtime_distributions.csv"
        csv_obj = s3.get_object(Bucket=bucket_name, Key=s3_file_key)
        csv_data = csv_obj['Body'].read().decode('utf-8')
        execution_times_df = pd.read_csv(StringIO(csv_data), index_col=0)
        
        for stat in stats:
            execution_times = stat.pop("runtimes")
            query_number = stat["query_id"]
            execution_times_df[str(query_number)] = execution_times
            
        csv_buffer = StringIO()
        execution_times_df.to_csv(csv_buffer)
        s3.put_object(Bucket=bucket_name, Key=s3_file_key, Body=csv_buffer.getvalue())
    
    
    save_list_results(path_to_save_stats, stats)

# COMMAND ----------

def run(data_sizes=['1G'], specific_queries=[], run_tests=False, load_data=True, get_distributions=False):    
    for i, data_size in enumerate(data_sizes):
        queries_path = "scripts/queries_generated/queries_{size}_Fixed.sql".format(size="1G")
        result_path = "s3a://tpcds-spark/results/{size}/{query_number}/test_run_csv"
        stats_path = "s3a://tpcds-spark/results/{size}/test_run_stats_csv".format(size=data_size)
        
        if load_data:
            start_create_db = time.time()
            # Create metastore for the given size
            create_database(name=db_name)
            create_tables(tables, s3_bucket, db_name, schemas_location, data_size, spark)
            end_create_db = time.time()
        else:
            start_create_db = 0
            end_create_db = 0

        if run_tests:
            # Load queries for the given size
            
            queries = load_queries(queries_path, data_size)
    #         queries_need_to_be_fixed = [queries[13], queries[22], queries[23], queries[34], queries[38]]

            start_run = time.time()
            run_queries(i+1, queries, result_path, stats_path, data_size, specific_queries, run_tests, get_distributions)
            end_run = time.time()
                
            # Saving the overall stats to csv file
            overall_stats = [{
                'batch_id': i+1,
                'create_db_time': end_create_db - start_create_db,
                'run_query_time': end_run - start_run
            }]

            overall_stats_path = "s3a://tpcds-spark/results/{size}/overall_stats_csv".format(size=data_size)
            save_stats(overall_stats_path, overall_stats)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the pipeline

# COMMAND ----------

# Please don't run full pipeline unless ready, try with run(data_sizes=['1G'])
data_sizes = ['1G', '2G', '3G', '4G']#, '10G', '20G', '30G']
#data_sizes = ['1G']
specific_queries = []

run(data_sizes=data_sizes, run_tests=True, load_data=False, get_distributions=True)
#run(data_sizes=['1G'])

