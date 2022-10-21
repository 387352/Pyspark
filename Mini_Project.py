# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType
import pyspark.sql.functions as f

spark = SparkSession.builder.appName("Mini Project").getOrCreate()


# COMMAND ----------

df = spark.read.options(header="True",inferSchema="True").csv("/FileStore/tables/OfficeDataProject.csv")
#df.show()
#df.select(df.employee_id).distinct().count()
#df.select(df.department).distinct().count()
#df.select(df.department).distinct().show()
#df.groupBy(df.department).agg(f.count('*').alias('num of employees in each dept')).show()
#df.groupBy(df.state).agg(f.count('*').alias('num of employees in each state')).show()
#df.groupBy(df.state,df.department).agg(f.count('*').alias('num of employees in each state')).show()
#df.groupBy(df.department ).agg(f.min(df.salary).alias('minsalary'),f.max(df.salary).alias('maxsalary')).orderBy(f.col("minsalary")).show()
avg_bonus= df.filter(df.state=="NY").groupBy(df.state).agg(f.avg(df.bonus).alias("avg_bonus")).select(f.col("avg_bonus")).collect()[0]["avg_bonus"]
df.filter((df.state =="NY") & (df.department == "Finance")&(df.bonus > avg_bonus) ).show()
#df.printSchema()

# COMMAND ----------

def IncSalary(salary,age):
  if age > 45:
    salary = salary+500
  return salary

udfIncSalary = f.udf(lambda x,y:IncSalary(x,y),IntegerType())

df.withColumn("salary",udfIncSalary(f.col("salary"),f.col("age"))).show()

# COMMAND ----------

df.filter(df.age > 45).write.csv("/FileStore/tables/output.45")
