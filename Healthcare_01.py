# Databricks notebook source
display(dbutils.fs.ls("/"))

# COMMAND ----------

poly_holders_01=spark.read.csv("file:/Workspace/Users/arshiya251995_gmail.com#ext#@arshiya251995gmail.onmicrosoft.com/datasets/01.poly_holders.csv",header=True,inferSchema=True)

display(poly_holders_01)

# COMMAND ----------

poly_holders_01.printSchema()

# COMMAND ----------

claims_02=spark.read.csv("file:/Workspace/Users/arshiya251995@gmail.com/datasets/02.Claims.csv",header=True,inferSchema=True)

display(claims_02)

# COMMAND ----------

medical_expenses_03=spark.read.csv("file:/Workspace/Users/arshiya251995@gmail.com/datasets/03.Medical_Expenses.csv",header=True,inferSchema=True)

display(medical_expenses_03)

# COMMAND ----------

disability_benefits_04=spark.read.csv("file:/Workspace/Users/arshiya251995@gmail.com/datasets/04.Disability_benefits.csv",header=True,inferSchema=True)

display(disability_benefits_04)

# COMMAND ----------

accidental_benefits_05=spark.read.csv("file:/Workspace/Users/arshiya251995@gmail.com/datasets/05.Accidental_benefits.csv",header=True,inferSchema=True)

display(accidental_benefits_05)

# COMMAND ----------

accidental_information_06=spark.read.csv("file:/Workspace/Users/arshiya251995@gmail.com/datasets/06.Accidental_information.csv",header=True,inferSchema=True)

display(accidental_information_06)

# COMMAND ----------

from pyspark.sql.functions import initcap


# COMMAND ----------

poly_holders_01=poly_holders_01.withColumn("Name",initcap("Name"))

display(poly_holders_01)

# COMMAND ----------



# COMMAND ----------

accidental_information_06=accidental_information_06.withColumn("Name",initcap("Name"))

display(accidental_information_06)

# COMMAND ----------


