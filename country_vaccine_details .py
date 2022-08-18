# Databricks notebook source
df_USA = spark.read.format("csv") \
  .option("inferSchema", "false") \
  .option("header", "true") \
  .option("sep", ",") \
  .load("/FileStore/tables/USA.csv")
df_USA.createOrReplaceTempView("USA_csv")

# COMMAND ----------

df_IND = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("sep", ",") \
  .load("/FileStore/tables/IND.csv")
df_IND.createOrReplaceTempView("IND_csv")

# COMMAND ----------

df_AUS= spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .load("/FileStore/tables/AUS.xlsx")
df_AUS.createOrReplaceTempView("AUS_xlsx")

# COMMAND ----------

spark.sql("DROP TABLE IF  EXISTS Vaccination_Details");
spark.sql("CREATE TABLE IF NOT EXISTS Vaccination_Details(ID Integer,Country String,Patient_Name String,Vaccine_Type String,Date_of_Birth String,Date_of_Vaccination String)");

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Vaccination_Details(ID,Country,Patient_Name,Vaccine_Type,Date_of_Birth,Date_of_Vaccination)
# MAGIC SELECT id,"India",name,VaccinationType, cast(DOB as date),cast(VaccinationDate as date) FROM IND_csv
# MAGIC WHERE id IS NOT NULL and VaccinationType IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Vaccination_Details(ID,Country,Patient_Name,Vaccine_Type,Date_of_Birth,Date_of_Vaccination)
# MAGIC SELECT id,'USA',name,VaccinationType,NULL,cast(concat(concat(concat(substring(VaccinationDate,5,8),'-'),substring(VaccinationDate,0,2)),'-',substring(VaccinationDate,3,2)) as date)FROM USA_csv
# MAGIC WHERE id IS NOT NULL and VaccinationType IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Vaccination_Details(ID,Country,Patient_Name,Vaccine_Type,Date_of_Birth,Date_of_Vaccination)
# MAGIC SELECT `Unique ID`,"Australia",`Patient Name`,`Vaccine Type`,cast(concat(concat(concat(substring(`Date of Birth`,0,4),'-'),substring(`Date of Birth`,6,2),'-'),substring(`Date of Birth`,9,2)) as date),cast(concat(concat(concat(substring(`Date of Vaccination`,0,4),'-'),substring(`Date of Vaccination`,6,2),'-'),substring(`Date of Vaccination`,9,2)) as date) FROM AUS_xlsx
# MAGIC WHERE `Unique ID` IS NOT NULL and `Vaccine Type` IS NOT NULL

# COMMAND ----------

# DBTITLE 1,Total vaccination count by country  and vaccinationtype
# MAGIC %sql
# MAGIC SELECT country as CountryName,Vaccine_Type as VaccinationType,count(*) as  `No. of vaccinations` FROM Vaccination_Details
# MAGIC GROUP BY country,Vaccine_Type

# COMMAND ----------

# DBTITLE 1,% vaccination in each country 
# MAGIC %sql
# MAGIC --Assuming values for total population in each country is 100
# MAGIC SELECT country AS ountryName,(count(*)/100)*100 AS `% Vaccinated` FROM Vaccination_Details 
# MAGIC GROUP BY country

# COMMAND ----------

# DBTITLE 1,vaccination contribution by country 
# MAGIC %sql
# MAGIC SELECT country AS CountryName,count(*)/a.totalcount*100 AS` % Contribution`
# MAGIC FROM Vaccination_Details b
# MAGIC JOIN 
# MAGIC (SELECT count(*) AS totalCount FROM Vaccination_Details) a GROUP BY country,a.totalCount

# COMMAND ----------


