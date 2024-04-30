-- Databricks notebook source
-- MAGIC %python
-- MAGIC # importing order details
-- MAGIC
-- MAGIC file_location = "/FileStore/tables/Order-1.csv"
-- MAGIC file_type = "csv"
-- MAGIC
-- MAGIC infer_schema = "True"
-- MAGIC first_row_is_header = "True"
-- MAGIC delimiter = ","
-- MAGIC
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_location)
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # creating temp view Order_csv
-- MAGIC df.createOrReplaceTempView("Order_csv")

-- COMMAND ----------

-- inspecting the temp table Order_csv
select * from order_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # importing Shipping data
-- MAGIC file_location = "/FileStore/tables/Shipping-1.json"
-- MAGIC file_type = "json"
-- MAGIC
-- MAGIC infer_schema = "True"
-- MAGIC first_row_is_header = "True"
-- MAGIC multiline="True"
-- MAGIC
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("multiline", multiline) \
-- MAGIC   .load(file_location)
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # creating Shipping_json temp view
-- MAGIC df.createOrReplaceTempView("Shipping_json")

-- COMMAND ----------

-- inspecting Shipping_json
select * from shipping_json

-- COMMAND ----------

-- MAGIC %md
-- MAGIC download package spark-excel version 2.12 from MAVEN central in the cluster library.
-- MAGIC Reason to choose this version is based on scala version we are using. In general scala and spark-excel version needs to be same.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # importing customer data 
-- MAGIC file_location='dbfs:/FileStore/tables/Customer-2.xls'
-- MAGIC df = spark.read.format("com.crealytics.spark.excel").option('header','true').load(file_location)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # displaying the customer dataframe
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # creating Temp View Customer_xls
-- MAGIC df.createOrReplaceTempView("Customer_xls")

-- COMMAND ----------

-- inspecting Customer_xls
select * from customer_xls

-- COMMAND ----------

-- checking missing values in Order_csv
select * from order_csv where amount is null or item is null or order_id is null or customer_id is null

-- COMMAND ----------

-- checking for duplicates in Order_csv
-- although we dont need to add order by clause in partition but it is throwing AnalysisException therefore had to add it
with temp1 as (
  select * , row_number() over(partition by order_id order by order_id) as rnk from order_csv
)
select * from temp1 where rnk>1

-- COMMAND ----------

-- to check if there is any customer id which is there in Order_csv but not there in Customer_xls
with temp1 as (select o.customer_id as c_o,c.customer_id as c_c
from order_csv o full join customer_xls c on o.customer_id=c.customer_id)
select c_o,c_c from temp1 where c_c is null

-- COMMAND ----------

-- checking max, min, average, 90 percentile for column Amount in Order_csv
select max(amount) as max_amount,min(amount) as min_amount, avg(amount) as average_amount, round(percentile_cont(0.9) within group (order by amount),5) as percentile_90 from order_csv

-- COMMAND ----------

-- count of orders and total amount spent by each customer in Order_csv
select customer_id, count(order_id) as order_count,sum(amount) as total_amount from order_csv group by customer_id order by total_amount desc

-- COMMAND ----------

-- checking for null values in Shipping_json
select * from shipping_json where customer_id is null or shipping_id is null or status is null

-- COMMAND ----------

-- checking for duplicate values in Shipping_json
-- although we dont need to add order by clause in partition but it is throwing AnalysisException therefore had to add it
with temp1 as (
  select *, row_number() over(partition by shipping_id order by shipping_id)as rnk from shipping_json
)
select * from temp1 where rnk>1

-- COMMAND ----------

-- checking is there any customer_id present in Shipping_json but not in Customer_xls
with temp1 as (select s.customer_id as c_s,c.customer_id as c_c
from shipping_json s full join customer_xls c on s.customer_id=c.customer_id)
select c_s,c_c from temp1 where c_c is null

-- COMMAND ----------

-- checking for how many customer status is pending or not pending 
select status, count(customer_id) from shipping_json group by status

-- COMMAND ----------

-- checking null values in Customer_xls
select * from customer_xls where customer_id is null or first is null or last is null or age is null or country is null

-- COMMAND ----------

-- checking for duplicates in Customer_xls
-- although we dont need to add order by clause in partition but it is throwing AnalysisException therefore had to add it
with temp1 as(
  select *, row_number() over(partition by customer_id order by customer_id) as rnk from customer_xls
)

select * from temp1 where rnk>1

-- COMMAND ----------

-- checking for maximum minimum and average age of customers ordering
select max(age), min(age), round(avg(age)) from customer_xls

-- COMMAND ----------

-- checking how many customers ordered from different country
select country, count(customer_id) from customer_xls group by country

-- COMMAND ----------

-- the total amount spent and the country for the Pending delivery status for each country.
select c.country, sum(o.amount) as total_amount
from customer_xls c
join order_csv o on c.customer_id=o.customer_id
join shipping_json s on c.customer_id=s.customer_id
where s.status='Pending'
group by c.country order by 2 desc

-- COMMAND ----------

-- the total number of transactions, total quantity sold, and total amount spent for each customer, along with the product details.
-- although we dont need to add order by clause in partition but it is throwing AnalysisException therefore had to add it
with temp1 as (select customer_id, count(order_id) as num_transaction, count(item) as quantity_sold,sum(amount) as total_amount from order_csv group by 1),
temp2 as(select customer_id, collect_list(item)over(partition by customer_id order by customer_id) as lst from order_csv),
temp3 as (select t1.customer_id,t1.num_transaction,t1.quantity_sold,t1.total_amount,t2.lst,
row_number()over(partition by t1.customer_id,t1.num_transaction,t1.quantity_sold,t1.total_amount,t2.lst order by t1.customer_id,t1.num_transaction,t1.quantity_sold,t1.total_amount,t2.lst) as rnk
from temp1 t1 join temp2 t2 on t1.customer_id=t2.customer_id)
select customer_id,num_transaction,quantity_sold,total_amount,lst from temp3 where rnk=1

-- COMMAND ----------

-- the maximum product purchased for each country.
with temp1 as (select c.country, o.item, count(o.item) as item_count, dense_rank() over(partition by c.country order by count(o.item) desc) as rnk
from customer_xls c
join order_csv o on c.customer_id=o.customer_id
group by 1,2)
select country,item, item_count from temp1 where rnk=1 order by 3

-- COMMAND ----------

-- the most purchased product based on the age category less than 30 and above 30.
with temp1 as (select o.item,count(o.item) as cnt_item, dense_rank() over(order by count(o.item) desc) as rnk
from order_csv o
join customer_xls c on o.customer_id=c.customer_id
where c.age<30
group by 1),
temp2 as(
  select o.item,count(o.item) as cnt_item, dense_rank() over(order by count(o.item) desc) as rnk
from order_csv o
join customer_xls c on o.customer_id=c.customer_id
where c.age>30
group by 1
)
select temp1.item as less_than_30, temp2.item as above_30 
from temp1 join temp2 where temp1.rnk=1 and temp2.rnk=1

-- COMMAND ----------

-- the country that had minimum transactions and sales amount
with temp1 as (select c.country,count(o.order_id) as transactions,sum(o.amount)as sales, dense_rank()over(order by count(o.order_id),sum(o.amount)) as rnk
from customer_xls c
join order_csv o on c.customer_id=o.customer_id group by 1)
select country,transactions as min_trans, sales as min_sales from temp1 where rnk=1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q1. Verify the accuracy, completeness, and reliability of source data. 
-- MAGIC
-- MAGIC Ans1. There is no missing values or duplicates in all the 3 tables. Checked maximum, minimum, average and 90 percentile values to check for outliers or anomilies in all the 3 tables. Chedked if there is any issue with the data types which is completely fine although we can implement varchar in place of string as it will take less space to store  based on data bricks usage. Checked if there are customer_id in Order_csv and Shipping_json table that are not in Customer_xls but found none. 
-- MAGIC We can divide the Order_csv table into 2 tables orders and items that will normalize the table more and will help in optimization of the queries when they use this tables containg large set of data
-- MAGIC we can also add multiple constraints to the tables like not null and unique for the primary keys so that there is no null values and all the values provided are unique
-- MAGIC
-- MAGIC
-- MAGIC Q2. Based on your findings, define and outline the requirements for anticipated datasets, detailing the necessary data components.'
-- MAGIC
-- MAGIC Ans2. Based on my findings the new table that we will create item_type will contain item_id, amount and item_name where item_id will act as primary key, amount will contain amount of each item and item_name will provide the name of the items. This will make the change in Order_csv table where we will not keep item and amount columns we will add the item_id as a foreign key in the table refering to the item table.
-- MAGIC
-- MAGIC
-- MAGIC Q3. Develop the data models to effectively organise and structure the information and provide a detailed mapping of existing data flows, focussing on the areas of concern.
-- MAGIC
-- MAGIC Ans3. we can use a snowflake dimensional modeling as we will make the Customer_xls as fact table and Order_csv and Shipping_json as dimension table using the Column customer_id as the Key to connect all the 3 tables. It will give us liberty to add more dimension and sub dimension tables and we will be able to perform Slow Changing Dimension techinique when we are adding the data to the tables like adding the item table to the order_csv table as a sub dimension table.
-- MAGIC
-- MAGIC Q4. Communicate the findings and insights to stakeholders in a visually comprehensive manner.
-- MAGIC
-- MAGIC Ans4. a)The total amount spent by USA, UK and UAE is 65500, 136300, 53800 respectively where delivery status is pending.
-- MAGIC
-- MAGIC b) The maximum product purchased by USA, UK and UAE is 18,24,12 respectively.
-- MAGIC
-- MAGIC c) Mouse is purchased by people less than 30 years of age and Keyboard is purchased the by people more than 30 years of age.
-- MAGIC
-- MAGIC d) UAE is the country with minimum number of transaction 40 and minimum number of sales amount 49950.
-- MAGIC
-- MAGIC e) Maximum ampunt spent by a person is 12000, minimum amount spent by a person is 200 and average spent is 2130.
-- MAGIC
-- MAGIC f) Maximum age of a person ordering is 80 and minimum age of a person ordering is 18.
-- MAGIC
-- MAGIC g) There are 100 customer whose delivery status is showing delivered and 150 customers whose delivery status is showing pending.
-- MAGIC
-- MAGIC h) Count of customers who ordered from USA is 101, from UK is 100 and UAE is 49.
-- MAGIC
-- MAGIC i) I have added the charts to each of the cells as databricks provide the liberty to add visualisation on cell outputs.
-- MAGIC
-- MAGIC Q5. What will be your insights to other peer teams of Data Engineers, Data Scientists and other technical and non-technical stakeholders?
-- MAGIC
-- MAGIC Ans5. To the technical team and data engineering team, I will mention them to bifercate the Order_csv table into items and order table and use the snowflake model along with the constraints that can be added.
-- MAGIC To the non-technical team I will provide the analysis and insights that I gathered above to help them better understand and help with there problem statement.

-- COMMAND ----------

