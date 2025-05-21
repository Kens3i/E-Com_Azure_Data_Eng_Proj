# End to End E-Commerce Data Engineering Project 💳
## Powered by Azure and Databricks

![](https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExYnpqbjUxbjlrMG52bDZydGV5cGNqMHhtNnB1YjJnbHNhOHNmbDA2NCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/fryY00CO4xCz4uJuDQ/giphy.gif)

## Table of Contents

1. [Overview](#overview) 
2. [Motivation](#motivation) 
3. [Functionalities Used](#functionalities-used)
4.  [Workflow](#workflow) 
5.  [Execution with Screenshots](#execution-with-screenshots) 
6.  [FAQ](#faq)

## Overview
In this project I implemented a **medallion (lakehouse) architecture** on Azure to ingest and refine data from multiple sources. In brief, the **Bronze** layer stores raw ingested data from multiple sources, the **Silver** layer holds cleaned and enriched data, and the **Gold** layer contains fully transformed, business-ready datasets. I structured the pipeline so that data flows progressively from Bronze → Silver → Gold, improving quality and consistency at each stage. This approach is recommended by Databricks/Azure for reliable enterprise data products. The schematic below (placeholder for architecture diagram) illustrates how I configured the flow:

![Architecture Overview.jpg](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/Architecture%20Overview.jpg?raw=true) _High-level medallion (Bronze/Silver/Gold) architecture with Azure Data Factory (ingestion), Databricks (processing), and Synapse (serving)._

![Dataset Overview.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/Dataset%20Overview.png?raw=true) _Dataset Overview_

-   **Bronze Layer (Raw Ingestion)** – All raw data is landed here with minimal processing. I used Azure Data Factory (ADF) pipelines to pull data from two sources: CSV files on GitHub, and a hosted MySQL database. Data is copied into Azure Data Lake Storage Gen2 in a _Bronze_ container/folder.
    
-   **Silver Layer (Cleaned/Enriched Data)** – In Databricks (Spark), I read the raw Bronze data from ADLS Gen2, applied cleaning, filtering, and joins, did enrichment of the tables using an external hosted MongoDB Database and wrote the refined tables back to ADLS in a _Silver_ container. 
    
-   **Gold Layer (Business-Ready Data)** – In Azure Synapse Analytics (serverless SQL pool), I exposed the Silver-level data for consumption. I created SQL **views** to produce business metrics (e.g. monthly sales, delivery KPIs) by querying the Silver Parquet tables via `OPENROWSET`. I then used **CETAS** (Create External Table As Select) to export those results into Parquet in a _Gold_ folder. Final external tables and views in Synapse make the data readily available for analytics and BI.
    
This layered design ensures data quality and traceability: raw inputs are always preserved in Bronze, intermediate analytics-friendly tables in Silver, and business-centric aggregates in Gold.

---


## Motivation
I designed and built this end-to-end pipeline to:
-   ✅ **Demonstrate a complete Azure-based data engineering workflow**, covering ingestion, transformation, and serving of data in a modern cloud architecture.
-   🔧 **Strengthen my hands-on skills** in:
    -   **Azure Data Factory** for orchestrating multi-source ingestion workflows.
    -   **Azure Databricks (Spark)** for performing scalable data transformations and enrichment.
    -   **Azure Synapse Analytics (serverless SQL)** for serving and aggregating curated data.
-   🌍 **Implement a real-world scenario** using Brazilian e-commerce customer order data to simulate how organizations consolidate data from:
    -   Public CSV files (GitHub)
    -   Relational databases (MySQL)
    -   NoSQL databases (MongoDB)
-   🔐 **Ensure secure and scalable architecture** by:
    -   Using **service principals** and **managed identities** for secure data access.
    -   Creating **parameterized pipelines** in ADF to handle dynamic, reusable workflows.
-   🏗️ **Follow best practices** in modern data architecture by:
    
    -   Implementing the **Medallion (Bronze/Silver/Gold) pattern** for layered data refinement and traceability.
    -   Using open formats like Parquet and tools like `OPENROWSET` and CETAS for optimized downstream consumption.
      
-   🧩 **Build a strong portfolio project** that showcases my ability to:
    -   Architect, implement, and document a complex, production-style data pipeline.
    -   Integrate multiple Azure services to deliver a reliable and secure end-to-end data solution.

---

## Functionalities Used

I leveraged various Azure services and tools, each serving a specific role in the pipeline:

**Azure Data Factory (ADF)**
Orchestrates and automates data ingestion pipelines. I used ADF **Copy Data** activities to fetch live CSVs from GitHub (via HTTP) and to extract data from the MySQL source. Parameterization in ADF enabled copying of multiple files and tables systematically.

**Azure Data Lake Storage Gen2 (ADLS Gen2)**
Serves as the central data lake. I stored raw (Bronze) data, cleaned (Silver) data, and final (Gold) data as Parquet files here. ADLS Gen2 provides hierarchical namespace and security.

**Azure Databricks (Spark)**
Performs Spark-based data processing. In Databricks notebooks, I read Bronze data from ADLS Gen2, executed Spark transformations (filter, join, aggregation), and wrote output to the Silver layer. I configured Spark with OAuth (service principal) to securely access ADLS.

**Azure Synapse Analytics (serverless SQL pool)**
Provides SQL-based analytics on data in the lake. I created database views and used **CETAS** (Create External Table As Select) to export Gold-level results back to ADLS Gen2 in Parquet. Synapse views enable BI tools (Power BI, etc.) or SQL clients to query the data.

**Azure Database for MySQL**
Relational source for transactional data (simulated e-commerce orders, payments, etc.). I populated this database (and its tables) to represent structured data input. ADF connected via the MySQL Linked Service to copy data from MySQL into the Bronze layer.

**MongoDB** 
NoSQL source for semi-structured data (e.g. customer reviews or product metadata). I used the MongoDB pipeline feature in ADF (Data Flow or Copy Activity) to ingest collections into Bronze. This demonstrated handling JSON-style data.

**GitHub (CSV files)**
Contains raw CSV data (e.g. order logs). I stored the CSV dataset (from Kaggle) on GitHub. ADF’s HTTP connector fetched the raw CSV from the GitHub raw URL into the Bronze storage.

**Python (Jupyter/Colab)**
I wrote Python scripts to _seed_ the MySQL and MongoDB sources with sample data. For example, using `mysql.connector` and `pymongo` in a notebook to insert the initial records. These scripts are not part of the automated pipeline but facilitated testing. Also created some custom tailored scripts to fetch live CSV data from GitHub using web scraping techniques and feed the data in ADF.

**Service Principal / Managed Identity**
Authentication mechanisms. I registered an Azure AD service principal for Databricks to access ADLS (via OAuth) and used the Synapse workspace’s **Managed Identity** to authorize reading/writing to ADLS Gen2. This avoids storing keys in code.

**Azure Key Vault (optional)**
For storing secrets like database passwords or service principal credentials, referenced by Databricks.

---


## Workflow
### Bronze Layer – Resource Setup and Data Ingestion

I start by provisioning the Azure resources: I create a Resource Group containing an **Azure Data Factory**, a **Databricks** workspace, an **ADLS Gen2** storage account (with `bronze`, `silver`, and `gold` containers), and an **Azure Synapse Analytics** workspace (serverless SQL pool). I also establish Linked Services/connections in ADF for my MYSQL data source.

-   **Azure Data Factory Pipelines:** I built ADF pipelines to ingest raw data into the Bronze storage.
	- First I created a linked service to get the base URL from GitHub and I can use the base URL with relative URLs via parameterization to fetch each and every CSVs. So when I was creating the copy activity, I setup a file-name parameter so that only the file names needs to be fed into the copy activity and the base url plus the file name will make the entire URL to fetch the file.
	- Now to get each and every .csv names from GitHub I created a custom python script using json and requests library, which first checks whether a successful connection is being established or not, and then filters out only the .csv files from the website and fetch as a form of JSON.
	- To get the .csv file names as an array output I created a Databricks notebook and executed a web-scraping script.
	- A Databricks notebook activity was added in the pipeline and the output of this will be the input of Copy activity.
	- But now the question is How to copy multiple CSV files. For that I created a ForEach activity which iterates through all the array input and inside it I have the copy activity.
	- 	Also I don't want the same data getting copied twice, hence I used a set variable for the custom python script to get unique values and then converted into array.
		- Custom Python Script:
```python
import requests
import json
# GitHub API URL for the contents of the Data folder in the repo

api_url = "https://api.github.com/repos/Kens3i/E-Com_Azure_Data_Eng_Proj/contents/Data"

# Send GET request to GitHub API
response = requests.get(api_url)

# Check for a successful response
if response.status_code == 200:
files = response.json()

# Filter and format only .csv files
csv_files = []

# Loop through each file in the folder
for  file  in files:
filename = file["name"]

# Only process files that end with .csv
if filename.endswith(".csv"):
csv_files.append({
"relative_csv_url": filename,
"sinkfilename": filename
})
 
# Print the result
print(json.dumps(csv_files, indent=4))
else:
print(f"Failed to fetch files: {response.status_code}")
```
Note: To get the output from Databrick to ADF I used the below code:
**dbutils.notebook.exit(json.dumps(csv_files, indent=4))**
	
- The output of the databricks notebook is a form of array, so the for each activity will one by one iterate all the .csv names and pass it into the copy activity to get the data.
    
-   **Loading MySQL DB Data:** I create an ADF dataset for the MySQL table(s) and a Copy activity to write them to Bronze as Parquet or CSV. Each raw file (e.g. `sales.csv`, `customers.json`, `reviews.json`) lands in the `bronze` container.

- **Pipeline Flow**:
![37.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/37.png?raw=true) 


This completes the Bronze layer: raw data is ingested and stored immutable for auditability. No cleaning or validation is done yet.

---


### Silver Layer – Data Cleaning, Transformation, and Enrichment

In Databricks, I load the raw Bronze data into Spark DataFrames for processing. My goals are to clean data quality issues and enrich by combining related datasets.

- **Creating an app registration** to link databricks with my ADLS Gen 2 account. -   Azure Databricks runs in its own VNet and does not automatically inherit Azure AD identities. To grant clusters secure, token‑based access to ADLS Gen2, I register an **Azure AD application** (service principal).
    
-   **Role Assignment:** I assign the service principal the **Storage Blob Data Contributor** role (and ACL) on the ADLS container. 

- **Secret Management:** I store the service principal’s client secret in a Databricks secret scope. At runtime, Databricks retrieves this secret and uses the client ID + secret to authenticate via OAuth.

- **Setting up Databricks**: Setup a cluster and installed required libraries for further processing in Databricks. For Example installed pymongo to connect Databricks with the mongodb database.

-   **Data Cleaning:** I remove duplicates, handle missing/null values, and correct data types. For example, to clean the sales data:
```python
import pyspark.sql.types as T
import pyspark.sql.functions as F

def  cleaning_duplicates_na(df, name):
print("Cleaning "  + name)
return df.dropDuplicates().na.drop('all')
```
- **Datatype Transformation**: Did few datatype transformations for example the dates were in string format and I transformed it into datetime format.

- **Adding few columns**: When doing the exploratory data analysis of the data I found that few columns can be added for few datasets. For Example: for order_delivery dataset I added a Time delay column using datediff function, which basically calculates the delay in delivery time.

- **Data Enrichment (Joins and Computations):** I join all the different tables into a single one to have all the necessary information. For example, I use the MongoDB connector pymongo, fetch data from the NOSQL Database and merge it into the main DataFrame:

```python
customers_df = spark.read.parquet("abfss://bronze@<storage_account>.dfs.core.windows.net/customers/")
enriched_df = clean_sales_df.join(customers_df, on="customer_id", how="left")
```

**Writing to Silver:** The cleaned/enriched DataFrame is then written back to ADLS in the **silver** container in Parquet format, often partitioning by date for efficiency. Example:
```python
enriched_df.write \
           .mode("overwrite") \
           .partitionBy("order_date") \
           .parquet("abfss://silver@<storage_account>.dfs.core.windows.net/enriched_sales/")
```

At this stage, data is now validated, joined, and ready for analytics.

---

### Gold Layer – Analytics-Ready Tables and External Views

In the Gold layer, I provide BI and analytics teams with easy-to-query tables derived from the Silver data. I do this in Azure Synapse Analytics using serverless SQL on-demand.

-   **OPENROWSET for Ad-hoc Queries:** I can run on-the-fly queries using `OPENROWSET` directly over the Parquet files in Silver. For example, to preview sales:
```sql
create  schema gold

create  view gold.final
AS
SELECT *
FROM  OPENROWSET(
BULK  'https://ecomdatastorageaccount.blob.core.windows.net/olistdata/Silver/',
FORMAT = 'PARQUET'
) AS res
```

**CETAS (Create External Table AS Select):** To materialize analytics tables, I use `CREATE EXTERNAL TABLE AS SELECT (CETAS)`. This exports query results back into ADLS Gold in parallel. For example, to aggregate monthly sales:
```sql
--Creating Password
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'abcd@@1234'

--Create a database scope
CREATE DATABASE SCOPED CREDENTIAL koustavadmin WITH IDENTITY = 'Managed Identity'

select * from sys.database_credentials

-- Specifying the file format
CREATE EXTERNAL FILE FORMAT extFileFormat WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);


-- Creating the location where the external table will be created
CREATE EXTERNAL DATA SOURCE goldLayer WITH (
    LOCATION = 'https://ecomdatastorageaccount.dfs.core.windows.net/olistdata/Gold',
    CREDENTIAL = koustavadmin
);


-- Creating the tablein adlsGen2 container, Location is basically folder name inside container
CREATE EXTERNAL TABLE gold.finalTable WITH (
        LOCATION = 'Serving',
        DATA_SOURCE = goldLayer,
        FILE_FORMAT = extFileFormat
) AS SELECT * FROM gold.final;
```

-   This creates Parquet files under the `gold/monthly_sales` folder. I repeat CETAS queries for other metrics (delivery stats, payment breakdown, etc.). Each CETAS run parallelizes the write to cloud storage for performance.
    
-   **External Tables (Views):** I created several important views/tables in Synapse to serve business analytics. Each is defined using SQL (serverless pool) on top of the Gold data. For example:

-   **MonthlySales:** Shows total sales by month.
```sql
CREATE  OR  ALTER  VIEW gold.sales_by_month_category_state
AS
SELECT
res.order_year_month AS order_year_month,
res.product_category_name AS product_category_name,
res.customer_state AS customer_state,
ROUND(SUM(res.payment_value), 2) AS total_sales,
COUNT(res.order_item_id) AS total_items
FROM
OPENROWSET(
BULK  'https://ecomdatastorageaccount.blob.core.windows.net/olistdata/Silver/',
FORMAT='PARQUET'
) AS res
BULK  'https://ecomdatastorageaccount.blob.core.windows.net/olistdata/Silver/',
FORMAT='PARQUET'
) AS res
GROUP  BY
res.order_year_month,
res.product_category_name,
res.customer_state;
GO
select * from gold.sales_by_month_category_state;
GROUP  BY
res.order_year_month,
res.product_category_name,
res.customer_state;
GO
select * from gold.sales_by_month_category_state;

CREATE  EXTERNAL  TABLE gold.sales_by_month_category_state_Table WITH (
LOCATION = 'Serving_sales_by_month_category_state',
DATA_SOURCE = goldLayer,
FILE_FORMAT = extFileFormat
) AS  SELECT * FROM gold.sales_by_month_category_state;
GO

SELECT * FROM gold.sales_by_month_category_state_Table;
```
(This view groups the enriched sales by year/month.)
    
-  **DeliveryPerformance:** Summarizes on-time delivery metrics.
```sql
CREATE  OR  ALTER  VIEW gold.delivery_performance_summary
AS
SELECT
res.customer_state AS customer_state,
res.product_category_name AS product_category_name,
ROUND(AVG(res.[Delay Time]), 2) AS avg_delivery_delay_days,
COUNT(res.order_item_id) AS total_delivered_items
FROM
OPENROWSET(
BULK  'https://ecomdatastorageaccount.blob.core.windows.net/olistdata/Silver/',
FORMAT='PARQUET'
) AS res
WHERE
res.order_status = 'delivered'
GROUP  BY
res.customer_state,
res.product_category_name;
GO

SELECT * from gold.delivery_performance_summary;

-- Creating the tablein adlsGen2 container, Location is basically folder name inside container
CREATE  EXTERNAL  TABLE gold.delivery_performance_summary_Table WITH (
LOCATION = 'Serving_delivery_performance_summary',
DATA_SOURCE = goldLayer,
FILE_FORMAT = extFileFormat
) AS  SELECT * FROM gold.delivery_performance_summary;
GO

SELECT * FROM gold.delivery_performance_summary_Table;
```
(This computes average delivery time and percentage delivered on time.)
    
-   **PaymentShare:** Shows how sales split by payment method.
```sql
CREATE  OR  ALTER  VIEW gold.payment_method_share
AS
SELECT
res.payment_type AS payment_type,
ROUND(SUM(res.payment_value), 2) AS total_revenue,
ROUND(
100.0 * SUM(res.payment_value)
/ SUM(SUM(res.payment_value)) OVER (), 2
) AS revenue_share_pct
FROM
OPENROWSET(
BULK  'https://ecomdatastorageaccount.blob.core.windows.net/olistdata/Silver/',
FORMAT='PARQUET'
) AS res
GROUP  BY
res.payment_type;
GO

SELECT * from gold.payment_method_share;

-- Creating the tablein adlsGen2 container, Location is basically folder name inside container
CREATE  EXTERNAL  TABLE gold.payment_method_share_Table WITH (
LOCATION = 'Serving_payment_method_share',
DATA_SOURCE = goldLayer,
FILE_FORMAT = extFileFormat
) AS  SELECT * FROM gold.payment_method_share;
GO

SELECT * FROM gold.payment_method_share_Table;
```
(This view calculates total and percentage of sales by payment type.)
    
-   **RFM_Summary:** Lists Recency-Frequency-Monetary values per customer.
```sql
CREATE  OR  ALTER  VIEW gold.customer_rfm_summary
AS
SELECT
res.customer_unique_id AS customer_unique_id,
MAX(res.order_purchase_timestamp) AS last_purchase_date,
COUNT(res.order_item_id) AS total_items,
ROUND(SUM(res.payment_value), 2) AS total_spent
FROM
OPENROWSET(
BULK  'https://ecomdatastorageaccount.blob.core.windows.net/olistdata/Silver/',
FORMAT='PARQUET'
) AS res
GROUP  BY
res.customer_unique_id;
GO

select * from gold.customer_rfm_summary;

-- Creating the tablein adlsGen2 container, Location is basically folder name inside container

CREATE  EXTERNAL  TABLE gold.customer_rfm_summary_Table WITH (
LOCATION = 'Serving_customer_rfm_summary',
DATA_SOURCE = goldLayer,
FILE_FORMAT = extFileFormat
) AS  SELECT * FROM gold.customer_rfm_summary;
GO


SELECT * FROM gold.customer_rfm_summary_Table;
```
(Recency can be derived as days since `LastPurchaseDate` in a reporting layer.)
    
Each of these views is based on the cleaned/enriched **External Tables** (denoted `_external`). At the end of the Gold layer, all key analytics tables (monthly sales, delivery performance, payment share, RFM, etc.) are available in Synapse for reporting.

---

# Execution-with-Screenshots
1. First we need to create a resource group, here I named it e-com-proj. ![1.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/1.png?raw=true)

2. Creating hosting DBs, to host out MySQL and MongoDB data. ![2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/2.png?raw=true)

3. Now I am gonna ingest olist order dataset into the mysqlsql db. For that creating a script and running it in Google collab. ![3.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/3.png?raw=true)

```python
#Code to connect MySQL DB to Google Collab Notebook:
!pip install mysql-connector-python
import mysql.connector
from mysql.connector import Error

hostname = "v56-a.h.filess.io"
database = "ecomDB_notebeltam"
port = "3307"
username = "ecomDB_notebeltam"
password = "45fb62fbf90397f512343f375a6aa97a6f09a2e2"

try:
    connection = mysql.connector.connect(host=hostname, database=database, user=username, password=password, port=port)
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")

#Reading the uploaded file in collab notebook:
import pandas as pd

order_payments = pd.read_csv("/content/olist_order_payments_dataset.csv")
order_payments.head()
```
 ![3.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/3.2.png?raw=true)

4. Now the code to upload data to mysql db.
```python
import mysql.connector
from mysql.connector import Error


# Connection details
hostname = "v56-a.h.filess.io"
database = "ecomDB_notebeltam"
port = "3307"
username = "ecomDB_notebeltam"
password = "45fb62fbf90397f512343f375a6aa97a6f09a2e2"


# CSV file path
csv_file_path = "/content/olist_order_payments_dataset.csv"


# Table name where the data will be uploaded
table_name = "olist_order_payments"


try:
    # Step 1: Establish a connection to MySQL server
    connection = mysql.connector.connect(
        host=hostname,
        database=database,
        user=username,
        password=password,
        port=port
    )
    if connection.is_connected():
        print("Connected to MySQL Server successfully!")


        # Step 2: Create a cursor to execute SQL queries
        cursor = connection.cursor()


        # Step 3: Drop table if it already exists (for clean insertion)
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        print(f"Table `{table_name}` dropped if it existed.")


        # Step 4: Create a table structure to match CSV file
        create_table_query = f"""
        CREATE TABLE {table_name} (
            order_id VARCHAR(50),
            payment_sequential INT,
            payment_type VARCHAR(20),
            payment_installments INT,
            payment_value FLOAT
        );
        """
        cursor.execute(create_table_query)
        print(f"Table `{table_name}` created successfully!")


        # Step 5: Load the CSV data into pandas DataFrame
        data = pd.read_csv(csv_file_path)
        print("CSV data loaded into pandas DataFrame.")


        # Step 6: Insert data in batches of 500 records
        batch_size = 500  # Define the batch size
        total_records = len(data)  # Get total records in the DataFrame


        print(f"Starting data insertion into `{table_name}` in batches of {batch_size} records.")
        for start in range(0, total_records, batch_size):
            end = start + batch_size
            batch = data.iloc[start:end]  # Get the current batch of records


            # Convert batch to list of tuples for MySQL insertion
            batch_records = [
                tuple(row) for row in batch.itertuples(index=False, name=None)
            ]

            # Prepare the INSERT query
            insert_query = f"""
            INSERT INTO {table_name}
            (order_id, payment_sequential, payment_type, payment_installments, payment_value)
            VALUES (%s, %s, %s, %s, %s);
            """


            # Execute the insertion query for the batch
            cursor.executemany(insert_query, batch_records)
            connection.commit()  # Commit after each batch
            print(f"Inserted records {start + 1} to {min(end, total_records)} successfully.")


        print(f"All {total_records} records inserted successfully into `{table_name}`.")


except Error as e:
    # Step 7: Handle any errors
    print("Error while connecting to MySQL or inserting data:", e)


finally:
    # Step 8: Close the cursor and connection
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed.")
```
Note: When uploading with batch size 1000 then as the db is hosted in free tire there are some space constraints hence uploading the data with batch size of 500, that means 500 rows will be inserted per execution.

5. Creating Azure Data Factory. ![5.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/5.png?raw=true)

6.  Creating a Copy data Pipeline and configuring the source (GitHub). ![6.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/6.png?raw=true)

7. As the files are in .csv format and then adding a new linked service. ![7.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/7.png?raw=true)

8. Pasting github data url in here. ![8.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/8.png?raw=true)
![8.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/8.2.png?raw=true)

9. The above process is only for 1 file, to import many files lets use base URL and relative url. ![9.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/9.png?raw=true)
![9.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/9.2.png?raw=true)

10.  Let's change the relative URL so all the data files can be fetched, for that making a parameter.
![10.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/10.png?raw=true)
![10.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/10.2.png?raw=true)
![10.3.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/10.3.png?raw=true)
Note: To make the project error proof I tried to first work on a single file, and copy it from source to sink, once that is done I use parametrization to copy all the files using relative and base URL.

11. Creating adlsgen2 Storage account for sink.
![11.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/11.png?raw=true)
Note: Enable hierarchical namespace on to enable adding directories inside container

12. Create a container inside the account.
![12.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/12.png?raw=true)

13. Adding directories.
![13.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/13.png?raw=true)

14. Configuring Sink, adls gen2 -> delimited:
![14.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/14.png?raw=true)

15. Making a new linked service for adls csv sink.
![15.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/15.png?raw=true)
![15.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/15.2.png?raw=true)

16. Opening the dataset, to add dynamic filepath.
![16.1.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/16.1.png?raw=true)
![16.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/16.2.png?raw=true)
![16.3.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/16.3.png?raw=true)

17. Now to get all the filenames from github we will make a python script.
```python
import requests
import json


# GitHub API URL for the contents of the Data folder in the repo
api_url = "https://api.github.com/repos/Kens3i/E-Com_Azure_Data_Eng_Proj/contents/Data"


# Send GET request to GitHub API
response = requests.get(api_url)


# Check for a successful response
if response.status_code == 200:
    files = response.json()


    # Filter and format only .csv files
    csv_files = []


    # Loop through each file in the folder
    for file in files:
        filename = file["name"]


        # Only process files that end with .csv
        if filename.endswith(".csv"):
            csv_files.append({
                "relative_csv_url": filename,
                "sinkfilename": filename
            })


    # Print the result
    print(json.dumps(csv_files, indent=4))
else:
    print(f"Failed to fetch files: {response.status_code}")


Output:
[
    {
        "relative_csv_url": "olist_customers_dataset.csv",
        "sinkfilename": "olist_customers_dataset.csv"
    },
    {
        "relative_csv_url": "olist_geolocation_dataset.csv",
        "sinkfilename": "olist_geolocation_dataset.csv"
    },
    {
        "relative_csv_url": "olist_order_items_dataset.csv",
        "sinkfilename": "olist_order_items_dataset.csv"
    },
    {
        "relative_csv_url": "olist_order_reviews_dataset.csv",
        "sinkfilename": "olist_order_reviews_dataset.csv"
    },
    {
        "relative_csv_url": "olist_orders_dataset.csv",
        "sinkfilename": "olist_orders_dataset.csv"
    },
    {
        "relative_csv_url": "olist_products_dataset.csv",
        "sinkfilename": "olist_products_dataset.csv"
    },
    {
        "relative_csv_url": "olist_sellers_dataset.csv",
        "sinkfilename": "olist_sellers_dataset.csv"
    }
]
```
18. Please note relative_csv_url and sinkfilename are actually the source and sink file parameters which we created.
![18.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/18.png?raw=true)
![18.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/18.2.png?raw=true)

19. Now it's time to run this python script in ADF. Create a databricks account -> go to databricks -. Create a notebook -> copy paste the below python.
```python
import requests
import json


# GitHub API URL for the contents of the Data folder in the repo
api_url = "https://api.github.com/repos/Kens3i/E-Com_Azure_Data_Eng_Proj/contents/Data"


# Send GET request to GitHub API
response = requests.get(api_url)


# Check for a successful response
if response.status_code == 200:
    files = response.json()


    # Filter and format only .csv files
    csv_files = []


    # Loop through each file in the folder
    for file in files:
        filename = file["name"]


        # Only process files that end with .csv
        if filename.endswith(".csv"):
            csv_files.append({
                "relative_csv_url": filename,
                "sinkfilename": filename
            })


    # Print the result
    print(json.dumps(csv_files, indent=4))
    dbutils.notebook.exit(json.dumps(csv_files, indent=4))
else:
    print(f"Failed to fetch files: {response.status_code}")
    dbutils.notebook.exit("Extraction failed")
```
Note: dbutils.notebook.exit(json.dumps(csv_files, indent=4)) -> Will give output in the pipeline

20. Now create a compute so that we can run databricks via adf pipeline.
![20.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/20.png?raw=true)

21. Now make a notebook activity in adf.
![21.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/21.png?raw=true)

22. URL can be found.
![22.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/22.png?raw=true)

23. Access Token can be found under databricks settings.
![23.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/23.png?raw=true)

24. Select existing interactive cluster.
![24.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/24.png?raw=true)

25. Give the notebook path.
![25.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/25.png?raw=true)

Output:
![25.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/25.2.png?raw=true)

26. Now Adding ForEach activity to get the output of the above notebook.
![26.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/26.png?raw=true)

27. The input for the activity will be output of “runOutput”.
![27.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/27.png?raw=true)

28. Inside for each there will be copy activity.
![28.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/28.png?raw=true)

29. The source will be the dataset which we first created. The sink will also be the dataset we had created earlier, sink bronze dataset.
![29.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/29.png?raw=true)

30. Now to populate the params.
![30.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/30.png?raw=true)

31. Source.
![31.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/31.png?raw=true)

32. Sink.
![32.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/32.png?raw=true)

33. Importing SQL data from mysql to ADF, making a linked service.
![33.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/33.png?raw=true)

34. Getting details from the host mysql db and copy pasting into the linked service.
![34.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/34.png?raw=true)

35. Making a copy source data link for importing data from MYSQL.
![35.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/35.png?raw=true)

36. Creating a sink for my sql copy activity.
![36.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/36.png?raw=true)

37. Debugging the pipeline.
![37.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/37.png?raw=true)
---
38. ONTO SILVER LAYER (all prev data + no sql data = enrichment process). Ingesting local data towards MongoDB.
```python
import pandas as pd
from pymongo import MongoClient

# Load the product_category CSV file into a pandas DataFrame
try:
  product_category_df = pd.read_csv("product_category_name_translation.csv")
except FileNotFoundError:
  print("Error: 'product_category_name_translation.csv' not found.")
  exit() # Exit the script if the file is not found


# MongoDB connection details (assuming these are already defined in your script)
hostname = "k3pcq.h.filess.io"
database = "ecomNoSQLDB_newbillbit"
port = "27018"
username = "ecomNoSQLDB_newbillbit"
password = "fdfe458a17d57fb8eeeafa728131973a77a6d45c"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database


try:
    # Establish a connection to MongoDB
    client = MongoClient(uri)
    db = client[database]

    # Select the collection (or create if it doesn't exist)
    collection = db["product_categories"]  # Choose a suitable name for your collection

    # Convert the DataFrame to a list of dictionaries for insertion into MongoDB
    data_to_insert = product_category_df.to_dict(orient="records")

    # Insert the data into the collection
    collection.insert_many(data_to_insert)

    print("Data uploaded to MongoDB successfully!")


except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the MongoDB connection
    if client:
        client.close()
```
39. While creating Databricks be very careful, don't select secure cluster Connectivity to "Yes". If you select "Yes" then you won't get the data.
![39.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/39.png?raw=true)

40. To connect databricks to adls gen 2.
```python
#Code to connect databricks to adls gen 2
service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", 
"https://login.microsoftonline.com/<directory-id>/oauth2/token")
```

41. To get details we need to first register an app.
![41.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/41.png?raw=true)

42. Let's add a secret.
![42.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/42.png?raw=true)
![42.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/42.2.png?raw=true)

43. Create variables outside, for the databricks code.
```python
# Variables for configuration
storage_account = "<storage-account>"
application_id = "<application-id>"
directory_id = "<directory-id>"
service_credential = "<service_credential>"

# Code to connect databricks to adls gen 2
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")
```
44. Now to fill each variables.
![44.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/44.png?raw=true)
![44.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/44.2.png?raw=true)

```python
# Variables for configuration
storage_account = "ecomdatastorageaccount"
application_id = "45c44a3d-747a-4d28-8bf9-a71324d89b68"
directory_id = "f8265e58-2ac1-4ddb-ac4a-7fa5a1215a64"
service_credential = ".Kp8Q~4HQReCI1Zm1uUrDNYVLqzjzLAkhAeL4ay9"


# Code to connect databricks to adls gen 2
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")
```
Note: This code sets the necessary configurations to connect Databricks to Azure Data Lake Storage Gen2 using OAuth.

45. Accessing the sink folder with databricks via role assignment of the storage account.
![45.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/45.png?raw=true)
![45.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/45.2.png?raw=true)

46. In the member add below (app registration name).
![46.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/46.png?raw=true)

Note: This permission will take 15-20 mins, it will allow databricks to connect with the sink folder.

47. When running code in databricks don’t select serverless, always select Compute.
![47.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/47.png?raw=true)
Note: The serverless will give error, as it does not have access to set spark configuration

48. Code to read adls gen 2 blob as a csv using spark dataframe.
```python
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("abfss://olistdata@ecomdatastorageaccount.dfs.core.windows.net/Bronze/olist_customers_dataset.csv")


display(df)

#OR

df = spark.read.\
    format("csv").\
        option("header", "true").\
            option("inferSchema","true").\ 
load("abfss://olistdata@ecomdatastorageaccount.dfs.core.windows.net/Bronze/olist_customers_dataset.csv")
```

49: Time to import mongo db data for data enrichment, we will enrich data with product categories dataset.
To install pymongo in the cluster:
![49.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/49.png?raw=true)
![49.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/49.2.png?raw=true)
![49.3.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/49.3.png?raw=true)

50: Connecting databricks with mongodb.
```python
# importing module
from pymongo import MongoClient

hostname = "k3pcq.h.filess.io"
database = "ecomNoSQLDB_newbillbit"
port = "27018"
username = "ecomNoSQLDB_newbillbit"
password = "fdfe458a17d57fb8eeeafa728131973a77a6d45c"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]

mydatabase
```
51:Checking data sent from mongo db.
```sql
import pandas as pd

collection = mydatabase["product_categories"]  # Choose a suitable name for your collection

mongo_data = pd.DataFrame(list(collection.find()))

#Converting Pandas to Sparkdf
#mongo_spark_df = spark.createDataFrame(mongo_data)
#but if we directly run above then error will be there as "_id" field is there which is not acceptable in spark df
#dropping _id field in pandas
mongo_data = mongo_data.drop("_id", axis=1)

#Converting Pandas to Sparkdf
mongo_spark_df = spark.createDataFrame(mongo_data)

display(mongo_spark_df)
```
52: Data Cleaning.
```python
#Remove duplicate rows from the DataFrame.
#.na.drop('all')->
'''
Removes rows where all columns are null (None/NaN).
drop('all') -> 'all' means the row will only be removed if every column is null.
If even one column has a value, the row will stay.
'''
import pyspark.sql.types as T
import pyspark.sql.functions as F

def cleaning_duplicates_na(df, name):
    print("Cleaning " + name)
    return df.dropDuplicates().na.drop('all')




#customer_df

#to show the number of rows and cols in customer_df
num_rows = customer_df.count()
num_cols = len(customer_df.columns)
display(f"Number of rows: {num_rows}, Number of columns: {num_cols}")

customer_df = cleaning_duplicates_na(customer_df,"customer dataframe")
customer_df.printSchema()
customer_df.show()

#to show the number of rows and cols in customer_df
num_rows = customer_df.count()
num_cols = len(customer_df.columns)
display(f"Number of rows: {num_rows}, Number of columns: {num_cols}")







#orders_df
#changing the data type of columns
#we won't be dropping duplicates here as orders can be repeated
orders_df.printSchema()

#to show the number of rows and cols in customer_df
num_rows = orders_df.count()
num_cols = len(orders_df.columns)
display(f"Number of rows: {num_rows}, Number of columns: {num_cols}")

#converting column datatypes
orders_df = orders_df.withColumn('order_purchase_timestamp', F.to_date('order_purchase_timestamp')).withColumn\
    ('order_delivered_carrier_date', F.to_date('order_delivered_carrier_date')).withColumn\
        ('order_approved_at',F.to_date('order_approved_at')).withColumn\
            ('order_delivered_customer_date',F.to_date('order_delivered_customer_date')).withColumn\
                ('order_estimated_delivery_date',F.to_date('order_estimated_delivery_date'))

orders_df.printSchema()

#adding a column which contains only the year and month
orders_df = orders_df.withColumn("order_year_month", F.date_format(F.col("order_purchase_timestamp"), format="y-M"))

#adding a column where delivery and time delays are calculated
orders_df = orders_df.withColumn("actual_delivery_time", F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
orders_df = orders_df.withColumn("estimated_delivery_time", F.datediff("order_estimated_delivery_date", "order_purchase_timestamp"))
orders_df =orders_df.withColumn("Delay Time", F.col("actual_delivery_time") - F.col("estimated_delivery_time"))

orders_df.printSchema()
orders_df.show()
display(orders_df)

#to show the number of rows and cols in customer_df
num_rows = orders_df.count()
num_cols = len(orders_df.columns)
display(f"Number of rows: {num_rows}, Number of columns: {num_cols}")







#order_payments_df

#to show the number of rows and cols in customer_df
num_rows = order_payments_df.count()
num_cols = len(order_payments_df.columns)
display(f"Number of rows: {num_rows}, Number of columns: {num_cols}")


order_payments_df = cleaning_duplicates_na(order_payments_df,"payments for orders")
order_payments_df.printSchema()
order_payments_df.show()


#to show the number of rows and cols in customer_df
num_rows = order_payments_df.count()
num_cols = len(order_payments_df.columns)
display(f"Number of rows: {num_rows}, Number of columns: {num_cols}")
```
53. Data Joining.
![53.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/53.png?raw=true)
```python
orders_cutomers_df = orders_df.join(customer_df, orders_df.customer_id == customer_df.customer_id,"left")
```

![53.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/53.2.png?raw=true)
```python
orders_cutomers_payments_df = orders_cutomers_df.join(order_payments_df, orders_cutomers_df.order_id == order_payments_df.order_id,"left")
```

![53.3.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/53.3.png?raw=true)
```python
orders_cutomers_payments_items_df = orders_cutomers_payments_df.join(order_items_df,"order_id","left")
```

![53.4.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/53.4.png?raw=true)
```python
orders_cutomers_payments_items_products_df = orders_cutomers_payments_items_df.join(products_df, orders_cutomers_payments_items_df.product_id == products_df.product_id,"left")
```

![53.5.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/53.5.png?raw=true)
```python
final_df = orders_cutomers_payments_items_products_df.join(sellers_df, orders_cutomers_payments_items_products_df.seller_id == sellers_df.seller_id,"left")
```

![53.6.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/53.6.png?raw=true)

54. Combining Combined df with the Mongo DB data.
```python
inal_df = final_df.join(spark_mongo_data, final_df.product_category_name == spark_mongo_data.product_category_name_english,"left")

#Dropping duplicate columns if there are any
def remove_duplicate_columns(df):
    columns=df.columns
    unique_cols = set()
    columns_to_drop = []

    for col in columns:
        if col in unique_cols:
            columns_to_drop.append(col)
        else:
            unique_cols.add(col)

    df_cleaned=df.drop(*columns_to_drop)

    return df_cleaned

final_cleaned_df = remove_duplicate_columns(final_df)
```

55. We can do Viz like this.
![55.1.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/55.1.png?raw=true)
![55.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/55.2.png?raw=true)

56. Storing Data in Silver container.
```python
final_cleaned_df.write.mode("overwrite").parquet("abfss://olistdata@ecomdatastorageaccount.dfs.core.windows.net/Silver/")
```
---
57. ONTO GOLD LAYER. Create Synapse acc.
![57.1.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/57.1.png?raw=true)
![57.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/57.2.png?raw=true)

58: Let's do the above so Synapse can access our ADLS gen 2.
![58.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/58.png?raw=true)
![58.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/58.2.png?raw=true)
![58.3.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/58.3.png?raw=true)
Also add this user.
![58.4.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/58.4.png?raw=true)
Note: Synapse can access its own datalake the above process is to access the data lake which we created which contains the Gold container

59. Creating a Serverless DB.
![59.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/59.png?raw=true)
![59.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/59.2.png?raw=true)

60. Create a TSQL script.
![61.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/61.2.png?raw=true)

61. Bulk will contain URL of silver folder.
![61.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/61.2.png?raw=true)
```sql
SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://ecomdatastorageaccount.blob.core.windows.net/olistdata/Silver/',
    FORMAT = 'PARQUET'
) AS result1
```
62. Create View.
```sql
create schema gold

create view gold.final
AS
SELECT  *
FROM OPENROWSET(
    BULK 'https://ecomdatastorageaccount.blob.core.windows.net/olistdata/Silver/',
    FORMAT = 'PARQUET'
) AS res
```
![62.1.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/62.1.png?raw=true)
![62.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/62.2.png?raw=true)

63. We will be using CETAS to transfer data over to the gold layer.
```sql
Code:
--Creating Password
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'abcd@@1234'


--Create a database scope
CREATE DATABASE SCOPED CREDENTIAL koustavadmin WITH IDENTITY = 'Managed Identity'


select * from sys.database_credentials

-- Specifying the file format
CREATE EXTERNAL FILE FORMAT extFileFormat WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);


-- Creating the location where the external table will be created
CREATE EXTERNAL DATA SOURCE goldLayer WITH (
    LOCATION = 'https://ecomdatastorageaccount.dfs.core.windows.net/olistdata/Gold',
    CREDENTIAL = koustavadmin
);


-- Creating the tablein adlsGen2 container, Location is basically folder name inside container
CREATE EXTERNAL TABLE gold.finalTable WITH (
        LOCATION = 'Serving',
        DATA_SOURCE = goldLayer,
        FILE_FORMAT = extFileFormat
) AS SELECT * FROM gold.final;


To drop db creds:
select * from sys.database_credentials
drop database scoped credential CREDNAME

--To drop Master Key:
Drop master key
```
Note: only after dropping credential we can drop the master key

64. After creating the external table we can access it via SQL.
![64.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/64.png?raw=true)
![64.2.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/64.2.png?raw=true)

65. To get the column name and datatypes from views/external table.
```sql
USE olist_serverless; -- Replace with your actual database name
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'final'; -- Replace with your actual table name
```
Note: Final is the view, we can also query finalTable, external table for the results

66. Creating View + External Table for:

Delivery Performance Summary

```sql
CREATE OR ALTER VIEW gold.delivery_performance_summary
AS
SELECT
    res.customer_state               AS customer_state,
    res.product_category_name        AS product_category_name,
    ROUND(AVG(res.[Delay Time]), 2)  AS avg_delivery_delay_days,
    COUNT(res.order_item_id)         AS total_delivered_items
FROM
    OPENROWSET(
      BULK 'https://ecomdatastorageaccount.blob.core.windows.net/olistdata/Silver/',
      FORMAT='PARQUET'
    ) AS res
WHERE
    res.order_status = 'delivered'
GROUP BY
    res.customer_state,
    res.product_category_name;
GO


SELECT * from gold.delivery_performance_summary;


-- --Creating Password
-- CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'abcd@@1234'


-- --Create a database scope
-- CREATE DATABASE SCOPED CREDENTIAL koustavadmin WITH IDENTITY = 'Managed Identity'


-- select * from sys.database_credentials


-- -- Specifying the file format
-- CREATE EXTERNAL FILE FORMAT extFileFormat WITH (
--     FORMAT_TYPE = PARQUET,
--     DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
-- );


-- -- Creating the location where the external table will be created
-- CREATE EXTERNAL DATA SOURCE goldLayer WITH (
--     LOCATION = 'https://ecomdatastorageaccount.dfs.core.windows.net/olistdata/Gold',
--     CREDENTIAL = koustavadmin
-- );


-- Creating the tablein adlsGen2 container, Location is basically folder name inside container
CREATE EXTERNAL TABLE gold.delivery_performance_summary_Table WITH (
        LOCATION = 'Serving_delivery_performance_summary',
        DATA_SOURCE = goldLayer,
        FILE_FORMAT = extFileFormat
) AS SELECT * FROM gold.delivery_performance_summary;
GO


SELECT * FROM gold.delivery_performance_summary_Table;
```

Monthly Sales by Category and State
```sql
CREATE OR ALTER VIEW gold.sales_by_month_category_state
AS
SELECT
    res.order_year_month              AS order_year_month,
    res.product_category_name         AS product_category_name,
    res.customer_state                AS customer_state,
    ROUND(SUM(res.payment_value), 2)  AS total_sales,
    COUNT(res.order_item_id)          AS total_items
FROM
    OPENROWSET(
      BULK 'https://ecomdatastorageaccount.blob.core.windows.net/olistdata/Silver/',
      FORMAT='PARQUET'
    ) AS res
      BULK 'https://ecomdatastorageaccount.blob.core.windows.net/olistdata/Silver/',
      FORMAT='PARQUET'
    ) AS res
GROUP BY
    res.order_year_month,
    res.product_category_name,
    res.customer_state;
GO


select * from gold.sales_by_month_category_state;


GROUP BY
    res.order_year_month,
    res.product_category_name,
    res.customer_state;
GO


select * from gold.sales_by_month_category_state;

-- Creating the tablein adlsGen2 container, Location is basically folder name inside container
CREATE EXTERNAL TABLE gold.sales_by_month_category_state_Table WITH (
        LOCATION = 'Serving_sales_by_month_category_state',
        DATA_SOURCE = goldLayer,
        FILE_FORMAT = extFileFormat
) AS SELECT * FROM gold.sales_by_month_category_state;
GO


SELECT * FROM gold.sales_by_month_category_state_Table;
```

Payment Method Revenue Share

```sql
CREATE OR ALTER VIEW gold.payment_method_share
AS
SELECT
    res.payment_type                 AS payment_type,
    ROUND(SUM(res.payment_value), 2) AS total_revenue,
    ROUND(
      100.0 * SUM(res.payment_value)
            / SUM(SUM(res.payment_value)) OVER (),
      2
    )                                                  AS revenue_share_pct
FROM
    OPENROWSET(
      BULK 'https://ecomdatastorageaccount.blob.core.windows.net/olistdata/Silver/',
      FORMAT='PARQUET'
    ) AS res
GROUP BY
    res.payment_type;
GO


SELECT * from gold.payment_method_share;


-- Creating the tablein adlsGen2 container, Location is basically folder name inside container
CREATE EXTERNAL TABLE gold.payment_method_share_Table WITH (
        LOCATION = 'Serving_payment_method_share',
        DATA_SOURCE = goldLayer,
        FILE_FORMAT = extFileFormat
) AS SELECT * FROM gold.payment_method_share;
GO


SELECT * FROM gold.payment_method_share_Table;
```

Customer RFM style Summary
A customer "RFM-style" summary means presenting a customer's buying behavior through an RFM analysis, which stands for Recency, Frequency, and Monetary value. It's a method of customer segmentation that categorizes customers based on how recently they purchased, how often they purchase, and the total amount they spend. 
Here's a breakdown of each component: 
Recency: How recently a customer made their last purchase.
Frequency: How often a customer makes purchases.
Monetary Value: The total amount of money a customer spends on purchases.

```sql
CREATE OR ALTER VIEW gold.customer_rfm_summary
AS
SELECT
    res.customer_unique_id            AS customer_unique_id,
    MAX(res.order_purchase_timestamp) AS last_purchase_date,
    COUNT(res.order_item_id)          AS total_items,
    ROUND(SUM(res.payment_value), 2)  AS total_spent
FROM
    OPENROWSET(
      BULK 'https://ecomdatastorageaccount.blob.core.windows.net/olistdata/Silver/',
      FORMAT='PARQUET'
    ) AS res
GROUP BY
    res.customer_unique_id;
GO


select * from gold.customer_rfm_summary;

-- Creating the tablein adlsGen2 container, Location is basically folder name inside container
CREATE EXTERNAL TABLE gold.customer_rfm_summary_Table WITH (
        LOCATION = 'Serving_customer_rfm_summary',
        DATA_SOURCE = goldLayer,
        FILE_FORMAT = extFileFormat
) AS SELECT * FROM gold.customer_rfm_summary;
GO


SELECT * FROM gold.customer_rfm_summary_Table;
```
67: Here is the Final Output:
![67.png](https://github.com/Kens3i/E-Com_Azure_Data_Eng_Proj/blob/main/Images/67.png?raw=true)

---

## FAQ

###  Challenges  
- **Batch Size Limits in MySQL Free Tier**  
  - **Issue:** Inserting 1,000 rows at once caused storage errors on the free‑tier MySQL instance.  
  - **Solution:** Reduced the batch size to 500 records per commit to avoid hitting storage quotas.


###  Design Decisions  
- **Why Medallion Architecture (Bronze/Silver/Gold)?**  
  - Traceability: Raw data stays untouched in Bronze for audit and reprocessing.  
  - Incremental Quality: Silver refines and enriches; Gold delivers business‑ready datasets.  
  - Separation of Concerns: Clear boundaries for ingestion, cleaning, and serving layers.

- **Why Parquet instead of Delta?**  
  - Compatibility: Parquet works out‑of‑the‑box with Synapse serverless SQL (`OPENROWSET`).  
  - Simplicity: No transaction log overhead; ideal for static/batch workloads.  
  - Delta Use Case: Opt for Delta when you need ACID transactions, schema enforcement, or time travel.


###  Best Practices  
- Parameterize ADF pipelines using base URL + relative path and ForEach loops.  
- Partition Silver data by date or key to improve Spark and Synapse performance.  
- Cache intermediate DataFrames in Databricks (`.cache()`) to avoid recomputation.  
- Store secrets (service principal credentials, tokens) securely in Azure Key Vault or Databricks secret scopes.  
- Implement CI/CD for notebooks and pipeline definitions via GitHub + Azure DevOps.


### Spark Concepts  
- **Transformations (Lazy):** Define a new RDD/DataFrame without execution (e.g. `filter()`, `select()`, `groupBy()`, `dropDuplicates()`).  
- **Actions (Eager):** Trigger execution and return results (e.g. `show()`, `count()`, `collect()`, `write()`).  
- **Caching / Persisting:** Store intermediate results in memory or disk to speed up repeated access (`df.cache()`, `df.persist()`). Use after heavy transformations on large DataFrames.


### 🔑 Authentication & Access  
| Service              | Auth Method                     | Needs App Registration? |
|----------------------|---------------------------------|-------------------------|
| **Azure Synapse SQL**| Managed Identity (built‑in MI)  | No                      |
| **Databricks**       | Azure AD Service Principal (SP) | Yes                     |

- **Role Assignment:** Grant the service principal the **Storage Blob Data Contributor** role (and ACL) on the ADLS container.  
- **Secret Management:** Store the service principal’s client secret in a Databricks secret scope. Databricks retrieves this secret at runtime to authenticate via OAuth.

**Alternatives to Service Principal:**  
1. **Storage Account Key** – Simple but broad permissions and key‐rotation risk.  
2. **Shared Access Signature (SAS) Token** – Granular, time‐limited access, but token management overhead.  
3. **Azure AD Passthrough** – Pass user AAD tokens directly to ADLS (requires premium features).  
4. **Managed Identity for Databricks (Preview)** – Assign an MI directly to the workspace/cluster (availability varies).


### OPENROWSET vs CETAS vs External Tables  
- **OPENROWSET:** Ad‑hoc, read‑only queries against files in ADLS.  
- **CREATE EXTERNAL TABLE:** Defines a permanent schema over file sets for repeated queries.  
- **CETAS (Create External Table AS Select):** Runs a query and writes the result back to ADLS as Parquet files in parallel—ideal for materializing Gold‐layer aggregates.


### Why Enrich with MongoDB Data?  
- Adds translated (English) product category names for readability.  
- Improves BI dashboard usability with human‑friendly labels.  
- Demonstrates combining NoSQL dimension data with relational/flat files.

### Why Use Synapse Serverless SQL Pool?  
- **Cost‑Effective, On‑Demand:** Pay only for data scanned; no compute to provision or idle.  
- **Direct Lake Queries:** Query Parquet/CSV in ADLS Gen2 via `OPENROWSET` or external tables with zero data movement.  
- **Instant Start:** Queries run immediately, with no cluster startup delay.  
- **Auto-Scaling:** Handles ad‑hoc and exploratory workloads seamlessly.  
- **Ideal for Prototyping:** Quickly surface insights before committing to dedicated resources.
---


### Thankyou For Spending Your Precious Time Going Through This Project!
### If You Find Any Value In This Project Or Gained Something New Please Do Give A ⭐.
