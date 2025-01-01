# Travel Agency Azure Pipeline


### This is a simple Azure End-to-End project, that solves a simple business problem and gives real analysis with conclusions.

<br>

# From this 👇

![alt text](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/images/reservations_data_preview.png "reservations data")


# To this 👇

![alt text](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/gifs/Travel%20Agency%20General%20Dashboard.gif "dashboard")

<br>


🧩 **The problem:**  

The travel agency have all kind of booking information across different database tables.  
The data is located on on-premise server and it is unstructured and hard to analyze.   
The agency needs structured data to create reports and track its KPIs.  

<br>

🧠 **The solution:**  

1️⃣ Connect the on-premise database (PostgreSQL) to the cloud  
2️⃣ Copy the raw data into Azure Data Lake Storage Gen 2 using Azure Data Factory  
3️⃣ Transform the data using Azure Databricks  
4️⃣ Optimize queries in Azure Synapse Analytics  
5️⃣ Use the processed/structured data to create personalized report in Power BI  

> [!NOTE]
> 💡 The process can be automated to run once every day which will guarantee that everyone will have accurate and up to date data.

<br>

### Problem Solving Process

<br>

### 1️⃣  
**Create initial resources and linked services in the Azure environment**  
- Create resource group with all the needed tools - Data Factory, Databricks, Synapse Analytics, Key Vaults
- Link the azure services to the on-premise server
- Create storage account and configure 3 layer containers - bronze (raw), silver (transformed), gold (structured/ready for analysis)

<br>

### 2️⃣  
**Create a pipeline in Azure Data Factory**
- Get all on-premises tables info using Lookup block
- Copy the data into the bronze layer container using For Each block 

<br>

### 3️⃣  
**Create Azure Databricks cluster, workspace and two notebooks**
- [bronze_to_silver notebook](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/databricks/bronze-to-silver.ipynb)
- [silver_to_gold notebook](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/databricks/silver_to_gold.ipynb)
- Add notbook block for each databricks one and triger the Data Factory pipeline
![alt text](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/images/data_factory_pipeline.png "data factory pipeline")

<br>

### 4️⃣  
**Create Azure Synapse Analytics pipeline**
- Write SQL procedure query to create a view for each gold layer table
```
  USE travel_agency_sql_database
GO

CREATE OR ALTER PROCEDURE create_serverless_view_of_gold_table @view_name NVARCHAR(100)
AS
BEGIN
    DECLARE @statement NVARCHAR(MAX);

    -- Build the dynamic SQL query
    SET @statement = N'CREATE OR ALTER VIEW ' + QUOTENAME(@view_name) + N' AS
    SELECT 
    * 
    FROM 
        OPENROWSET(
            BULK ''https://storageaacountname.dfs.core.windows.net/gold/data_tables/' + @view_name + '/'',
            FORMAT = ''DELTA''
        ) AS [result];';

    -- Execute the dynamic SQL query
    EXEC sp_executesql @statement;
END;
GO
```
- Get gold layer metadata and create a storage procedure for each table
![alt text](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/images/synapse_analytics_pipeline.png "synapse pipeline")

<br>

### 5️⃣  
**Create new dashboard in Power BI using the data from Azure Synapse SQL Database**
- General metrics example
![](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/gifs/Travel%20Agency%20General%20Dashboard.gif)
- Airline metrics example
![](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/gifs/Travel%20Agency%20Airline%20Dashboard.gif)

📓 [Travel Agency Dashboard PDF](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/dashboard/travel_agency_dashboard.pdf)
