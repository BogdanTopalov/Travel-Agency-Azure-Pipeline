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
Create initial resources and linked services in the Azure environment  
- Create resource group with all the needed tools - Data Factory, Databricks, Synapse Analytics, Key Vaults
- Link the azure services to the on-premise server
- Create storage account and configure 3 layer containers - bronze (raw), silver (transformed), gold (structured/ready for analysis)

### 2️⃣  
Create a pipeline in Azure Data Factory  
- Get all on-premises tables info using Lookup block
- Copy the data into the bronze layer container using For Each block 

### 3️⃣  
Create Azure Databricks cluster, workspace and two notebooks
- [bronze_to_silver notebook](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/databricks/bronze-to-silver.ipynb)
- [silver_to_gold notebook](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/databricks/silver_to_gold.ipynb)

### 4️⃣  
Create Azure Synapse Analytics pipeline
- Write SQL procedure query to create a view for each gold layer table
  ![alt text](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/gifs/Travel%20Agency%20General%20Dashboard.gif "dashboard")
- Get gold layer metadata and create a storage procedure for each table

### 5️⃣  


