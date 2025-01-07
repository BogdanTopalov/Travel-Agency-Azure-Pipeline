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
  - **Firstly, each table column names are converted from Camel case to Snake case**

  - **Reservations table**  
    
  | ReservationCreatedAt | ReservationId | CustomerId | FlightId | HotelId | RoomType | TotalPrice | PaymentMethod |
  |:-----------------------|----------------:|-------------:|-----------:|----------:|:-----------|:-------------|:----------------|
  | 2024-03-11 16:42:00 | 1446 | 7505 | 1926 | 636 | Single | 9460.83BGN | Credit Card |
  | 2024-05-09 14:40:47 | 1326 | 3254 | 8436 | 240 | Double | 7687.94BGN | PayPal |
  | 2024-02-12 16:47:52 | 1315 | 3545 | 5542 | 681 | Suite | 9870.13USD | Credit Card |
  | 2023-11-27 13:31:59 | 2936 | 7005 | 9672 | 855 | Double | 3302.29GBP | Credit Card |
  | 2024-09-21 08:05:44 | 2980 | 5857 | 6894 | 447 | Double | 1217.77EUR | Credit Card |
  
  👇

  | reservation_created_at | reservation_date | reservation_time | reservation_id | customer_id | flight_id | hotel_id | room_type | payment_method | total_price | total_price_amount | total_price_currency | total_price_in_eur |
  | -------- | ------- | -------- | ------- | -------- | ------- | -------- | ------- | -------- | ------- | -------- | ------- | ------- |
  | 2024-03-11 16:42:00 |	2024-03-11 | 16:42 | 1446 | 7505 | 1926 | 636 | Single | Credit Card  | 9460.83BGN | 9460.83 | BGN | 4825.02 |
  | 2024-05-09 14:40:47 | 2024-05-09 | 14:40 | 1326 | 3254 | 8436 | 240 | Double | PayPal | 7687.94BGN | 7687.94 | BGN | 3920.85 |
  | 2024-02-12 16:47:52	| 2024-02-12 | 16:47 | 1315 | 3545 | 5542 | 681 | Suite | Credit Card | 9870.13USD | 9870.13 | USD | 9376.62 |
  | 2023-11-27 13:31:59 | 2023-11-27 | 13:31 | 2936 | 7005 | 9672 | 855 | Double | Credit Card | 3302.29GBP | 3302.29 | GBP | 3962.75 |
  | 2024-09-21 08:05:44 | 2024-09-21 | 08:05 | 2980 | 5857 | 6894 | 447 | Double | Credit Card | 1217.77EUR | 1217.77 | EUR | 1217.77 |

<br>

  - **Flights table**  

  | flight_id | flight | flight_departure | airport |
  | ------- | -------- | ------- | ------- |
  | 1926 | CD456-Ozu(Airbus A320) | 1718728920000 | CPR&#124;Casper-Natrona County International Airport |
  | 8436 | KL333-Jabbertype(Boeing 737) | 1723819247000 | KSU&#124;Kristiansund Airport (Kvernberget) |
  | 5542 | EF789-Jayo(Airbus A380) | 1716310072000 | PBE&#124;Puerto Berrio Airport |
  | 1196 | IJ222-Feedbug(Airbus A320) | 1730629337000 | VDI&#124;Vidalia Regional Airport |
  | 9672 | CD456-Ozu(Boeing 787) | 1709645519000 | YQC&#124;Quaqtaq Airport |

  👇

  ...

<br>

  - **Hotels table**

<br>

  - **Customers table**

<br>

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

📓 [Travel Agency Report PDF](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/dashboard/travel_agency_dashboard.pdf)
