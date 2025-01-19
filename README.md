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
> 💡 The data in this project is not real. It is created on random using [Mockaroo Random Data Generator](https://www.mockaroo.com/).  
> 💡 The process can be automated to run once every day which will guarantee that everyone will have accurate and up to date data.

<br>

### Problem Solving Process

### 1️⃣  
**Create initial resources and linked services in the Azure environment**  
- Create resource group with all the needed tools - Data Factory, Databricks, Synapse Analytics, Key Vaults
- Link the azure services to the on-premise server (Used Integration Runtime service to connect the local server with the cloud)
- Create storage account and configure 3 layer containers - bronze (raw), silver (transformed), gold (structured/ready for analysis)

<br>

### 2️⃣  
**Create a pipeline in Azure Data Factory**
- Get all on-premises tables info using Lookup block
- Copy the data into the bronze layer container using the For Each block  

<br>

### 3️⃣  
**Create Azure Databricks cluster, workspace and two notebooks**
- **[bronze_to_silver notebook](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/databricks/bronze-to-silver.ipynb)**
  - #### Firstly, each table column names are converted from Camel case to Snake case

  - #### Reservations table
    
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

  - #### Flights table

  | FlightId | Flight | FlightDeparture | Airport |
  | ------- | -------- | ------- | ------- |
  | 1926 | CD456-Ozu(Airbus A320) | 1718728920000 | CPR&#124;Casper-Natrona County International Airport |
  | 8436 | KL333-Jabbertype(Boeing 737) | 1723819247000 | KSU&#124;Kristiansund Airport (Kvernberget) |
  | 5542 | EF789-Jayo(Airbus A380) | 1716310072000 | PBE&#124;Puerto Berrio Airport |
  | 1196 | IJ222-Feedbug(Airbus A320) | 1730629337000 | VDI&#124;Vidalia Regional Airport |
  | 9672 | CD456-Ozu(Boeing 787) | 1709645519000 | YQC&#124;Quaqtaq Airport |

  👇

  | flight_id | flight | flight_number | flight_airline_name | flight_plane_model | flight_departure | flight_departure_date | flight_departure_time | airport | airport_code | airport_name |
  | ------- | -------- | ------- | ------- | ------- | -------- | ------- | ------- | -------- | ------- | ------- |
  | 1926 | CD456-Ozu(Airbus A320) | CD456 | Ozu | Airbus A320 | 1718728920000 | 2024-06-18 | 16:42 | CPR&#124;Casper-Natrona County International Airport | CPR | Casper-Natrona County International Airport |
  | 8436 | KL333-Jabbertype(Boeing 737) | KL333 | Jabbertype | Boeing 737 | 1723819247000 | 2024-08-16 | 14:40 | KSU&#124;Kristiansund Airport (Kvernberget) | KSU | Kristiansund Airport (Kvernberget) |
  | 5542 | EF789-Jayo(Airbus A380) | EF789 | Jayo | Airbus A380 | 1716310072000 | 2024-05-21 | 16:47 | PBE&#124;Puerto Berrio Airport | PBE | Puerto Berrio Airport |
  | 1196 | IJ222-Feedbug(Airbus A320) | IJ222 | Feedbug | Airbus A320 | 1730629337000 | 2024-11-03 | 10:22 | VDI&#124;Vidalia Regional Airport | VDI | Vidalia Regional Airport |
  | 9672 | CD456-Ozu(Boeing 787) | CD456 | Ozu | Boeing 787 | 1709645519000 | 2024-03-05 | 13:31 | YQC&#124;Quaqtaq Airport | YQC | Quaqtaq Airport |
  
  - #### Hotels table

  | HotelId | Hotel |
  | -------- | -------- |
  | 636 | Oloo*5 |
  | 240 | Kaymbo*3 |
  | 681 | Vitz*5 |
  | 267 | Omba*3 |
  | 855 | Kanoodle*5 |

  👇

  | hotel_id | hotel | hotel_name | hotel_stars |
  | -------- | -------- | -------- | -------- |
  | 636 | Oloo*5 | Oloo | 5 |
  | 240 | Kaymbo*3 | Kaymbo | 3 |
  | 681 | Vitz*5 | Vitz | 5 |
  | 267 | Omba*3 | Omba | 3 |
  | 855 | Kanoodle*5 | Kanoodle | 5 |

  - #### Customers table

  | CustomerId | CustomerName | CustomerGender | CustomerEmail | CustomerDateOfBirth |
  | -------- | -------- | -------- | -------- | -------- |
  | 7505 | Boone Aylmore | F | baylmore0@nationalgeographic.com | 3/31/1970 |
  | 3254 | Karlis Blasio | F | kblasio1@cbsnews.com | 4/12/1995 |
  | 3545 | Christiano Mantram | M | cmantram2@wordpress.com | 12/24/1991 |
  | 9260 | Arlen Dicey | M | adicey3@dell.com | 10/23/1993 |
  | 7005 | Papagena Sproson | F | psproson4@salon.com | 6/9/1976 |

  👇

  | customer_id | customer_name | customer_first_name | customer_last_name | customer_gender | customer_email | customer_date_of_birth | customer_age |
  | -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- |
  | 7505 | Boone Aylmore | Boone | Aylmore | Female | baylmore0@nationalgeographic.com | 1970-03-31 | 54 |
  | 3254 | Karlis Blasio | Karlis | Blasio | Female | kblasio1@cbsnews.com | 1995-12-04 | 29 |
  | 3545 | Christiano Mantram | Christiano | Mantram | Male | cmantram2@wordpress.com | 1991-12-24 | 33 |
  | 9260 | Arlen Dicey | Arlen | Dicey | Male | adicey3@dell.com | 1993-10-23 | 31 |
  | 7005 | Papagena Sproson | Papagena | Sproson | Female | psproson4@salon.com | 1976-06-09 | 48 |


- **[silver_to_gold notebook](https://github.com/BogdanTopalov/Travel-Agency-Azure-Pipeline/blob/main/databricks/silver_to_gold.ipynb)**
  - #### Join all tables by their ids and leave only analysis relevant columns in the final table

  👇
  
  | reservation_id | reservation_date | customer_gender | customer_age | flight_departure_date | flight_airline_name | flight_plane_model | hotel_stars | room_type | payment_method | total_price_amount | total_price_currency | total_price_in_eur |
  | -------------- | ---------------- | --------------- | ------------ | --------------------- | ------------------- | ------------------ | ----------- | --------- | -------------- | ------------------ | ------------------ | ------------------ |
  | 1446 | 2024-03-11 | Female | 54 | 2024-06-18 | Ozu | Airbus A320 | 5 | Single | Credit Card | 9460.83 | BGN | 4825.02 |
  | 1326 | 2024-05-09 | Female | 29 | 2024-08-16 | Jabbertype | Boeing 737 | 3 | Double | PayPal | 7687.94 | BGN | 3920.85 |
  | 1315 | 2024-02-12 | Male | 33 | 2024-05-21 | Jayo | Airbus A380 | 5 | Suite | Credit Card | 9870.13 | USD | 9376.62 |
  | 2980 | 2024-09-21 | Male | 22 | 2024-12-29 | Cogidoo | Airbus A380 | 4 | Double | Credit Card | 1217.77 | EUR | 1217.77 |
  | 2936 | 2023-11-27 | Female | 48 | 2024-03-05 | Ozu | Boeing 787 | 5 | Double | Credit Card | 3302.29 | GBP | 3962.75 |
 

- **Add notbook block for each databricks one and triger the Data Factory pipeline**
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
