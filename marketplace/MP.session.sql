SELECT * FROM stage_clients
select * from hub_clients order by client_id
select * from sat_clients order by client_id
select count (*) from stage_clients 
select count (*) from hub_clients 
TRUNCATE table sat_clients
drop table hub_clients, sat_clients


SELECT client_id, COUNT(*)
FROM stage_clients
GROUP BY client_id
HAVING COUNT(*) > 1;

SELECT *
FROM stage_clients
WHERE LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) != 11
   OR phone NOT LIKE '8%'
   OR email NOT ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
   OR client_id IS NULL;



drop table temp_stage_clients

CREATE temp TABLE temp_stage_clients AS
SELECT
    client_id,
    INITCAP(name) AS name,
    LOWER(email) AS email,
    CASE 
        WHEN REGEXP_REPLACE(phone, '[^0-9]', '', 'g') LIKE '7%' THEN 
            '8' || SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g') FROM 2) -- Replace 7 with 8
        ELSE 
            REGEXP_REPLACE(phone, '[^0-9]', '', 'g') -- Keep as is if already starts with 8
    END AS phone,
    COALESCE(region, 'Unknown') AS region,
    CURRENT_TIMESTAMP AS start_date,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS end_date,
    CURRENT_TIMESTAMP AS load_date,
    'stage' AS record_source
FROM stage_clients;
-- WHERE LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) = 11
--   AND REGEXP_REPLACE(phone, '[^0-9]', '', 'g') LIKE '8%'
--   AND email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$';

-- SELECT * from temp_stage_clients
-- SELECT * FROM temp_stage_clients WHERE client_id IS NULL;
-- SELECT * FROM temp_stage_clients WHERE LENGTH(phone) != 11;

INSERT INTO hub_clients (client_id, load_date, record_source)
SELECT DISTINCT client_id, CURRENT_TIMESTAMP, 'stage'
FROM temp_stage_clients;

INSERT INTO sat_clients (client_id, name, email, phone, region, start_date, end_date, load_date, record_source)
SELECT
    client_id,
    name,
    email,
    phone,
    region,
    start_date,
    end_date,
    load_date,
    record_source
FROM temp_stage_clients;


CREATE TABLE hub_clients (
    client_id BIGINT PRIMARY KEY,  -- Unique identifier for each client
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Date when the record is loaded
    record_source VARCHAR(50) NOT NULL  -- Source of the record (e.g., 'stage')
);

CREATE TABLE sat_clients (
    client_id BIGINT NOT NULL,             -- Foreign key referencing hub_clients
    name VARCHAR(100),                     -- Client's name (max length adjusted)
    email VARCHAR(100),                    -- Client's email (reasonable size)
    phone VARCHAR(15),                     -- Client's phone number (digits only)
    region VARCHAR(50),                    -- Client's region
    start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When the record becomes valid
    end_date TIMESTAMP DEFAULT '9999-12-31',         -- When the record is no longer valid
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,   -- Load date
    record_source VARCHAR(50) NOT NULL,             -- Source of the record
    PRIMARY KEY (client_id, start_date),
    FOREIGN KEY (client_id) REFERENCES hub_clients (client_id)
);

-- drop table sat_clients

CREATE TABLE hub_products (
    product_id BIGINT PRIMARY KEY, -- Unique identifier for the product
    load_date TIMESTAMP NOT NULL, -- Date the record was loaded
    record_source VARCHAR(50) NOT NULL -- Source of the data
);

CREATE TABLE sat_products (
    product_id BIGINT NOT NULL REFERENCES hub_products(product_id), -- FK to hub_products
    name VARCHAR(100) NOT NULL, -- Name of the product
    category VARCHAR(50) NOT NULL, -- Product category
    price NUMERIC(10, 2) NOT NULL, -- Product price
    start_date TIMESTAMP NOT NULL, -- Start date for the record's validity
    end_date TIMESTAMP NOT NULL, -- End date for the record's validity
    load_date TIMESTAMP NOT NULL, -- Date the record was loaded
    record_source VARCHAR(50) NOT NULL, -- Source of the data
    PRIMARY KEY (product_id, start_date) -- Composite key for SCD2 implementation
);

-- Create temp table with cleaned data
CREATE TEMP TABLE temp_stage_products AS
SELECT
    product_id,
    INITCAP(name) AS name, -- Format product names
    CASE 
        WHEN category IS NULL OR TRIM(category) = '' THEN 'Unknown' -- Default for null or empty categories
        ELSE LOWER(TRIM(category)) -- Ensure categories are lowercase and trimmed
    END AS category,
    CASE 
        WHEN price < 0 THEN NULL -- Invalid prices
        ELSE price -- Keep valid prices
    END AS price,
    CURRENT_TIMESTAMP AS start_date,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS end_date,
    CURRENT_TIMESTAMP AS load_date,
    'stage' AS record_source
FROM stage_products
WHERE price >= 0 -- Filter out products with negative prices
  AND name IS NOT NULL -- Ensure product name is not null
  AND category IS NOT NULL; -- Ensure category is not null

-- Validate data
SELECT * FROM temp_stage_products WHERE price IS NULL; -- Check for invalid prices
SELECT * FROM temp_stage_products WHERE name IS NULL; -- Check for missing names
SELECT * FROM temp_stage_products WHERE category IS NULL; -- Check for missing categories

-- Insert into Hub table
INSERT INTO hub_products (product_id, load_date, record_source)
SELECT DISTINCT product_id, CURRENT_TIMESTAMP, 'stage'
FROM temp_stage_products;

-- Insert into Satellite table
INSERT INTO sat_products (product_id, name, category, price, start_date, end_date, load_date, record_source)
SELECT
    product_id,
    name,
    category,
    price,
    start_date,
    end_date,
    load_date,
    record_source
FROM temp_stage_products;

select count(*) from hub_products
select count(*) from stage_products
select * from sat_products
select * from hub_products order by product_id

drop table sat_warehouses, hub_warehouses

CREATE TABLE hub_warehouses (
    warehouse_id BIGINT PRIMARY KEY, -- Unique identifier for the warehouse
    load_date TIMESTAMP NOT NULL, -- Date the record was loaded
    record_source VARCHAR(50) NOT NULL -- Source of the data
);

CREATE TABLE sat_warehouses (
    warehouse_id BIGINT NOT NULL REFERENCES hub_warehouses(warehouse_id), -- FK to hub_warehouses
    region VARCHAR(50) NOT NULL, -- Warehouse region
    capacity NUMERIC(10, 2) NOT NULL, -- Capacity of the warehouse
    start_date TIMESTAMP NOT NULL, -- Start date for the record's validity
    end_date TIMESTAMP NOT NULL, -- End date for the record's validity
    load_date TIMESTAMP NOT NULL, -- Date the record was loaded
    record_source VARCHAR(50) NOT NULL, -- Source of the data
    PRIMARY KEY (warehouse_id, start_date) -- Composite key for SCD2 implementation
);

CREATE TEMP TABLE temp_stage_warehouses AS
SELECT
    warehouse_id,
    INITCAP(region) AS region, -- Normalize region names
    capacity::NUMERIC(10, 2) AS capacity, -- Ensure capacity is numeric
    CURRENT_TIMESTAMP AS start_date,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS end_date,
    CURRENT_TIMESTAMP AS load_date,
    'stage' AS record_source
FROM stage_warehouses
WHERE warehouse_id IS NOT NULL -- Ensure no NULL warehouse_id
  AND capacity > 0; -- Exclude warehouses with invalid capacity

INSERT INTO hub_warehouses (warehouse_id, load_date, record_source)
SELECT DISTINCT warehouse_id, CURRENT_TIMESTAMP, 'stage'
FROM temp_stage_warehouses;

INSERT INTO sat_warehouses (warehouse_id, region, capacity, start_date, end_date, load_date, record_source)
SELECT
    warehouse_id,
    region,
    capacity,
    start_date,
    end_date,
    load_date,
    record_source
FROM temp_stage_warehouses;

select * from hub_warehouses order by warehouse_id;
select * from sat_warehouses;

-- Create Hub table for Orders
CREATE TABLE hub_orders (
    order_id BIGINT PRIMARY KEY,   -- Business key for order
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Date when the record was loaded
    record_source VARCHAR(255) -- Source of the record (e.g., 'source_system_name')
);

-- Create Satellite table for Orders (stores the descriptive attributes)
CREATE TABLE sat_orders (
    order_id BIGINT,                  -- Foreign Key referencing the Hub (order_id)
    client_id BIGINT,                 -- Client ID (referenced from the hub_clients table)
    product_id BIGINT,                -- Product ID (referenced from the hub_products table)
    order_date DATE,                  -- Date of the order
    quantity INT,                     -- Quantity of the product ordered
    start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Date when the record was created
    end_date TIMESTAMP,               -- Date when the record was replaced (null if active)
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Date when the record was loaded
    record_source VARCHAR(255),       -- Source of the record (e.g., 'source_system_name')
    PRIMARY KEY (order_id, start_date) -- Composite Primary Key to track historical records
);

drop table hub_orders, sat_orders

-- Hub Table for Orders
CREATE TABLE hub_orders (
    order_id BIGINT PRIMARY KEY, -- Unique identifier for each order
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Date when the record is loaded
    record_source VARCHAR(50) NOT NULL -- Source of the record (e.g., 'stage')
);

-- Satellite Table for Orders
CREATE TABLE sat_orders (
    order_id BIGINT NOT NULL, -- Foreign key referencing hub_orders
    order_date DATE NOT NULL, -- Date of the order
    start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When the record becomes valid
    end_date TIMESTAMP DEFAULT '9999-12-31 23:59:59', -- When the record is no longer valid
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Load date
    record_source VARCHAR(50) NOT NULL, -- Source of the record
    PRIMARY KEY (order_id, start_date), -- Composite key for SCD2
    FOREIGN KEY (order_id) REFERENCES hub_orders (order_id)
);

-- Temporary table for verified and transformed data
CREATE TEMP TABLE temp_stage_orders AS
SELECT
    o.order_id,
    o.order_id,
    o.order_date::DATE AS order_date, -- Ensure valid date format
    CURRENT_TIMESTAMP AS start_date,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS end_date,
    CURRENT_TIMESTAMP AS load_date,
    'stage' AS record_source
FROM stage_orders o
WHERE o.order_id IS NOT NULL -- Ensure order_id is not null
  AND o.product_id IS NOT NULL -- Ensure product_id is not null
  AND o.order_date IS NOT NULL -- Ensure order_date is not null
  AND o.quantity > 0; -- Ensure quantity is positive

  -- Insert into Hub Table
INSERT INTO hub_orders (order_id, load_date, record_source)
SELECT DISTINCT order_id, CURRENT_TIMESTAMP, 'stage'
FROM temp_stage_orders
ON CONFLICT (order_id) DO NOTHING; -- Prevent duplicate entries

-- Insert into Satellite Table
INSERT INTO sat_orders (order_id, order_date, start_date, end_date, load_date, record_source)
SELECT
    order_id,
    order_date,
    start_date,
    end_date,
    load_date,
    record_source
FROM temp_stage_orders;

drop table hub_orders,sat_orders

select * from hub_orders;
select * from sat_orders

ALTER TABLE sat_orders DROP client_id

CREATE TABLE link_order_product (
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    load_date TIMESTAMP NOT NULL,
    record_source VARCHAR(50) NOT NULL,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES hub_orders(order_id),
    FOREIGN KEY (product_id) REFERENCES hub_products(product_id)
);

CREATE TABLE link_product_warehouse (
    product_id BIGINT NOT NULL,
    warehouse_id BIGINT NOT NULL,
    load_date TIMESTAMP NOT NULL,
    record_source VARCHAR(50) NOT NULL,
    PRIMARY KEY (product_id, warehouse_id),
    FOREIGN KEY (product_id) REFERENCES hub_products(product_id),
    FOREIGN KEY (warehouse_id) REFERENCES hub_warehouses(warehouse_id)
);

CREATE INDEX idx_link_order_product_order_id ON link_order_product(order_id);
CREATE INDEX idx_link_order_product_product_id ON link_order_product(product_id);
CREATE INDEX idx_link_product_warehouse_product_id ON link_product_warehouse(product_id);
CREATE INDEX idx_link_product_warehouse_warehouse_id ON link_product_warehouse(warehouse_id);

CREATE TABLE link_order_client (
    order_id BIGINT NOT NULL,
    client_id BIGINT NOT NULL,
    load_date TIMESTAMP NOT NULL,
    record_source VARCHAR(50) NOT NULL,
    PRIMARY KEY (order_id, client_id),
    FOREIGN KEY (order_id) REFERENCES hub_orders(order_id),
    FOREIGN KEY (client_id) REFERENCES hub_clients(client_id)
);

ALTER TABLE sat_orders
ADD CONSTRAINT sat_orders_client_id_fkey FOREIGN KEY (client_id) REFERENCES hub_clients(client_id);

CREATE INDEX idx_sat_orders_client_id ON sat_orders(client_id);
CREATE INDEX idx_sat_orders_order_date ON sat_orders(order_date);

ALTER TABLE sat_orders DROP COLUMN client_id;

select * from link_order_client

CREATE TEMP TABLE temp_stage_orders AS
SELECT
    o.order_id,
    o.client_id,
    o.product_id,
    o.order_date::DATE AS order_date, -- Ensure valid date format
    CURRENT_TIMESTAMP AS start_date,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS end_date,
    CURRENT_TIMESTAMP AS load_date,
    'stage' AS record_source
FROM stage_orders o
WHERE o.order_id IS NOT NULL -- Ensure order_id is not null
  AND o.product_id IS NOT NULL -- Ensure product_id is not null
  AND o.order_date IS NOT NULL -- Ensure order_date is not null
  AND o.quantity > 0; -- Ensure quantity is positive

INSERT INTO link_order_product (order_id, product_id, load_date, record_source)
SELECT
    order_id,
    product_id,
    CURRENT_TIMESTAMP AS load_date,
    'stage' AS record_source
FROM temp_stage_orders; -- or stage_orders

INSERT INTO link_order_client (order_id, client_id, load_date, record_source)
SELECT
    order_id,
    client_id,
    CURRENT_TIMESTAMP AS load_date,
    'stage' AS record_source
FROM temp_stage_orders; -- or stage_orders

select * from link_order_client;
select * from link_order_product;
select * from link_product_warehouse

INSERT INTO link_product_warehouse (product_id, load_date, record_source)
SELECT
    product_id,
    warehouse_id,
    CURRENT_TIMESTAMP AS load_date,
    'stage' AS record_source
FROM stage_products; -- or stage_products


INSERT INTO link_product_warehouse (product_id, load_date, record_source)
SELECT
    product_id,
    warehouse_id,
    CURRENT_TIMESTAMP AS load_date,
    'stage' AS record_source
FROM stage_products;

WITH ranked_warehouses AS (
    SELECT
        warehouse_id,
        region,
        capacity,
        ROW_NUMBER() OVER (ORDER BY capacity DESC) AS rank
    FROM stage_warehouses
),
product_assignments AS (
    SELECT
        sp.product_id,
        rw.warehouse_id,
        ROW_NUMBER() OVER (PARTITION BY rw.warehouse_id ORDER BY sp.product_id) AS assignment_rank
    FROM stage_products sp
    CROSS JOIN ranked_warehouses rw
)
INSERT INTO link_product_warehouse (product_id, warehouse_id, load_date, record_source)
SELECT
    pa.product_id,
    pa.warehouse_id,
    CURRENT_TIMESTAMP AS load_date,
    'stage' AS record_source
FROM product_assignments pa
WHERE pa.assignment_rank <= (SELECT capacity FROM stage_warehouses WHERE warehouse_id = pa.warehouse_id);


-- Dimension Tables

-- DimDate
CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    DateValue DATE NOT NULL,
    Day INT,
    Month INT,
    Year INT
    -- Add other date attributes as needed
);

-- DimClient
CREATE TABLE DimClient (
    ClientKey INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    ClientID BIGINT UNIQUE NOT NULL,
    ClientName VARCHAR(100),
    Email VARCHAR(100),
    Phone VARCHAR(15),
    Region VARCHAR(50)
    -- Add other client attributes as needed
);

-- DimProduct
CREATE TABLE DimProduct (
    ProductKey INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    ProductID BIGINT UNIQUE NOT NULL,
    ProductName VARCHAR(100),
    Category VARCHAR(50)
    -- Add other product attributes as needed
);

-- DimWarehouse
CREATE TABLE DimWarehouse (
    WarehouseKey INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    WarehouseID BIGINT UNIQUE NOT NULL,
    WarehouseName VARCHAR(100),
    Region VARCHAR(50),
    Capacity NUMERIC(10,2)
    -- Add other warehouse attributes as needed
);

-- DimOrder
CREATE TABLE DimOrder (
    OrderKey INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    OrderID BIGINT UNIQUE NOT NULL,
    OrderDate DATE
    -- Add other order attributes as needed
);

-- Fact Tables

-- FactSales
CREATE TABLE FactSales (
    SalesKey INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    ClientKey INT NOT NULL,
    ProductKey INT NOT NULL,
    WarehouseKey INT NOT NULL,
    DateKey INT NOT NULL,
    Quantity INT,
    SalesAmount NUMERIC(10,2),
    FOREIGN KEY (ClientKey) REFERENCES DimClient(ClientKey),
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    FOREIGN KEY (WarehouseKey) REFERENCES DimWarehouse(WarehouseKey),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey)
);

-- FactInventory
CREATE TABLE FactInventory (
    InventoryKey INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    ProductKey INT NOT NULL,
    WarehouseKey INT NOT NULL,
    DateKey INT NOT NULL,
    QuantityOnHand INT,
    QuantityReserved INT,
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    FOREIGN KEY (WarehouseKey) REFERENCES DimWarehouse(WarehouseKey),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey)
);

-- FactProductMovement
CREATE TABLE FactProductMovement (
    MovementKey INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    ProductKey INT NOT NULL,
    SourceWarehouseKey INT NOT NULL,
    DestinationWarehouseKey INT NOT NULL,
    DateKey INT NOT NULL,
    QuantityMoved INT,
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    FOREIGN KEY (SourceWarehouseKey) REFERENCES DimWarehouse(WarehouseKey),
    FOREIGN KEY (DestinationWarehouseKey) REFERENCES DimWarehouse(WarehouseKey),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey)
);

-- FactCustomerActivity
CREATE TABLE FactCustomerActivity (
    ActivityKey INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    ClientKey INT NOT NULL,
    OrderKey INT NOT NULL,
    DateKey INT NOT NULL,
    ActivityType VARCHAR(50),
    ActivityDetails VARCHAR(200),
    FOREIGN KEY (ClientKey) REFERENCES DimClient(ClientKey),
    FOREIGN KEY (OrderKey) REFERENCES DimOrder(OrderKey),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey)
);

-- Dimension Tables

-- DimDate
CREATE TABLE DimDate (
    DateKey INTEGER PRIMARY KEY,
    DateValue DATE NOT NULL,
    Day INTEGER,
    Month INTEGER,
    Year INTEGER
    -- Add other date attributes as needed
);

-- DimProduct
CREATE TABLE DimProduct (
    ProductKey INTEGER PRIMARY KEY,
    ProductID BIGINT UNIQUE NOT NULL,
    ProductName VARCHAR(100),
    Category VARCHAR(50)
    -- Add other product attributes as needed
);

-- DimClient
CREATE TABLE DimClient (
    ClientKey INTEGER PRIMARY KEY,
    ClientID BIGINT UNIQUE NOT NULL,
    ClientName VARCHAR(100),
    Email VARCHAR(100)
    -- Add other client attributes as needed
);

-- DimWarehouse
CREATE TABLE DimWarehouse (
    WarehouseKey INTEGER PRIMARY KEY,
    WarehouseID BIGINT UNIQUE NOT NULL,
    WarehouseName VARCHAR(100),
    Region VARCHAR(50)
    -- Add other warehouse attributes as needed
);

-- DimOrder
CREATE TABLE DimOrder (
    OrderKey INTEGER PRIMARY KEY,
    OrderID BIGINT UNIQUE NOT NULL,
    OrderDate DATE
    -- Add other order attributes as needed
);

-- Fact Tables

-- FactSales
CREATE TABLE FactSales (
    SalesKey INTEGER PRIMARY KEY,
    DateKey INTEGER NOT NULL,
    ProductKey INTEGER NOT NULL,
    ClientKey INTEGER NOT NULL,
    WarehouseKey INTEGER NOT NULL,
    SalesAmount NUMERIC(10,2),
    Quantity INTEGER,
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    FOREIGN KEY (ClientKey) REFERENCES DimClient(ClientKey),
    FOREIGN KEY (WarehouseKey) REFERENCES DimWarehouse(WarehouseKey)
);

-- FactInventory
CREATE TABLE FactInventory (
    InventoryKey INTEGER PRIMARY KEY,
    DateKey INTEGER NOT NULL,
    ProductKey INTEGER NOT NULL,
    WarehouseKey INTEGER NOT NULL,
    QuantityOnHand INTEGER,
    QuantityReserved INTEGER,
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    FOREIGN KEY (WarehouseKey) REFERENCES DimWarehouse(WarehouseKey)
);

-- FactProductMovement
CREATE TABLE FactProductMovement (
    MovementKey INTEGER PRIMARY KEY,
    DateKey INTEGER NOT NULL,
    ProductKey INTEGER NOT NULL,
    SourceWarehouseKey INTEGER NOT NULL,
    DestinationWarehouseKey INTEGER NOT NULL,
    QuantityMoved INTEGER,
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    FOREIGN KEY (SourceWarehouseKey) REFERENCES DimWarehouse(WarehouseKey),
    FOREIGN KEY (DestinationWarehouseKey) REFERENCES DimWarehouse(WarehouseKey)
);

-- FactCustomerActivity
CREATE TABLE FactCustomerActivity (
    ActivityKey INTEGER PRIMARY KEY,
    DateKey INTEGER NOT NULL,
    ClientKey INTEGER NOT NULL,
    OrderKey INTEGER,
    ActivityType VARCHAR(50),
    ActivityDetails VARCHAR(200),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (ClientKey) REFERENCES DimClient(ClientKey),
    FOREIGN KEY (OrderKey) REFERENCES DimOrder(OrderKey)
);

-- Drop Fact Tables
DROP TABLE IF EXISTS FactSales CASCADE;
DROP TABLE IF EXISTS FactInventory CASCADE;
DROP TABLE IF EXISTS FactProductMovement CASCADE;
DROP TABLE IF EXISTS FactCustomerActivity CASCADE;

-- Drop Dimension Tables
DROP TABLE IF EXISTS DimDate CASCADE;
DROP TABLE IF EXISTS DimProduct CASCADE;
DROP TABLE IF EXISTS DimClient CASCADE;
DROP TABLE IF EXISTS DimWarehouse CASCADE;
DROP TABLE IF EXISTS DimOrder CASCADE;

-- Drop Sequences (if they were created manually)
DROP SEQUENCE IF EXISTS seq_dimdate CASCADE;
DROP SEQUENCE IF EXISTS seq_dimproduct CASCADE;
DROP SEQUENCE IF EXISTS seq_dimclient CASCADE;
DROP SEQUENCE IF EXISTS seq_dimwarehouse CASCADE;
DROP SEQUENCE IF EXISTS seq_dimorder CASCADE;
DROP SEQUENCE IF EXISTS seq_factsales CASCADE;
DROP SEQUENCE IF EXISTS seq_factinventory CASCADE;
DROP SEQUENCE IF EXISTS seq_factproductmovement CASCADE;
DROP SEQUENCE IF EXISTS seq_factcustomeractivity CASCADE;

-- Create tables with auto-incrementing primary keys using SERIAL

-- Dimension Tables

-- DimDate
CREATE TABLE DimDate (
    DateKey SERIAL PRIMARY KEY,
    DateValue DATE NOT NULL,
    Day INTEGER,
    Month INTEGER,
    Year INTEGER
    -- Add other date attributes as needed
);

-- DimProduct
CREATE TABLE DimProduct (
    ProductKey SERIAL PRIMARY KEY,
    ProductID BIGINT UNIQUE NOT NULL,
    ProductName VARCHAR(100),
    Category VARCHAR(50)
    -- Add other product attributes as needed
);

-- DimClient
CREATE TABLE DimClient (
    ClientKey SERIAL PRIMARY KEY,
    ClientID BIGINT UNIQUE NOT NULL,
    ClientName VARCHAR(100),
    Email VARCHAR(100)
    -- Add other client attributes as needed
);

-- DimWarehouse
CREATE TABLE DimWarehouse (
    WarehouseKey SERIAL PRIMARY KEY,
    WarehouseID BIGINT UNIQUE NOT NULL,
    WarehouseName VARCHAR(100),
    Region VARCHAR(50)
    -- Add other warehouse attributes as needed
);

-- DimOrder
CREATE TABLE DimOrder (
    OrderKey SERIAL PRIMARY KEY,
    OrderID BIGINT UNIQUE NOT NULL,
    OrderDate DATE
    -- Add other order attributes as needed
);

-- Fact Tables

-- FactSales
CREATE TABLE FactSales (
    SalesKey SERIAL PRIMARY KEY,
    DateKey INTEGER NOT NULL,
    ProductKey INTEGER NOT NULL,
    ClientKey INTEGER NOT NULL,
    WarehouseKey INTEGER NOT NULL,
    SalesAmount NUMERIC(10,2),
    Quantity INTEGER,
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    FOREIGN KEY (ClientKey) REFERENCES DimClient(ClientKey),
    FOREIGN KEY (WarehouseKey) REFERENCES DimWarehouse(WarehouseKey)
);

-- FactInventory
CREATE TABLE FactInventory (
    InventoryKey SERIAL PRIMARY KEY,
    DateKey INTEGER NOT NULL,
    ProductKey INTEGER NOT NULL,
    WarehouseKey INTEGER NOT NULL,
    QuantityOnHand INTEGER,
    QuantityReserved INTEGER,
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    FOREIGN KEY (WarehouseKey) REFERENCES DimWarehouse(WarehouseKey)
);

-- FactProductMovement
CREATE TABLE FactProductMovement (
    MovementKey SERIAL PRIMARY KEY,
    DateKey INTEGER NOT NULL,
    ProductKey INTEGER NOT NULL,
    SourceWarehouseKey INTEGER NOT NULL,
    DestinationWarehouseKey INTEGER NOT NULL,
    QuantityMoved INTEGER,
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    FOREIGN KEY (SourceWarehouseKey) REFERENCES DimWarehouse(WarehouseKey),
    FOREIGN KEY (DestinationWarehouseKey) REFERENCES DimWarehouse(WarehouseKey)
);

-- FactCustomerActivity
CREATE TABLE FactCustomerActivity (
    ActivityKey SERIAL PRIMARY KEY,
    DateKey INTEGER NOT NULL,
    ClientKey INTEGER NOT NULL,
    OrderKey INTEGER,
    ActivityType VARCHAR(50),
    ActivityDetails VARCHAR(200),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (ClientKey) REFERENCES DimClient(ClientKey),
    FOREIGN KEY (OrderKey) REFERENCES DimOrder(OrderKey)
);

-- Load DimDate
INSERT INTO DimDate (DateValue, Day, Month, Year)
SELECT DISTINCT
    order_date,
    EXTRACT(DAY FROM order_date),
    EXTRACT(MONTH FROM order_date),
    EXTRACT(YEAR FROM order_date)
FROM
    sat_orders
-- 
select * from DimDate

-- Load DimProduct
INSERT INTO DimProduct (ProductID, ProductName, Category)
SELECT DISTINCT
    product_id,
    name,
    category
FROM
    sat_products;

select * from DimProduct

-- Load DimClient
INSERT INTO DimClient (ClientID, ClientName, Email)
SELECT DISTINCT
    client_id,
    name,
    email
FROM
    sat_clients;

select * from DimClient

-- Load DimWarehouse
INSERT INTO DimWarehouse (WarehouseID, Region)
SELECT DISTINCT
    warehouse_id,
    region
FROM
    sat_warehouses;

select * from DimWarehouse
alter table DimWarehouse drop column WarehouseName

-- Load DimOrder
INSERT INTO DimOrder (OrderID, OrderDate)
SELECT DISTINCT
    order_id,
    order_date
FROM
    sat_orders;

select * from DimOrder

-- Load factsales
INSERT INTO factsales (datekey, productkey, clientkey, warehousekey, salesamount, quantity)
SELECT 
       dd.datekey,
       dp.productkey,
       dc.clientkey,
       dw.warehousekey,
       s.quantity * sp.price AS salesamount,
       s.quantity
FROM stage_orders s
JOIN link_order_client loc ON s.order_id = loc.order_id
JOIN link_order_product lop ON s.order_id = lop.order_id
JOIN link_product_warehouse lpw ON lop.product_id = lpw.product_id
JOIN sat_orders so ON s.order_id = so.order_id
JOIN sat_products sp ON s.product_id = sp.product_id
JOIN dimdate dd ON so.order_date = dd.datevalue
JOIN dimproduct dp ON sp.product_id = dp.productid
JOIN dimclient dc ON loc.client_id = dc.clientid
JOIN dimwarehouse dw ON lpw.warehouse_id = dw.warehouseid
;

select * from factsales

INSERT INTO factinventory (datekey, productkey, warehousekey, quantityonhand, quantityreserved)
SELECT 
       dd.datekey,
       dp.productkey,
       dw.warehousekey,
       src.quantity_on_hand,
       src.quantity_reserved
FROM source_inventory_table src
JOIN sat_products sp ON src.product_id = sp.product_id
JOIN sat_warehouses sw ON src.warehouse_id = sw.warehouse_id
JOIN dimdate dd ON src.inventory_date = dd.datevalue
JOIN dimproduct dp ON sp.product_id = dp.productid
JOIN dimwarehouse dw ON sw.warehouse_id = dw.warehouseid
;

-- Load factproductmovement
INSERT INTO factproductmovement (movementkey, datekey, productkey, sourcewarehousekey, destinationwarehousekey, quantitymoved)
SELECT NEXTVAL('factproductmovement_movementkey_seq'),
       dd.datekey,
       dp.productkey,
       dw_source.warehousekey,
       dw_dest.warehousekey,
       pm.quantity_moved
FROM stage_productmovements pm
JOIN sat_products sp ON pm.product_id = sp.product_id
JOIN sat_warehouses sw_source ON pm.source_warehouse_id = sw_source.warehouse_id
JOIN sat_warehouses sw_dest ON pm.destination_warehouse_id = sw_dest.warehouse_id
JOIN dimdate dd ON pm.movement_date = dd.datevalue
JOIN dimproduct dp ON sp.product_id = dp.productid
JOIN dimwarehouse dw_source ON sw_source.warehouse_id = dw_source.warehouseid
JOIN dimwarehouse dw_dest ON sw_dest.warehouse_id = dw_dest.warehouseid
WHERE sp.end_date = '9999-12-31'
  AND sw_source.end_date = '9999-12-31'
  AND sw_dest.end_date = '9999-12-31';

-- Load factcustomeractivity
INSERT INTO factcustomeractivity (activitykey, datekey, clientkey, orderkey, activitytype, activitydetails)
SELECT NEXTVAL('factcustomeractivity_activitykey_seq'),
       dd.datekey,
       dc.clientkey,
       do.orderkey,
       s.activity_type,
       s.activity_details
FROM stage_activities s
JOIN link_order_client loc ON s.order_id = loc.order_id
JOIN sat_orders so ON s.order_id = so.order_id
JOIN dimdate dd ON s.activity_date = dd.datevalue
JOIN dimclient dc ON loc.client_id = dc.clientid
JOIN dimorder do ON so.order_id = do.orderid
WHERE so.end_date = '9999-12-31';