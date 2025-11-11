--Retail Store Sales// Data Cleaning
-- available files: retailsales (retailsales.csv), categories (EHE.csv, FUR.csv)
-- retailsales has missing data in the category column (part of the cleaning task), but are only 2 category tables and 8 distinct categories. 
-- Author has confirmed that the other 6 category tables follow the same pattern as existing 2.

--1. Add all tables into pgadmin. Combined category tables into 1. 
-- Entered all missing data, except for 'itemname'. code obtained from GEMINI.
WITH SourceData AS (
    -- Generate the item number (1 to 25) dynamically using ROW_NUMBER()
    SELECT
        price,
        ROW_NUMBER() OVER () AS item_num -- Assigns 1, 2, 3... to each price row
    FROM
        (VALUES
            (5.0), (6.5), (8.0), (9.5), (11.0),
            (12.5), (14.0), (15.5), (17.0), (18.5),
            (20.0), (21.5), (23.0), (24.5), (26.0),
            (27.5), (29.0), (30.5), (32.0), (33.5),
            (35.0), (36.5), (38.0), (39.5), (41.0)
        ) AS t(price) -- Alias 't' is required, price is the column name for the values
),
Suffixes (suffix) AS (
    VALUES
        ('CEA'), ('FOOD'), ('BEV'), ('MILK'), ('PAT'), ('BUT')
)
INSERT INTO categories (itemcode, itemname, price)
SELECT
    -- Use the calculated item_num, converting it to text
    'Item_' || sd.item_num::TEXT || '_' || s.suffix,
    NULL,
    sd.price
FROM
    SourceData sd
CROSS JOIN
    Suffixes s;
-- add itemcategory column data from retailsales table
UPDATE categories
SET itemcategory = retailsales.category
FROM retailsales
WHERE itemcode = retailsales.item

--2.Create stg_categories, remove duplicates in stg_categories
CREATE TABLE stg_categories AS (
	SELECT *,
	ROW_NUMBER () OVER ( PARTITION BY itemcode) AS rownum
	FROM categories
	);
-- Remove duplicates
DELETE FROM stg_categories
WHERE rownum >1;
-- Drop rownum column
ALTER TABLE stg_categories
DROP COLUMN rownum;

--3. Create stg_retailsales, remove duplicates in stg_retailsales
CREATE TABLE stg_retailsales AS
SELECT *
    NOW() AS load_timestamp,
    'NA' AS source_system_name
FROM retailsales;
-- Used transactionid to check for duplicates. 
WITH dupecheck AS(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY transactionid ) AS rownum
FROM stg_retailsales
)
SELECT *
FROM dupecheck
WHERE rownum > 1;
--//--//-- no duplicates found

-- 4. Standardizing Data
-- Create another staging table
CREATE TABLE stg2_retailsales AS
    SELECT *
        NOW() AS load_timestamp,
        'NA' AS source_system_name
    FROM stg_retailsales;
-- remove trailing blanks for all text columns in stg2_retailsales
UPDATE stg2_retailsales
SET
    transactionid = TRIM(transactionid),
    customerid = TRIM(customerid),
    category = TRIM(category),
    item = TRIM(item),
    paymentmethod = TRIM(paymentmethod),
    location = TRIM(location),
    discountapplied = TRIM(discountapplied)
WHERE 
    -----Included for performance purposes. Only update row where at least one of these columns has a space
    transactionid LIKE ' %' OR transactionid LIKE '% ' OR 
    customerid LIKE ' %' OR customerid LIKE '% ' OR 
    category LIKE ' %' OR category LIKE '% ' OR 
    item LIKE ' %' OR item LIKE '% ' OR 
    paymentmethod LIKE ' %' OR paymentmethod LIKE '% ' OR 
    location LIKE ' %' OR location LIKE '% ' OR 
    discountapplied LIKE ' %' OR discountapplied LIKE '% ';
-- Check category column for similar records (ie milk & dairy).
SELECT DISTINCT(category)
FROM stg2_retailsales
ORDER BY category;
-- Check category paymentmethod for similar records
SELECT DISTINCT(paymentmethod)
FROM stg2_retailsales;
-- Check category location for similar records
SELECT DISTINCT(location)
FROM stg2_retailsales;
-- Check item paymentmethod for similar records
SELECT DISTINCT (item)
FROM stg2_retailsales
ORDER BY item;
-- Check discountapplied paymentmethod for similar records
SELECT DISTINCT (discountapplied)
FROM stg2_retailsales;
---//---//-- No similar records. But mispelling and nulls spotted.
-- Change spelling. stg2_retailsales.category: "Milk Products" = "Milk products"
UPDATE stg2_retailsales
SET category = 'Milk products'
WHERE category = 'Milk Products';
-- change spelling. stg_items.itemcategory: "Milk Products" = "Milk products"
UPDATE stg_categories
SET itemcategory = 'Milk products'
WHERE itemcategory = 'Milk Products';
-- 4.3 changed 'quantity' col type to INTEGER
ALTER TABLE stg2_retailsales
ALTER COLUMN quantity TYPE INTEGER

-- 5. NULL handling
-- 5.1 Find out which columns have nulls
SELECT
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN transactionid IS NULL THEN 1 END) AS transactionid,
    COUNT(CASE WHEN customerid IS NULL THEN 1 END) AS customerid,
    COUNT(CASE WHEN category IS NULL THEN 1 END) AS category,
	COUNT(CASE WHEN item IS NULL THEN 1 END) AS item,
    COUNT(CASE WHEN priceperunit IS NULL THEN 1 END) AS priceperunit,
    COUNT(CASE WHEN quantity IS NULL THEN 1 END) AS quantity,
	COUNT(CASE WHEN totalspent IS NULL THEN 1 END) AS totalspent,
    COUNT(CASE WHEN paymentmethod IS NULL THEN 1 END) AS paymentmethod,
    COUNT(CASE WHEN location IS NULL THEN 1 END) AS location,
    COUNT(CASE WHEN transactiondate IS NULL THEN 1 END) AS transactiondate,
    COUNT(CASE WHEN discountapplied IS NULL THEN 1 END) AS discountapplied
FROM
    stg2_retailsales;
--//--//-- Found columns with null. (item, priceperunit, quantity, totalspent)
-- Replace null values in stg2_retailsales, by creating stg3_retailsales.
CREATE TABLE stg3_retailsales AS
SELECT     
	transactionid,
    customerid,
    category,
    item,
    priceperunit,
    quantity,
    totalspent,
    paymentmethod,
    location,
    transactiondate,
	discountapplied,
	ROW_NUMBER() OVER (PARTITION BY transactionid ) AS rownum,
    NOW() AS load_timestamp,
    'NA' AS source_system_name 
FROM stg2_retailsales
--update 'priceperunit'
UPDATE stg3_retailsales
SET priceperunit = stg_categories.price
FROM stg_categories
WHERE priceperunit IS NULL;
--update 'quantity'
UPDATE stg3_retailsales
SET quantity = totalspent / priceperunit
WHERE quantity IS NULL;
--update 'totalspent'
UPDATE stg3_retailsales
SET totalspent = priceperunit * quantity
WHERE totalspent IS NULL;
--update 'item'
UPDATE stg3_retailsales
SET item = stg_categories.itemcode
FROM stg_categories
WHERE stg3_retailsales.item IS NULL 
	AND stg_categories.price = stg3_retailsales.priceperunit
	AND stg_categories.itemcategory = stg3_retailsales.category ;
--check for nulls again
SELECT
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN transactionid IS NULL THEN 1 END) AS transactionid,
    COUNT(CASE WHEN customerid IS NULL THEN 1 END) AS customerid,
    COUNT(CASE WHEN category IS NULL THEN 1 END) AS category,
	COUNT(CASE WHEN item IS NULL THEN 1 END) AS item,
    COUNT(CASE WHEN priceperunit IS NULL THEN 1 END) AS priceperunit,
    COUNT(CASE WHEN quantity IS NULL THEN 1 END) AS quantity,
	COUNT(CASE WHEN totalspent IS NULL THEN 1 END) AS totalspent,
    COUNT(CASE WHEN paymentmethod IS NULL THEN 1 END) AS paymentmethod,
    COUNT(CASE WHEN location IS NULL THEN 1 END) AS location,
    COUNT(CASE WHEN transactiondate IS NULL THEN 1 END) AS transactiondate,
    COUNT(CASE WHEN discountapplied IS NULL THEN 1 END) AS discountapplied
FROM
    stg3_retailsales;
--same number of nulls were found in quantity and total spent column. 
SELECT
    COUNT (CASE WHEN quantity IS NULL THEN 1 END) AS quant_null,
    COUNT (CASE WHEN totalspent IS NULL THEN 1 END) AS totalspent_null,
    COUNT (CASE WHEN quantity IS NULL or totalspent is null then 1 end) as OR,
    COUNT (CASE WHEN quantity IS NULL AND totalspent IS NULL then 1 end) AS AND
FROM stg3_retailsales
--Records showing null in quantity also show null in total spent.

-- 6. Check relevance of discountapplied column, using retailsales original table
--Check if discountapplied = TRUE, totalspent/quantity <> priceperunit
SELECT *
FROM retailsales
WHERE discountapplied LIKE 'True' AND totalspent/quantity <> priceperunit;
--//--//-- no records shown.
--Check if priceperunit is already discounted
SELECT *
FROM retailsales
WHERE discountapplied LIKE 'True'
ORDER BY item;
--//--//-- scanning through, prices no different from discountapplied = 'False' or null

------- data cleaning completed -----
