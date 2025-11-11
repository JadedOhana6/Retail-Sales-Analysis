--Retail Store Sales// Data Cleaning
-- available files: retailsales (retailsales.csv), categories (EHE.csv, FUR.csv)
-- retailsales has missing data in the category column (part of the cleaning task), but are only 2 category tables and 9 distinct categories. 
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


--2.REMOVE DUPLICATES
--Create stg_categories & rownum
CREATE TABLE stg_categories AS (
	SELECT *,
	ROW_NUMBER () OVER ( PARTITION BY itemcode) AS rownum
	FROM categories
	);
-- Drop rownum column
ALTER TABLE stg_categories
DROP COLUMN rownum;

--Create stg_retailsales
CREATE TABLE stg_retailsales AS
SELECT *,
    NOW() AS load_timestamp,
    'NA' AS source_system_name,
    ROW_NUMBER() OVER (PARTITION BY transactionid ) AS rownum
FROM retailsales;
-- add rownum
SELECT *
FROM stg_retailsales
WHERE rownum > 1;
--//--//-- no duplicates found

-- 4. Standardizing Data
-- Create another staging table for retailsales
CREATE TABLE stg2_retailsales AS
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
-- Create another staging table for categories table
CREATE TABLE stg2_categories AS
SELECT
    itemcode,
    itemname,
    price,
    NOW() AS load_timestamp,
    'NA' AS source_system_name
FROM stg_categories;
-- trim text columns
UPDATE stg2_categories
SET 
    itemcode = TRIM(itemcode),
    itemname = TRIM(itemname)
WHERE 
    itemcode LIKE '% ' OR itemcode LIKE ' %' OR
    itemname LIKE '% ' OR itemname LIKE ' %';
-- Check for text columns in stg_retailsales for records that can be groupped together. (ie ewallet & GooglePay)
-- category column
SELECT DISTINCT(category)
FROM stg2_retailsales
ORDER BY category;
-- paymentmethod
SELECT DISTINCT(paymentmethod)
FROM stg2_retailsales;
-- location
SELECT DISTINCT(location)
FROM stg2_retailsales;
-- item
SELECT DISTINCT (item)
FROM stg2_retailsales
ORDER BY item;
-- discountapplied
SELECT DISTINCT (discountapplied)
FROM stg2_retailsales;
---//---//-- No similar records. But mispellings.
-- Change spelling. stg2_retailsales.category: "Milk Products" = "Milk products"
UPDATE stg2_retailsales
SET category = 'Milk products'
WHERE category = 'Milk Products';
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
SELECT
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN itemcode IS NULL THEN 1 END) AS itemcode,
    COUNT(CASE WHEN itemname IS NULL THEN 1 END) AS itemname,
    COUNT(CASE WHEN price IS NULL THEN 1 END) AS price
FROM
    stg2_categories;
--//--//-- Found column with null. (price)
--5.2 fill columns
-- addcol stg2_categories.itemcategory
ALTER TABLE stg2_categories
ADD COLUMN itemcategory text;
-- update stg2_categories.itemcategory
UPDATE stg2_categories
SET itemcategory = stg2_retailsales.category
FROM stg2_retailsales
WHERE itemcode = stg2_retailsales.item
-- check for any nulls in stg2_categories.itemcategory
SELECT
    COUNT(CASE WHEN itemcategory IS NULL THEN 1 END) AS itemcategory_nullcount,
    COUNT(CASE WHEN itemcode IS NULL THEN 1 END)AS itemcode_nullcount,
    COUNT (CASE WHEN price IS NULL THEN 1 END) AS price_nullcount
FROM stg2_categories;
--update stg2_retailsales.quantity
UPDATE stg2_retailsales
SET quantity = totalspent / priceperunit
WHERE quantity IS NULL;
--update stg2_retailsales.totalspent
UPDATE stg2_retailsales
SET totalspent = priceperunit * quantity
WHERE totalspent IS NULL;
-- update stg2_retailsales.priceperunit
UPDATE stg_retailsales 
SET priceperunit = totalspent/quantity
WHERE priceperunit IS NULL
--update stg2_retailsales.item
UPDATE stg2_retailsales AS r
SET item = c.itemcode
FROM stg2_categories AS c
WHERE r.priceperunit = c.price
AND r.category = c.itemcategory
AND r.item IS NULL
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
    stg2_retailsales;
--same number of nulls were found in quantity and total spent column. 
SELECT
    COUNT (CASE WHEN quantity IS NULL THEN 1 END) AS quant_null,
    COUNT (CASE WHEN totalspent IS NULL THEN 1 END) AS totalspent_null,
    COUNT (CASE WHEN quantity IS NULL or totalspent is null then 1 end) as OR,
    COUNT (CASE WHEN quantity IS NULL AND totalspent IS NULL then 1 end) AS AND
FROM stg2_retailsales
--Records showing nulls for both columns in the AND section of the query. 

-- 6. Check relevance of discountapplied column, using retailsales original table
--Check if discountapplied = TRUE, totalspent/quantity <> priceperunit
SELECT *
FROM stg2_retailsales
WHERE discountapplied LIKE 'TRUE' AND totalspent/quantity <> priceperunit;
--//--//-- The output shows that totalspent/quantity <> priceperunit  = discountapplied TRUE
--//--//--priceperunit / totalspent does not show whether discount was used. 
-- Check if discountapplied is affected by item
SELECT
item
FROM stg2_retailsales
GROUP BY item
HAVING COUNT (CASE WHEN discountapplied LIKE 'TRUE' THEN 1 END) = 0 
	OR COUNT (CASE WHEN discountapplied LIKE 'FALSE' THEN 1 END) = 0
--//--//-- The output shows not affected
-- Check if discountapplied is affected by transaction date range
SELECT
MAX (transactiondate),
MIN (transactiondate)
FROM stg2_retailsales
WHERE discountapplied LIKE 'TRUE'
;
SELECT
MAX (transactiondate),
MIN (transactiondate)
FROM stg2_retailsales
WHERE discountapplied LIKE 'FALSE'
;
--//--//-- discountapplied is not affected by transactiondate
