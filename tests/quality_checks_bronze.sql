/*
==========================================================================
Bronze Schema Quality Checks
==========================================================================

Script Purpose:
	This script performs various quality checks for data consistency, accuracy, and standardisation across the
	'bronze' schema. It includes checks for:
	- Null or duplicate primary keys
	- Unwanted spaces in string fields
	- Data standardisation and consistency
	- Invalid date ranges and orders
	- Data consistency between related fields across tables

Usage Notes:
	- Run these checks after loading bronze layer
	- Investigate and resolve any discrepencies found by the checks
*/


/*
==========================================================================
Table: bronze.crm_cust_info
==========================================================================
*/

-- Check for nulls and duplicates in primary key
-- Expectation: No result
SELECT cst_id
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

SELECT
	*
FROM (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
) sub WHERE flag_last = 1;

SELECT * FROM bronze.crm_cust_info;

-- Check for unwanted spaces
-- Expectation: No results
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Query for cleaned data
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname),
	TRIM(cst_lastname),
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		 ELSE 'n/a'
	END AS cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 ELSE 'n/a'
	END AS cst_gndr,
	cst_create_date
FROM (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
) sub WHERE flag_last = 1;

/*
==========================================================================
Table: bronze.crm_prd_info
==========================================================================
*/

SELECT * FROM bronze.crm_prd_info;

-- Check for nulls and duplicates in primary key
-- Expectation: No result
SELECT prd_id
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No results
SELECT prd_line
FROM bronze.crm_prd_info
WHERE prd_line != TRIM(prd_line);

-- Check for NULLs or negative numbers
-- Expectation: No results
SELECT *
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data standardisation & consistency
SELECT DISTINCT prd_line, LEN(prd_line) FROM bronze.crm_prd_info;


SELECT * FROM bronze.crm_prd_info;
-- Check for invalid date orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt
ORDER BY prd_id;

SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_start_dt,
	prd_end_dt,
	(LEAD(prd_start_dt, 1) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')
ORDER BY prd_key;

-- Query for cleaned data
SELECT
	prd_id,
	prd_nm,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST((LEAD(prd_start_dt, 1) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1) AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;


/*
==========================================================================
Table: bronze.crm_sales_details 
==========================================================================
*/

-- Check for nulls and duplicates in primary key
-- Expectation: No result
SELECT
	sls_ord_num,
	sls_prd_key,
	COUNT(*)
FROM bronze.crm_sales_details
GROUP BY sls_ord_num, sls_prd_key
HAVING COUNT(*) > 1 OR sls_ord_num IS NULL OR sls_prd_key IS NULL;

-- Check for unwanted spaces
-- Expectation: No result
SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key != TRIM(sls_prd_key);

-- Checking if prd_key can be used to join crm_sales_details to crm_prd_info
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- Checking if cust_id can be used to join crm_sales_details to crm_cust_info
SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Check for invalid order dates
-- Negative numbers, value of 0, less than 19000101, greater than 20500101, length is not 8
SELECT
	NULLIF(sls_order_dt, 0)
FROM bronze.crm_sales_details
WHERE
	sls_order_dt <= 0
	OR sls_order_dt < 19000101
	OR sls_order_dt > 20500101
	OR LEN(sls_order_dt) != 8;

-- Check for invalid shipping dates
-- Negative numbers, value of 0, less than 19000101, greater than 20500101, length is not 8
SELECT
	NULLIF(sls_ship_dt, 0)
FROM bronze.crm_sales_details
WHERE
	sls_ship_dt <= 0
	OR sls_ship_dt < 19000101
	OR sls_ship_dt > 20500101
	OR LEN(sls_ship_dt) != 8;

-- Check for invalid due dates
-- Negative numbers, value of 0, less than 19000101, greater than 20500101, length is not 8
SELECT
	NULLIF(sls_due_dt, 0)
FROM bronze.crm_sales_details
WHERE
	sls_due_dt <= 0
	OR sls_due_dt < 19000101
	OR sls_due_dt > 20500101
	OR LEN(sls_due_dt) != 8;

-- Check for invalid date orders
SELECT
	*
FROM bronze.crm_sales_details
WHERE
	sls_order_dt > sls_ship_dt
	OR sls_order_dt > sls_due_dt;

SELECT * FROM bronze.crm_sales_details; 
-- Check data consistency between sales, quantity, and price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative
SELECT DISTINCT
	sls_sales AS old_sls_sales,
	sls_quantity,
	sls_price AS old_sls_price,
	CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	CASE WHEN sls_price = 0 OR sls_price IS NULL
		 THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details
WHERE
	sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


-- Query for cleaned data
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price = 0 OR sls_price IS NULL
		 THEN sls_sales / NULLIF(sls_quantity, 0)
		 WHEN sls_price < 0
		 THEN ABS(sls_price)
		 ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details;

/*
==========================================================================
Table: bronze.erp_CUST_AZ12
==========================================================================
*/

-- Check for nulls and duplicates in primary key
-- Expectation: No result
SELECT CID, COUNT(*)
FROM bronze.erp_CUST_AZ12
GROUP BY CID
HAVING COUNT(*) > 1 OR CID IS NULL;

-- Check that bronze.erp_CUST_AZ12 can be joined with silver.crm_cust_info
SELECT * FROM silver.crm_cust_info;
SELECT * FROM bronze.erp_CUST_AZ12;

-- Identify out of range birthdates
SELECT DISTINCT BDATE
FROM bronze.erp_CUST_AZ12
WHERE
	BDATE < '1924-01-01' OR BDATE > GETDATE();


SELECT * FROM bronze.erp_CUST_AZ12;
SELECT DISTINCT gen, LEN(gen) FROM bronze.erp_CUST_AZ12;

-- Query for cleaned data
SELECT
	CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
			ELSE CID
	END AS CID,
	CASE WHEN BDATE > GETDATE() THEN NULL
		 ELSE BDATE
	END AS BDATE,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
FROM bronze.erp_CUST_AZ12;




/*
==========================================================================
Table: bronze.erp_LOC_A101
==========================================================================
*/

SELECT * FROM bronze.erp_LOC_A101;

-- Check for nulls and duplicates in primary key
-- Expectation: No result
SELECT
	CID,
	COUNT(*)
FROM bronze.erp_LOC_A101
GROUP BY CID
HAVING COUNT(*) > 1 OR CID IS NULL;

-- Checking for invalid countries
SELECT DISTINCT
	CNTRY,
	CASE WHEN UPPER(TRIM(CNTRY)) IN ('DE', 'Germany') THEN 'Germany'
		 WHEN UPPER(TRIM(CNTRY)) IN ('USA', 'United States', 'US') THEN 'United States'
		 WHEN UPPER(TRIM(CNTRY)) IN (NULL, '') THEN 'n/a'
		 ELSE CNTRY
	END AS CNTRY2
FROM bronze.erp_LOC_A101;

-- Check CID from erp_LOC_A101 can be used to join with cst_key in crm_cust_info
SELECT
	REPLACE(CID, '-', '') AS CID
FROM bronze.erp_LOC_A101;

SELECT cst_key FROM silver.crm_cust_info WHERE cst_key NOT IN (
SELECT
	REPLACE(CID, '-', '') AS CID
FROM bronze.erp_LOC_A101
)


-- Query for cleaned data
SELECT
	REPLACE(CID, '-', '') AS CID,
	CASE WHEN TRIM(CNTRY) IN ('DE', 'Germany') THEN 'Germany'
		 WHEN TRIM(CNTRY) IN ('USA', 'United States', 'US') THEN 'United States'
		 WHEN TRIM(CNTRY) = '' OR TRIM(CNTRY) IS NULL THEN 'n/a'
		 ELSE CNTRY
	END AS CNTRY
FROM bronze.erp_LOC_A101;


/*
==========================================================================
Table: bronze.erp_PX_CAT_G1V2
==========================================================================
*/

-- Check for duplicates or NULLs in the primary key
SELECT
	ID
FROM bronze.erp_PX_CAT_G1V2
GROUP BY ID
HAVING COUNT(*) > 1 OR ID IS NULL;

SELECT * FROM bronze.erp_PX_CAT_G1V2;
SELECT * FROM silver.crm_prd_info;

-- Check ID in bronze.erp_PX_CAT_G1V2 can be used to join with prd_key in silver.crm_prd_info
SELECT
	ID
FROM bronze.erp_PX_CAT_G1V2
WHERE ID NOT IN (SELECT cat_id FROM silver.crm_prd_info);


-- Check for unwanted spaces
SELECT CAT, SUBCAT, MAINTENANCE
FROM bronze.erp_PX_CAT_G1V2
WHERE CAT != TRIM(CAT) OR SUBCAT != TRIM(SUBCAT) OR MAINTENANCE != TRIM(MAINTENANCE);

-- Check data standardisation & consistency
SELECT DISTINCT MAINTENANCE FROM bronze.erp_PX_CAT_G1V2 ORDER BY MAINTENANCE;
