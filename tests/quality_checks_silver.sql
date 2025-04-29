/*
==========================================================================
Silver Schema Quality Checks
==========================================================================

Script Purpose:
	This script performs various quality checks for data consistency, accuracy, and standardisation across the
	'silver' schema. It includes checks for:
	- Null or duplicate primary keys
	- Unwanted spaces in string fields
	- Data standardisation and consistency
	- Invalid date ranges and orders
	- Data consistency between related fields across tables

Usage Notes:
	- Run these checks after loading silver layer
	- Investigate and resolve any discrepencies found by the checks
*/



/*
==========================================================================
Table: silver.crm_cust_info
==========================================================================
*/

-- Check for nulls and duplicates in primary key
-- Expectation: No result
SELECT cst_id
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- Check for unwanted spaces
-- Expectation: No results
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT * FROM silver.crm_cust_info;


/*
==========================================================================
Table: silver.crm_prd_info
==========================================================================
*/


SELECT * FROM silver.crm_prd_info;

-- Check for nulls and duplicates in primary key
-- Expectation: No result
SELECT prd_id
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No results
SELECT prd_line
FROM silver.crm_prd_info
WHERE prd_line != TRIM(prd_line);

-- Check for NULLs or negative numbers
-- Expectation: No results
SELECT *
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data standarisation & consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt
ORDER BY prd_id;

/*
==========================================================================
Table: silver.crm_sales_details 
==========================================================================
*/

-- Check for nulls and duplicates in primary key
-- Expectation: No result
SELECT
	sls_ord_num,
	sls_prd_key,
	COUNT(*)
FROM silver.crm_sales_details
GROUP BY sls_ord_num, sls_prd_key
HAVING COUNT(*) > 1 OR sls_ord_num IS NULL OR sls_prd_key IS NULL;

-- Check for unwanted spaces
-- Expectation: No result
SELECT sls_prd_key
FROM silver.crm_sales_details
WHERE sls_prd_key != TRIM(sls_prd_key);

-- Checking if prd_key can be used to join crm_sales_details to crm_prd_info
SELECT *
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- Checking if cust_id can be used to join crm_sales_details to crm_cust_info
SELECT *
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Check for invalid order dates
-- Negative numbers, value of 0, less than 19000101, greater than 20500101, length is not 8
SELECT
	NULLIF(sls_order_dt, 0)
FROM silver.crm_sales_details
WHERE
	sls_order_dt < CAST('1900-01-01' AS DATE)
	OR sls_order_dt > CAST('2050-01-01' AS DATE);

-- Check for invalid shipping dates
-- Negative numbers, value of 0, less than 19000101, greater than 20500101, length is not 8
SELECT
	NULLIF(sls_ship_dt, 0)
FROM silver.crm_sales_details
WHERE
	sls_ship_dt <= 0
	OR sls_ship_dt < 19000101
	OR sls_ship_dt > 20500101
	OR LEN(sls_ship_dt) != 8;

-- Check for invalid due dates
-- Negative numbers, value of 0, less than 19000101, greater than 20500101, length is not 8
SELECT
	NULLIF(sls_due_dt, 0)
FROM silver.crm_sales_details
WHERE
	sls_due_dt <= 0
	OR sls_due_dt < 19000101
	OR sls_due_dt > 20500101
	OR LEN(sls_due_dt) != 8;

-- Check for invalid date orders
SELECT
	*
FROM silver.crm_sales_details
WHERE
	sls_order_dt > sls_ship_dt
	OR sls_order_dt > sls_due_dt;

-- Check data consistency between sales, quantity, and price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE
	sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

/*
==========================================================================
Table: silver.crm_sales_details 
==========================================================================
*/

-- Check for nulls and duplicates in primary key
-- Expectation: No result
SELECT CID, COUNT(*)
FROM silver.erp_CUST_AZ12
GROUP BY CID
HAVING COUNT(*) > 1 OR CID IS NULL;

-- Check that silver.erp_CUST_AZ12 can be joined with silver.crm_cust_info
SELECT * FROM silver.crm_cust_info;
SELECT * FROM silver.erp_CUST_AZ12;

-- Identify out of range birthdates
SELECT DISTINCT BDATE
FROM silver.erp_CUST_AZ12
WHERE
	BDATE < '1924-01-01' OR BDATE > GETDATE();

-- Check unique values of gender
SELECT DISTINCT gen, LEN(gen) FROM silver.erp_CUST_AZ12;

/*
=================================================================
Table: silver.erp_LOC_A101
=================================================================
*/

SELECT * FROM silver.erp_LOC_A101;

-- Check for nulls and duplicates in primary key
-- Expectation: No result
SELECT
	CID,
	COUNT(*)
FROM silver.erp_LOC_A101
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
FROM silver.erp_LOC_A101;

-- Check CID from erp_LOC_A101 can be used to join with cst_key in crm_cust_info
SELECT
	REPLACE(CID, '-', '') AS CID
FROM silver.erp_LOC_A101;

SELECT cst_key FROM silver.crm_cust_info WHERE cst_key NOT IN (
SELECT
	REPLACE(CID, '-', '') AS CID
FROM silver.erp_LOC_A101
)

/*
=================================================================
Table: silver.erp_PX_CAT_G1V2
=================================================================
*/

-- Check for duplicates or NULLs in the primary key
SELECT
	ID
FROM silver.erp_PX_CAT_G1V2
GROUP BY ID
HAVING COUNT(*) > 1 OR ID IS NULL;


-- Check ID in silver.erp_PX_CAT_G1V2 can be used to join with prd_key in silver.crm_prd_info
SELECT
	ID
FROM silver.erp_PX_CAT_G1V2
WHERE ID NOT IN (SELECT cat_id FROM silver.crm_prd_info);


-- Check for unwanted spaces
SELECT CAT, SUBCAT, MAINTENANCE
FROM silver.erp_PX_CAT_G1V2
WHERE CAT != TRIM(CAT) OR SUBCAT != TRIM(SUBCAT) OR MAINTENANCE != TRIM(MAINTENANCE);

-- Check data standardisation & consistency
SELECT DISTINCT MAINTENANCE FROM silver.erp_PX_CAT_G1V2 ORDER BY MAINTENANCE;
