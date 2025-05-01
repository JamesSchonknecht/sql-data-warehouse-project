/*
=============================================================
DDL Script: Create Gold Views
=============================================================

Script Purpose:
This script creates views in the 'gold' schema.
The Gold layer represents the final fact and dimension tables (Star Schema).
	
Each view performs transformations and combines data from the Silver layer to produce a clean,
enriched, and business-ready dataset.

Usage:
	- These views can be queried directly for analytics and reporting.

=============================================================
*/

/*
-- ============================================================
-- Create Dimension: gold.dim_customers
-- ============================================================
*/

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.CNTRY AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' AND ci.cst_gndr IS NOT NULL THEN ci.cst_gndr
		 ELSE COALESCE(ca.GEN, 'n/a')
	END AS gender,
	ca.BDATE AS birth_date,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_CUST_AZ12 ca
ON ci.cst_key = ca.CID
LEFT JOIN silver.erp_LOC_A101 la
ON ci.cst_key = la.CID;

/*
-- ============================================================
-- Create Dimension: gold.dim_products
-- ============================================================
*/

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
	pi.prd_id AS product_id,
	pi.prd_key AS product_number,
	pi.prd_nm AS product_name,
	pi.cat_id AS category_id,
	pc.CAT AS category,
	pc.SUBCAT AS subcategory,
	pc.MAINTENANCE AS maintenance,
	pi.prd_cost AS cost,
	pi.prd_line AS product_line,
	pi.prd_start_dt AS start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_PX_CAT_G1V2 pc
ON pi.cat_id = pc.ID
WHERE prd_end_dt IS NULL -- Filter out all historical data
;

/*
-- ============================================================
-- Create Fact: gold.fact_sales
-- ============================================================
*/

CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id
