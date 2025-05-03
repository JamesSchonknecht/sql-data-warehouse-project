USE DataWarehouse;

/*
======================================================================
Changes Over Time Analysis
======================================================================
*/

SELECT
	YEAR(order_date) AS order_year,
	MONTH(order_date) AS order_month,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);


/*
======================================================================
Cumulative Analysis
======================================================================
*/

-- Calculating the total sales per month, running total of sales over time, and moving average price

SELECT
	order_year,
	order_month,
	total_sales,
	SUM(total_sales) OVER(PARTITION BY order_year ORDER BY order_year, order_month) AS running_total_sales,
	average_price,
	AVG(average_price) OVER(PARTITION BY order_year ORDER BY order_year, order_month) AS moving_average_price
FROM(
	SELECT
		YEAR(order_date) AS order_year,
		MONTH(order_date) AS order_month,
		SUM(sales_amount) AS total_sales,
		AVG(price) AS average_price
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY YEAR(order_date), MONTH(order_date)
) t;

/*
======================================================================
Performance Analysis
======================================================================
*/

-- Comparing yearly performance of products to average sales performance and previous year's performance
WITH yearly_product_sales AS (
	SELECT
		YEAR(s.order_date) AS order_year,
		p.product_name,
		SUM(s.sales_amount) AS current_sales
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p
	ON s.product_key = p.product_key
	WHERE s.order_date IS NOT NULL
	GROUP BY YEAR(s.order_date), p.product_name
)
SELECT
	order_year,
	product_name,
	current_sales,
	AVG(current_sales) OVER(PARTITION BY product_name),
	current_sales - AVG(current_sales) OVER(PARTITION BY product_name) AS diff_avg,
	CASE WHEN (current_sales - AVG(current_sales) OVER(PARTITION BY product_name)) > 0 THEN 'Above Avg'
		 WHEN (current_sales - AVG(current_sales) OVER(PARTITION BY product_name)) < 0 THEN 'Below Avg'
		 ELSE 'Avg'
	END AS avg_change,
	-- Year-over-year Analysis
	LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS prev_year_sales,
	current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS prev_year_diff,
	CASE WHEN (current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year)) > 0 THEN 'Increase'
		 WHEN (current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year)) < 0 THEN 'Decrease'
		 ELSE 'No Change'
	END AS year_change
FROM yearly_product_sales
ORDER BY product_name, order_year

/*
======================================================================
Part-to-Whole Analysis
======================================================================
*/

-- Finding which categories contribute most to overall sales
WITH category_cte AS(
	SELECT
		p.category,
		SUM(s.sales_amount) AS category_sales
	FROM gold.dim_products p
	LEFT JOIN gold.fact_sales s
	ON p.product_key = s.product_key
	WHERE p.category IS NOT NULL
	GROUP BY p.category
)
SELECT
	category,
	category_sales,
	SUM(category_sales) OVER() AS total_sales,
	CONCAT(ROUND(100 * CAST(category_sales AS FLOAT) / (SUM(category_sales) OVER()), 2), '%') AS percent_of_total
FROM category_cte
ORDER BY category_sales DESC;

/*
======================================================================
Data Segmentation
======================================================================
*/

-- Classifying products into three groups based on their cost
SELECT
	cost_range,
	COUNT(*) AS number_of_products
FROM (
	SELECT
		product_key,
		product_name,
		cost,
		CASE WHEN cost < 100 THEN 'Below 100'
			 WHEN cost BETWEEN 100 AND 499 THEN '100-499'
			 WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
			 ELSE 'Above 1000'
		END AS cost_range
	FROM gold.dim_products
)t
GROUP BY cost_range
ORDER BY number_of_products DESC;

-- Grouping customers into three groups based on their spending
	-- VIP: Customers with at least 12 months of history, who have spent more than 5,000
	-- Regular: Customers with at least 12 months of history but spending 5,000 or less
	-- New: Customers with a lifespan less than 12 months
-- Also finding total number of customers in each group

WITH customer_spending AS(
	SELECT
		c.customer_key,
		MIN(s.order_date) AS first_order_date,
		MAX(s.order_date) AS last_order_date,
		DATEDIFF(MONTH, MIN(s.order_date), MAX(s.order_date)) AS lifespan,
		SUM(s.price) AS total_spent
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_customers c
	ON s.customer_key = c.customer_key
	GROUP BY c.customer_key
)
SELECT
	customer_group,
	COUNT(customer_key) AS total
FROM (
	SELECT
		customer_key,
		CASE WHEN lifespan >= 12 AND total_spent > 5000 THEN 'VIP'
			 WHEN lifespan >=12 AND total_spent <= 5000 THEN 'Regular'
			 ELSE 'New'
		END AS customer_group
	FROM customer_spending
)t
GROUP BY customer_group
ORDER BY total DESC;

/*
======================================================================
Customer Report
======================================================================
Purpose:
	- This report consolidates key customer metrics and behaviours

Highlights:
	1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups
	3. Aggregates customer-level metrics:
		- total orders
		- total sales
		- total quantity purchased
		- total products
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last order)
		- average order value
		- average monthly spend
======================================================================
*/


CREATE VIEW	gold.report_customers AS
WITH base_query AS (
	/*
	----------------------------------------------------------------------
	Base Query: Retrieves core columns from tables
	----------------------------------------------------------------------
	*/
	SELECT
		s.order_number,
		s.product_key,
		s.order_date,
		s.sales_amount,
		s.quantity,
		c.customer_key,
		c.customer_number,
		CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
		DATEDIFF(YEAR, c.birth_date, GETDATE()) AS age
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_customers c
	ON s.customer_key = c.customer_key
	LEFT JOIN gold.dim_products p 
	ON s.product_key = p.product_key
	WHERE order_date IS NOT NULL
), customer_aggregations AS (
	/*
	----------------------------------------------------------------------
	Customer Aggregations: Summarises key metrics at the customer level
	----------------------------------------------------------------------
	*/
	SELECT
		customer_key,
		customer_number,
		customer_name,
		age,
		COUNT(DISTINCT order_number) AS total_orders,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity,
		COUNT(DISTINCT product_key) AS total_products,
		MAX(order_date) AS last_order_date,
		DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
	FROM base_query
	GROUP BY
		customer_key,
		customer_number,
		customer_name,
		age
)

SELECT
	customer_key,
	customer_number,
	customer_name,
	age,
	CASE WHEN age < 20 THEN 'Under 20'
		 WHEN age BETWEEN 20 AND 29 THEN '20-29'
		 WHEN age BETWEEN 30 AND 39 THEN '30-39'
		 WHEN age BETWEEN 40 AND 49 THEN '40-49'
		 ELSE 'Over 50'
	END AS age_group,
	CASE WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
			WHEN lifespan >=12 AND total_sales <= 5000 THEN 'Regular'
			ELSE 'New'
	END AS customer_group,
	total_orders,
	total_sales,
	total_quantity,
	total_products,
	last_order_date,
	DATEDIFF(MONTH, last_order_date, GETDATE()) AS recency,
	lifespan,
	-- Compute average order value (AOV)
	CASE WHEN total_orders = 0 THEN 0
		 ELSE total_sales / total_orders
	END AS avg_order_value,
	-- Compute average monthly spend
	CASE WHEN lifespan = 0 THEN total_sales
		 ELSE total_sales / lifespan
	END AS avg_monthly_spend
FROM customer_aggregations;

/*
======================================================================
Product Report
======================================================================
Purpose:
	- This report consolidates key product metrics and behaviours

Highlights:
	1. Gathers essential fields such as product name, category, subcategory, and cost.
	2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
	3. Aggregates product-level metrics:
		- total orders
		- total sales
		- total quantity sold
		- total products (unique)
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last sale)
		- average order revenue (AOR)
		- average monthly revenue
======================================================================
*/

CREATE VIEW gold.report_products AS
WITH base_query AS (
	/*
	----------------------------------------------------------------------
	Base Query: Retrieves core columns from tables
	----------------------------------------------------------------------
	*/
	SELECT
		s.order_number,
		s.order_date,
		s.customer_key,
		s.sales_amount,
		s.quantity,
		p.product_key,
		p.product_name,
		p.category,
		p.subcategory,
		p.cost
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p
	ON s.product_key = p.product_key
	WHERE s.order_date IS NOT NULL
), product_aggregations AS (
	/*
	----------------------------------------------------------------------
	Product Aggregations: Summarises key metrics at the product level
	----------------------------------------------------------------------
	*/
	SELECT
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
		MAX(order_date) AS last_sale_date,
		COUNT(DISTINCT order_number) AS total_orders,
		COUNT(DISTINCT customer_key) AS total_customers,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity,
		ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 1) AS avg_selling_price
	FROM base_query
	GROUP BY
		product_key,
		product_name,
		category,
		subcategory,
		cost
)

/*
----------------------------------------------------------------------
Final Query: Combines all products results into one output
----------------------------------------------------------------------
*/
SELECT
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_sale_date,
	DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_in_months,
	CASE WHEN total_sales > 50000 THEN 'High-Performer'
		 WHEN total_sales >= 10000 THEN 'Mid-Range'
		 ELSE 'Low-Performer'
	END AS product_segment,
	lifespan,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	-- Average Order Revenue (AOR)
	CASE WHEN total_orders = 0 THEN 0
		 ELSE total_sales / total_orders
	END AS total_order_revenue,
	-- Average Monthly Revenue
	CASE WHEN lifespan = 0 THEN total_sales
		 ELSE total_sales / lifespan
	END AS avg_monthly_revenue
FROM product_aggregations;