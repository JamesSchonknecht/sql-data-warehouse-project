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
SELECT TOP 20 * FROM gold.fact_sales;
SELECT TOP 20 * FROM gold.dim_customers;

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