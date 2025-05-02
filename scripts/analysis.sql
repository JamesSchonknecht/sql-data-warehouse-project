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
SELECT TOP 20 * FROM gold.fact_sales;
SELECT TOP 20 * FROM gold.dim_products;

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

