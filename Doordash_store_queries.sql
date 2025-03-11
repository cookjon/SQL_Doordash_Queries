--What store had the most/least orders
SELECT store_id, COUNT(1) AS num_orders
FROM doordash_historical_data
GROUP BY store_id
ORDER BY num_orders DESC;

--What store had the largest/smallest order/avg by subtotal 
SELECT store_id, SUM(subtotal) AS total_sales, AVG(subtotal) AS avg_sales
FROM doordash_historical_data
GROUP BY store_id
ORDER BY total_sales DESC, avg_sales DESC;

--What store had the largest order/avg order size by number of items
SELECT store_id, SUM(total_items) AS total_items_sold, AVG(total_items) AS avg_items_sold
FROM doordash_historical_data
GROUP BY store_id
ORDER BY total_items_sold DESC, avg_items_sold DESC;

--What stores experienced the largest/smallest backlog of orders
SELECT store_id, SUM(total_outstanding_orders) AS sum_outstanding_orders, AVG(total_outstanding_orders) AS avg_outstanding_orders
FROM doordash_historical_data
WHERE total_outstanding_orders IS NOT NULL AND total_outstanding_orders >= 0
GROUP BY store_id
ORDER BY sum_outstanding_orders DESC, avg_outstanding_orders DESC;

--What store had the fastest/slowest delivery/pickup times
SELECT store_id, SUM(estimated_store_to_consumer_driving_duration) AS sum_delivery_time, AVG(estimated_store_to_consumer_driving_duration) AS avg_delivery_time, SUM(estimated_order_place_duration) AS sum_pickup_travel_time, AVG(estimated_order_place_duration) AS avg_pickup_time
FROM doordash_historical_data
WHERE estimated_store_to_consumer_driving_duration IS NOT NULL
GROUP BY store_id
ORDER BY sum_delivery_time DESC, avg_delivery_time DESC;

--What are the cheapest/most expensive things each store sells
WITH CTE AS (
	SELECT 
		store_id, 
		MIN(min_item_price) AS min_price, 
		MAX(max_item_price) AS max_price 
	FROM doordash_historical_data 
	WHERE min_item_price > 0 
	GROUP BY store_id)
SELECT store_id, min_price, max_price
FROM CTE
GROUP BY store_id, min_price, max_price
ORDER BY min_price ASC, store_id 

