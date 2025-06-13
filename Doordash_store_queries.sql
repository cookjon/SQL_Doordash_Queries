--CLEANING QUERIES  
/* 
-- Partition to find duplicate entries. Confirmed no de-duplication is needed on this dataset
	SELECT
	*,
	ROW_NUMBER() OVER (
		PARTITION BY
			market_id,
			created_at,
			actual_delivery_time,
			store_id
	) AS row_num
	FROM
		doordash_historical_data
	WHERE
		row_num > 1
	
-- Removing outlier of the only sale from prior year (2014-OCT). Could be worthwhile, but 
	DELETE FROM doordash_historical_data
	WHERE
		EXTRACT(
			YEAR
			FROM
				created_at
		) = '2014'
	
-- Determine if there are any misspellings/non-standardized categories that need can be fixed
	SELECT DISTINCT
	store_primary_category
	FROM
		doordash_historical_data
	ORDER BY
		store_primary_category ASC;

-- Fill in NULL market_ID with corresponding market_ID from same store_ID, but different entries 
	UPDATE DOORDASH_HISTORICAL_DATA AS dhd1
	SET
		market_id = dhd2.market_id
	FROM
		doordash_historical_data AS dhd2
	WHERE
		dhd1.store_id = dhd2.store_id
		AND dhd1.market_id IS NULL
		AND dhd2.market_id IS NOT NULL;
*/


--STORE
--What store had the most/least orders
SELECT
	store_id,
	COUNT(1) AS num_orders
FROM
	doordash_historical_data
GROUP BY
	store_id
ORDER BY
	num_orders DESC;

--What store had the largest/smallest order/avg by subtotal 
SELECT
	store_id,
	SUM(subtotal) AS total_sales,
	AVG(subtotal) AS avg_sales
FROM
	doordash_historical_data
GROUP BY
	store_id
ORDER BY
	total_sales DESC,
	avg_sales DESC;

--What store had the largest order/avg order size by number of items
SELECT
	store_id,
	SUM(total_items) AS total_items_sold,
	AVG(total_items) AS avg_items_sold
FROM
	doordash_historical_data
GROUP BY
	store_id
ORDER BY
	total_items_sold DESC,
	avg_items_sold DESC;

--What stores experienced the largest/smallest backlog of orders
SELECT
	store_id,
	SUM(total_outstanding_orders) AS sum_outstanding_orders,
	AVG(total_outstanding_orders) AS avg_outstanding_orders
FROM
	doordash_historical_data
WHERE
	total_outstanding_orders IS NOT NULL
GROUP BY
	store_id
ORDER BY
	sum_outstanding_orders DESC,
	avg_outstanding_orders DESC;

--What store had the fastest/slowest delivery/pickup times on avg
SELECT
	store_id,
	SUM(estimated_store_to_consumer_driving_duration) AS sum_delivery_time,
	AVG(estimated_store_to_consumer_driving_duration) AS avg_delivery_time,
	SUM(estimated_order_place_duration) AS sum_pickup_travel_time,
	AVG(estimated_order_place_duration) AS avg_pickup_time
FROM
	doordash_historical_data
WHERE
	estimated_store_to_consumer_driving_duration IS NOT NULL
GROUP BY
	store_id
ORDER BY
	avg_delivery_time DESC,
	sum_delivery_time DESC;


--What are the cheapest/most expensive things each store sells
WITH
	min_cte AS (
		SELECT
			store_id,
			MIN(min_item_price) AS min_priced_item
		FROM
			doordash_historical_data
		WHERE
			min_item_price > 0
		GROUP BY
			store_id
		ORDER BY
			store_id ASC,
			min_priced_item ASC
	)
SELECT
	dhd.store_id,
	min_cte.min_priced_item,
	MAX(max_item_price) AS max_priced_item
FROM
	doordash_historical_data dhd
	JOIN min_cte ON min_cte.store_id = dhd.store_id
GROUP BY
	dhd.store_id,
	min_cte.min_priced_item
ORDER BY
	store_id ASC,
	min_cte.min_priced_item ASC;

	
--Food
--What food category had the most/least orders
SELECT
	store_primary_category,
	COUNT(store_primary_category) AS num_orders
FROM
	doordash_historical_data
GROUP BY
	store_primary_category
ORDER BY
	num_ordersdesc;

--What food category had the largest/smallest orders/avg order size and backlogs (subtotal/items/backlog)
SELECT
	store_primary_category,
	SUM(subtotal) AS total_sales,
	SUM(total_items) AS num_items,
	SUM(total_outstanding_orders) AS total_backlog,
	AVG(subtotal) AS avg_sales,
	AVG(total_items) AS avg_order_size,
	AVG(total_outstanding_orders) AS avg_backlog
FROM
	doordash_historical_data
GROUP BY
	store_primary_category
ORDER BY
	total_sales DESC,
	num_items DESC;

--What food category had the fastest/slowest delivery times
SELECT
	store_primary_category,
	AVG(estimated_store_to_consumer_driving_duration) AS avg_delivery_time,
	SUM(estimated_store_to_consumer_driving_duration) AS total_delivery_time
FROM
	doordash_historical_data
GROUP BY
	store_primary_category
ORDER BY
	avg_delivery_time DESC,
	total_delivery_time;


--Time
--What time of day has the most/least orders
SELECT
	EXTRACT(
		HOUR
		FROM
			created_at
	) AS created_hour,
	SUM(total_items) AS num_items,
	SUM(subtotal) AS total_sales,
	AVG(total_items) AS avg_order_size,
	AVG(subtotal) AS avg_sales
FROM
	doordash_historical_data
GROUP BY
	created_hour
ORDER BY
	created_hour;


--What time of day has the most/least dashers (Dashers may be counted multiple times, since they may be present on multiple orders)
SELECT
	EXTRACT(
		HOUR
		FROM
			created_at
	) AS created_hour,
	SUM(total_onshift_dashers) AS total_dashers
FROM
	doordash_historical_data
GROUP BY
	created_hour
ORDER BY
	created_hour;
	
--When are the largest/smallest orders purchased (subtotal/items)
SELECT
	EXTRACT(
		dow --Day of week
		FROM
			created_at
	) AS dow_created,
	EXTRACT(
		HOUR
		FROM
			created_at
	) AS hour_created,
	COUNT(1) AS num_orders,
	SUM(subtotal) AS sum_sales
FROM
	doordash_historical_data
GROUP BY
	EXTRACT(
		dow
		FROM
			created_at
	),
	EXTRACT(
		HOUR
		FROM
			created_at
	)
ORDER BY
	dow_created,
	hour_created,
	sum_sales;

--When do deliveries take the longest/shortest
SELECT
	EXTRACT(
		dow --Day of week
		FROM
			created_at
	) AS dow_created,
	EXTRACT(
		HOUR
		FROM
			created_at
	) AS hour_created,
	SUM(estimated_store_to_consumer_driving_duration) AS total_delivery_time, 
	AVG(estimated_store_to_consumer_driving_duration) AS avg_delivery_time
FROM
	doordash_historical_data
GROUP BY
	EXTRACT(
		dow --Day of week
		FROM
			created_at
	),
	EXTRACT(
		HOUR
		FROM
			created_at
	)
ORDER BY
	dow_created,
	hour_created;

-- AVG time for restaurant to prepare order. (actual delivery time - driving time to deliver - order placed)
WITH ttp AS (
		SELECT
			EXTRACT(
				dow --Day of week
				FROM
					created_at
			) AS dow_created,
			EXTRACT(
				HOUR
				FROM
					created_at
			) AS hour_created,
			(actual_delivery_time - created_at) - (
				interval '1 second' * (
					estimated_order_place_duration + estimated_store_to_consumer_driving_duration
				)
			) AS time_to_prepare
		FROM
			doordash_historical_data
		GROUP BY
			EXTRACT(
				dow --Day of week
				FROM
					created_at
			),
			EXTRACT(
				HOUR
				FROM
					created_at
			),
			actual_delivery_time,
			created_at,
			estimated_order_place_duration,
			estimated_store_to_consumer_driving_duration
		ORDER BY
			dow_created,
			hour_created
	)
SELECT
	dow_created,
	hour_created,
	AVG(time_to_prepare)
FROM
	ttp
GROUP BY
	dow_created,
	hour_created
ORDER BY
	dow_created,
	hour_created;
	
--Compound questions
--What is the top restaurant in each food category by sales segmented by market and month.
 WITH
	store_sales AS (
		SELECT
			market_id,
			store_id,
			store_primary_category,
			TO_CHAR(created_at, 'Month') AS m_created_at,
			SUM(subtotal) AS total_sales
		FROM
			doordash_historical_data
		GROUP BY
			market_id,
			store_id,
			m_created_at,
			store_primary_category
		ORDER BY
			market_id,
			store_id,
			store_primary_category
	)
SELECT
	market_id,
	m_created_at,
	store_id,
	store_primary_category,
	RANK() OVER (
		PARTITION BY
			market_id,
			m_created_at,
			store_primary_category
		ORDER BY
			total_sales DESC
	) AS ranked_sales,
	total_sales
FROM
	store_sales
GROUP BY
	market_id,
	m_created_at,
	store_id,
	store_primary_category,
	total_sales
ORDER BY
	m_created_at DESC,
	market_id,
	store_primary_category,
	ranked_sales ASC
	;

-- When do orders take the longest to fulfill
-- -- Testing query
SELECT
 actual_delivery_time - created_at AS time_interval, order_protocol, EXTRACT(
				HOUR
				FROM
					created_at
			) AS hour_of_day, TOTAL_ONSHIFT_DASHERS, TOTAL_BUSY_DASHERS, *
FROM
	doordash_historical_data
	WHERE order_protocol = 5
	--EXTRACT(dow FROM created_at) = 3 AND 
	--EXTRACT(HOUR FROM created_at) >= 7
	ORDER BY time_interval DESC
	;


--When do orders take longest to fulfill
WITH
	ttp AS (
		SELECT
			--EXTRACT(dow FROM created_at) AS dow_created,
			TO_CHAR(created_at, 'Day') AS dow_created,
			--TO_CHAR(created_at, 'HH12 PM') AS hour_created,
			TO_CHAR((created_at - (Interval '8 hours')), 'HH12 PM') AS hour_created, --Interval used to correct for suspected timezone
			/*EXTRACT(HOUR FROM created_at)+1 AS hour_created, */
			(actual_delivery_time - created_at) AS time_to_fulfill
		FROM
			doordash_historical_data
		GROUP BY
			EXTRACT(
				dow --Day of week
				FROM
					created_at
			),
			EXTRACT(
				HOUR
				FROM
					created_at
			),
			actual_delivery_time,
			created_at,
			estimated_order_place_duration,
			estimated_store_to_consumer_driving_duration
		ORDER BY
			dow_created,
			hour_created
	),
	ttfi AS (
		SELECT
			dow_created,
			hour_created, ( EXTRACT(HOUR FROM time_to_fulfill)* 3600) + 
			(
				EXTRACT(
					MINUTE
					FROM
						time_to_fulfill
				) * 60
			) + (
				EXTRACT(
					SECOND
					FROM
						time_to_fulfill
				)
			) AS time_integer,
			AVG(time_to_fulfill) AS avg_time_to_fulfill
		FROM
			ttp
		GROUP BY
			dow_created,
			hour_created,
			time_to_fulfill
		ORDER BY
			dow_created,
			hour_created
	)
SELECT
	dow_created,
	hour_created,
	AVG(time_integer) AS avg_time_integer
FROM
	ttfi
GROUP BY
	dow_created,
	hour_created
ORDER BY
	dow_created,
	hour_created;
