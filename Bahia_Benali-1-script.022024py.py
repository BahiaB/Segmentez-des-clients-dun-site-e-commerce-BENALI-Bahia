import mysql.connector

# Connexion à la base de données
conn = mysql.connector.connect(
  host="localhost",
  port=3306,
  user="root",
  database="olist"
)

cursor = conn.cursor()

# Requete 1
query1 = """
WITH latest_order AS (
    SELECT MAX(order_delivered_customer_date) - INTERVAL 3 month AS last3month 
    FROM orders
)
SELECT *, DATEDIFF(order_delivered_customer_date , order_estimated_delivery_date) AS delta
FROM orders
WHERE order_purchase_timestamp >= (SELECT * FROM latest_order) 
AND DATEDIFF(order_delivered_customer_date , order_estimated_delivery_date) >= 3
AND order_status = 'delivered';
"""
cursor.execute(query1)
result1 = cursor.fetchall()
print("Resultat de la requete 1: \n",result1,'\n')


# Deuxième requête SQL
query2 = """WITH orders_joined as (
	SELECT DISTINCT  oi.seller_id,
	oi.order_id,
	oi.price,
	orders.order_purchase_timestamp 
	FROM order_items AS oi
	LEFT JOIN orders
	ON orders.order_id = oi.order_id
	WHERE orders.order_status = 'delivered'
	ORDER BY seller_id, order_purchase_timestamp
),
total_sales as (
SELECT seller_id,  ROUND(SUM(price), 1) as total_amount_sold,
count(order_id) as total_items
FROM orders_joined
GROUP BY seller_id
)
SELECT * from total_sales
WHERE total_amount_sold > 100000"""
cursor.execute(query2)
result2 = cursor.fetchall()
print("Resultat de la requete 2: \n",result2, '\n')

# Troisième requête SQL

query3 = """WITH latest_order AS (
    SELECT MAX(order_delivered_customer_date) AS max_purchase_timestamp
    FROM orders
),
orders_joined AS (
    SELECT DISTINCT
        i.seller_id, i.order_id, i.price,
        o.order_purchase_timestamp
    FROM
        order_items  i
    INNER JOIN
        orders  o ON o.order_id = i.order_id
    WHERE
        o.order_status = 'delivered'
    ORDER BY
        seller_id,
        order_purchase_timestamp
),
seller_agg AS (
    SELECT
        seller_id,
        SUM(price) AS total_amount_sold,
        COUNT(order_id) AS total_items_sold
    FROM
        orders_joined
    GROUP BY
        seller_id
    HAVING
        MIN(order_purchase_timestamp) > (SELECT DATE_SUB(max_purchase_timestamp, INTERVAL 3 MONTH)
        FROM latest_order)
)
SELECT * FROM seller_agg
WHERE total_items_sold > 30;"""
cursor.execute(query3)
result3 = cursor.fetchall()
print("Resultat de la requete 3 : \n",result3,'\n')

# Quatrième requête SQL

query4 = """WITH latest_order AS (
    SELECT MAX(order_purchase_timestamp) AS max_purchase_timestamp
    FROM orders
),
join_orders_geoloc AS (
    SELECT DISTINCT o.order_id, o.order_purchase_timestamp, c.customer_zip_code_prefix
    FROM orders AS o
    INNER JOIN customers AS c ON o.customer_id = c.customer_id
),
average_review_score_per_zip AS (
    SELECT
        customer_zip_code_prefix,
        AVG(review_score) AS avg_review_score,
        COUNT(review_score) AS nb_reviews
    FROM
        order_review AS r
    INNER JOIN
        join_orders_geoloc AS o ON r.order_id = o.order_id
    WHERE
        order_purchase_timestamp >= (SELECT max_purchase_timestamp FROM latest_order) - INTERVAL 12 MONTH
    GROUP BY
        customer_zip_code_prefix
)
SELECT *
FROM
    average_review_score_per_zip
WHERE
    nb_reviews > 30
ORDER BY
    avg_review_score ASC
LIMIT 5"""
cursor.execute(query4)
result4 = cursor.fetchall()
print("Resultat de la requete 4 : \n",result4, '\n')
# Fermeture de la connexion
conn.close()