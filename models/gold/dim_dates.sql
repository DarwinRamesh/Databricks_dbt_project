WITH dates AS (
    SELECT DISTINCT
        DATE(order_purchase_timestamp)                AS date_id,
        YEAR(order_purchase_timestamp)                AS year,
        MONTH(order_purchase_timestamp)               AS month,
        QUARTER(order_purchase_timestamp)             AS quarter,
        DAY(order_purchase_timestamp)                 AS day,
        DAYOFWEEK(order_purchase_timestamp)           AS day_of_week,
        DATE_FORMAT(order_purchase_timestamp, 'EEEE') AS day_name,
        DATE_FORMAT(order_purchase_timestamp, 'MMMM') AS month_name
    FROM {{ source('silver', 'orders') }}
    WHERE order_purchase_timestamp IS NOT NULL
)

SELECT * FROM dates