WITH orders AS (
    SELECT * FROM {{ source('silver', 'orders') }}
),

order_items AS (
    SELECT * FROM {{ source('silver', 'order_items') }}
),

payments AS (
    SELECT
        order_id,
        SUM(payment_value)        AS total_payment_value,
        MAX(payment_installments) AS payment_installments,
        MAX(payment_type)         AS payment_type
    FROM {{ source('silver', 'order_payments') }}
    GROUP BY order_id
),

reviews AS (
    SELECT
        order_id,
        AVG(review_score)         AS avg_review_score
    FROM {{ source('silver', 'order_reviews') }}
    GROUP BY order_id
)

SELECT
    o.order_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    DATE(o.order_purchase_timestamp)  AS date_id,
    o.order_status,
    oi.price,
    oi.freight_value,
    p.total_payment_value,
    p.payment_type,
    p.payment_installments,
    r.avg_review_score,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date
FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
LEFT JOIN payments p     ON o.order_id = p.order_id
LEFT JOIN reviews r      ON o.order_id = r.order_id