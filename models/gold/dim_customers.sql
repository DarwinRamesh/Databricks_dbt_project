WITH customers AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_city,
        customer_state,
        customer_zip_code_prefix
    FROM {{ source('silver', 'customers') }}
)

SELECT * FROM customers