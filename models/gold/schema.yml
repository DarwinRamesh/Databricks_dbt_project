version: 2

models:
  - name: fact_orders
    columns:
      - name: order_id
        tests:
          - not_null
  - name: dim_customers
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
  - name: dim_products
    columns:
      - name: product_id
        tests:
          - unique
          - not_null
  - name: dim_sellers
    columns:
      - name: seller_id
        tests:
          - unique
          - not_null
  - name: dim_dates
    columns:
      - name: date_id
        tests:
          - unique
          - not_null