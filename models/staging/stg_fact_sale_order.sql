WITH fact_sales_order__source AS (
    SELECT * 
    FROM `vit-lam-data.wide_world_importers.sales__orders`)

, fact_sales_order__rename AS (
  SELECT 
    order_id AS sales_order_key
    , customer_id AS  customer_key
  FROM fact_sales_order__source
)

, fact_sales_order__cast_type AS (
  SELECT 
    CAST(sales_order_key AS INTEGER) AS sales_order_key
    , CAST(customer_key AS INTEGER) AS customer_key
  FROM fact_sales_order__rename
)

SELECT 
  sales_order_key
  , customer_key
FROM fact_sales_order__cast_type