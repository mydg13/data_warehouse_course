WITH fact_sales_order__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order__rename AS (
  SELECT 
    order_line_id AS sales_order_line_key
    , order_id AS sales_order_key
    , stock_item_id AS product_key
    , quantity
    , unit_price
  FROM fact_sales_order__source
)

, fact_sales_order__cast_type AS (
  SELECT 
    CAST(sales_order_line_key AS INTEGER) AS sales_order_line_key
    , CAST(sales_order_key AS INTEGER) AS sales_order_key
    , CAST(product_key AS INTEGER) AS product_key
    , CAST(quantity AS INTEGER) AS quantity
    , CAST(unit_price AS NUMERIC) AS unit_price
  FROM fact_sales_order__rename
)

SELECT 
  fact_line.sales_order_line_key
  , fact_line.sales_order_key
  , fact_line.product_key
  , fact_header.customer_key
  , fact_line.quantity
  , fact_line.unit_price
  , fact_line.quantity * fact_line.unit_price AS gross_amount
FROM fact_sales_order__cast_type AS fact_line 
JOIN {{ref('stg_fact_sale_order')}} AS fact_header
  ON fact_line.sales_order_key = fact_header.sales_order_key
