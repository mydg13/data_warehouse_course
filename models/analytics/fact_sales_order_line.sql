WITH fact_sales_order__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order__rename AS (
  SELECT 
    order_line_id AS  sales_order_line_key
    , stock_item_id AS product_key
    , quantity
    , unit_price
  FROM fact_sales_order__source
)

, fact_sales_order__cast_type AS (
  SELECT 
    CAST(sales_order_line_key AS INTEGER) AS sales_order_line_key
    , CAST(product_key AS INTEGER) AS product_key
    , CAST(quantity AS INTEGER) AS quantity
    , CAST(unit_price AS NUMERIC) AS unit_price
  FROM fact_sales_order__rename
)

SELECT 
  sales_order_line_key
  , product_key
  , quantity
  , unit_price
  , quantity * unit_price gross_amount
FROM fact_sales_order__cast_type

