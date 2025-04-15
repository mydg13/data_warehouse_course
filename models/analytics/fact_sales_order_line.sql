SELECT 
  CAST (order_line_id AS integer) AS sales_order_line_key
  , CAST (stock_item_id AS integer) AS product_key
  , CAST (quantity AS integer) AS quantity
  , CAST (unit_price AS numeric ) AS unit_price 
  , CAST (quantity AS integer) *  CAST (unit_price AS numeric ) AS gross_amount
FROM `vit-lam-data.wide_world_importers.sales__order_lines`