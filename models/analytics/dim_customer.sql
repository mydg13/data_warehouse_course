WITH dim_customer__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename AS (
  SELECT 
    customer_id AS  customer_key
    , customer_name AS customer_name
    , is_on_credit_hold AS is_on_credit_hold_boolean
    , customer_category_id as customer_category_key
    , buying_group_id AS buying_group_key
  FROM dim_customer__source
)

, dim_customer__cast_type AS (
  SELECT 
    CAST(customer_key AS INTEGER) AS customer_key
    , CAST(customer_name AS STRING) AS customer_name
    , CAST(is_on_credit_hold_boolean AS BOOLEAN) AS is_on_credit_hold_boolean
    , CAST(customer_category_key AS INTEGER) AS customer_category_key
    , CAST(buying_group_key AS INTEGER) AS buying_group_key
  FROM dim_customer__rename
)

, dim_customer__convert_boolean AS (
  SELECT *
  , CASE 
    WHEN is_on_credit_hold_boolean IS TRUE THEN 'On Creadit Hold'
    WHEN is_on_credit_hold_boolean IS FALSE THEN 'Not On Creadit Hold'
    WHEN is_on_credit_hold_boolean IS NULL THEN 'Undefined'
    ELSE 'Invalid'
    END AS is_on_credit_hold
  FROM dim_customer__cast_type
)

, dim_customer__add_undefined_record AS (
  SELECT 
    customer_key
    , customer_name
    , is_on_credit_hold
    , customer_category_key
    , buying_group_key
  FROM dim_customer__convert_boolean

  UNION ALL 
  
  SELECT 
    0 AS customer_key
    , "Undefined" AS customer_name
    , "Undefined" AS is_on_credit_hold
    , 0 AS customer_category_key
    , 0 AS buying_group_key

    UNION ALL 
  
  SELECT 
    -1 AS customer_key
    , "Invalid" AS customer_name
    , "Invalid" AS is_on_credit_hold
    , -1 AS customer_category_key
    , -1 AS buying_group_key
)

SELECT 
  dim_customer.customer_key
  , dim_customer.customer_name
  , dim_customer.is_on_credit_hold
  , dim_customer.customer_category_key
  , COALESCE(dim_customer_category.customer_category_name,'Invalid') AS customer_category_name --data raw cho phép null
  , dim_customer.buying_group_key
  , COALESCE(dim_buying_group.buying_group_name,'Invalid') AS buying_group_name --data raw cho phép null
FROM dim_customer__add_undefined_record dim_customer
LEFT JOIN {{ref('stg_dim_buying_group')}} as dim_buying_group
ON dim_customer.buying_group_key = dim_buying_group.buying_group_key
LEFT JOIN {{ref('stg_dim_customer_category')}} as dim_customer_category
ON dim_customer.customer_category_key = dim_customer_category.customer_category_key