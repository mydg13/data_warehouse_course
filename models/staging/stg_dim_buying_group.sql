WITH dim_buying_group__source AS (
    SELECT * 
    FROM `vit-lam-data.wide_world_importers.sales__buying_groups`)

, dim_buying_group__rename AS (
  SELECT 
    buying_group_id AS buying_group_key
    , buying_group_name
  FROM dim_buying_group__source
)

, dim_buying_group__cast_type AS (
  SELECT 
    CAST(buying_group_key AS INTEGER) AS buying_group_key
    , CAST(buying_group_name AS STRING) AS buying_group_name
  FROM dim_buying_group__rename
)

SELECT 
  buying_group_key
  , buying_group_name
FROM dim_buying_group__cast_type