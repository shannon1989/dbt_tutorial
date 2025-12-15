{{ config(
    materialized='table'
) }}

-- step 1: 标准化分隔符
with cleaned as (
    select
        land_uid, 
        regexp_replace(land_use, '；|、|:|：|，', ',') as cleaned_land_use
    from {{ref("fact_transaction_detail")}}
),

-- step 2：split + explode 拆分
exploded as (
    select
        land_uid,
        trim(keyword) as keyword
    from cleaned
    lateral view explode(split(cleaned_land_use, ',')) t as keyword
),

-- step 3：映射 land_use_mapping
mapped as (
    select
        e.land_uid,
        m.category_l1,
        m.category_l2
    from exploded e
    left join land_use_mapping m
        on e.keyword like concat('%', m.keyword, '%')
)

select land_uid, 
concat_ws('、', sort_array(collect_set(category_l1))) AS land_use_categories
 from mapped GROUP BY land_uid