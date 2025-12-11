{{ config(materialized='table') }}

with source as (
    select  transaction_id,
        title,
        publish_time,
        url,
        `地块编号` as land_number,
        `土地使用权竞得人` as land_owner,
        `土地位置` as land_location,
        double(`土地面积_公顷`) as land_area_hectare,
        double(regexp_extract(`土地面积_sqm`, '([0-9.]+)', 1)) as land_area_sqm, 
        `土地用途` as land_use,
        case when regexp_extract(`建筑面积_sqm_容积率`, '([0-9.]+)', 1) = "" then null else regexp_extract(`建筑面积_sqm_容积率`, '([0-9.]+)', 1) end as building_area_sqm_1,
        case when regexp_extract(nvl(`建筑面积_sqm`,`建筑面积`), '([0-9.]+)', 1) = "" then null else regexp_extract(nvl(`建筑面积_sqm`,`建筑面积`), '([0-9.]+)', 1) end as building_area_sqm_2,
        `容积率` as land_plot_ratio_desc,
        `出让年限` as lease_years,
        `出让方式` as lease_type,
        double(nvl(`起始价_万元`, `起始价_万元_1`)) * 10000 as lease_price,
        double(`成交价_万元` * 10000) as lease_price_paid,
        to_date(`成交时间`,'yyyy.M.d') as lease_date
 from land_market_dev.20_datastore.transaction_detail where `地块编号` <> ""
),

land_area_refined as (
    select transaction_id, title, publish_time, url, land_number, land_owner, land_location,
      nvl(land_area_hectare, land_area_sqm / 10000) as land_area_hectare,
      nvl(land_area_sqm, land_area_hectare * 10000) as land_area_sqm,
      land_use,
      double(nvl(building_area_sqm_1, building_area_sqm_2)) as building_area_sqm,
      land_plot_ratio_desc,
      lease_type,
      lease_price,
      lease_price_paid,
      lease_date
  from source)

select * from land_area_refined