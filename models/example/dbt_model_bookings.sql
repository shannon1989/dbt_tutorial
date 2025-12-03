{{ config(materialized='table') }}

select *
from workspace.silver.silver_bookings