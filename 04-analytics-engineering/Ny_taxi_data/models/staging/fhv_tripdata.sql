{{ config(materialized="table") }}

select
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pulocationid,
    dolocationid,
    sr_flag,
    affiliated_base_number
from {{ source('staging', 'external_fhv_tripdata') }}
where extract(year from pickup_datetime) = 2019

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %} limit 100 {% endif %}
