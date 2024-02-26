{{ config(materializaed="table") }}

-- How to set `model_schema`
-- 1. Copy the schema from BigQuery (copy as JSON)
-- 2. Change the column data `type`
-- 3. Add `rename` if you need to rename the column name
-- 4. Add the CTE step `change_data_type`
{% if execute %}
    {% set model_schema = [
        {
            "name": "dispatching_base_num",
            "mode": "",
            "type": "STRING",
            "description": null,
            "fields": [],
        },
        {
            "name": "pickup_datetime",
            "mode": "",
            "type": "TIMESTAMP",
            "description": null,
            "fields": [],
        },
        {
            "name": "dropOff_datetime",
            "rename": "dropoff_datetime",
            "mode": "",
            "type": "TIMESTAMP",
            "description": null,
            "fields": [],
        },

        {
            "name": "PUlocationID",
            "rename": "pickup_location_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
            "description": null,
            "fields": []
        },
        {
            "name": "DOlocationID",
            "rename": "dropoff_location_id",
            "mode": "NULLABLE",
            "type": "INTEGER",
            "description": null,
            "fields": []
        },

          {
            "name": "SR_Flag",
            "mode": "NULLABLE",
            "type": "INTEGER",
            "description": null,
            "fields": []
        },

        {
            "name": "Affiliated_base_number",
            "rename": "affiliated_base_num",
            "mode": "",
            "type": "STRING",
            "description": null,
            "fields": [],
        },

    ] %}
{% endif %}

with
    base as (
        select {{ dbt_utils.star(from=source("staging", "external_fhv_tripdata")) }}
        from {{ source('staging', 'external_fhv_tripdata') }}
        {% if var("is_test_run", default=false) %}
            -- To run in PRD `dbt build -m stg_fhv_tripdata --vars '{'is_test_run':'false'}'`
            -- limit 100
        {% endif %}
    ),

    change_data_type as (
        select
            {% for val in model_schema -%}
                cast(
                    `{{ val["name"] }}` as {{ val["type"] }}
                ) as `{{ val.get('rename', val['name']) }}`
                {%- if not loop.last %},{% endif %}
            {% endfor %}
        from base
    ),

    filter_pickup_date_2019 as (
        select *
        from change_data_type
        where EXTRACT(YEAR FROM pickup_datetime) = 2019
    ),
    
    final as (select * from filter_pickup_date_2019)

select *
from final