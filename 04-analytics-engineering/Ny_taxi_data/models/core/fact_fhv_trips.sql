{{ config(materializaed="table") }}

with
    fhv_data as (
        select {{ dbt_utils.star(from=ref("fhv_tripdata")) }}
        from {{ ref("fhv_tripdata") }}
    ),
    trips_unioned as (
        select *
        from fhv_data
    ),
    dim_zones as (
        select {{ dbt_utils.star(from=ref("dim_zones")) }} 
        from {{ ref("dim_zones") }}
        where borough != 'Unknown'
    ),
    joined_location as (
        select
            trips_unioned.`pickup_location_id`,
            pickup_zone.borough as pickup_borough,
            pickup_zone.zone as pickup_zone,
            trips_unioned.`dropoff_location_id`,
            dropoff_zone.borough as dropoff_borough,
            dropoff_zone.zone as dropoff_zone,
            trips_unioned.`pickup_datetime`,
            trips_unioned.`dropoff_datetime`
        from trips_unioned
        inner join
            dim_zones as pickup_zone
            on trips_unioned.pickup_location_id = pickup_zone.locationid
        inner join
            dim_zones as dropoff_zone
            on trips_unioned.dropoff_location_id = dropoff_zone.locationid
    ),
    final as (
        select *
        from joined_location
    )
select *
from final