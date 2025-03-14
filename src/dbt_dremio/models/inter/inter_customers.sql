with source as (
select 
    cast(brewery_type as text) as brewery_type,
    cast(state as text) as state,
    cast(total_breweries as int ) as total_breweries

from GCS."jvq-test".gold."breweries_aggregated.parquet"
)
select * from source