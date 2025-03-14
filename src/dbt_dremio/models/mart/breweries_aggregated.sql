with source as (
    select 
        {{ cast_to_text('brewery_type') }} as brewery_type,
        {{ cast_to_text('state') }} as state,
        cast(total_breweries as INTEGER) as total_breweries  
    from GCS."jvq-test".gold."breweries_aggregated.parquet"
)
select * from source