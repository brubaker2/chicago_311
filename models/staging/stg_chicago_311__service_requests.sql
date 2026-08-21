with source as (
    select * from {{ source('chicago_311', 'service_requests') }}
),

renamed as (
    select
        -- ids
        sr_number,
        parent_sr_number,
        legacy_sr_number,

        -- request classification
        sr_type,
        sr_short_code,
        owner_department,
        created_department,
        origin,
        status,

        -- flags
        cast(duplicate as boolean)      as is_duplicate,
        cast(legacy_record as boolean)  as is_legacy_record,

        -- timestamps
        cast(created_date as timestamp)        as created_at,
        cast(last_modified_date as timestamp)  as last_modified_at,
        cast(closed_date as timestamp)         as closed_at,

        -- location: address
        street_address,
        zip_code,
        city,
        state,

        -- location: administrative geography
        community_area,
        ward,
        precinct,

        -- location: police geography
        police_district,
        police_sector,
        police_beat,

        -- location: utility geography
        electrical_district,
        electricity_grid,
        sanitation_division_days,

        -- location: coordinates
        latitude,
        longitude

    from source
)

select * from renamed
qualify row_number() over (
    partition by sr_number
    order by last_modified_at desc
) = 1