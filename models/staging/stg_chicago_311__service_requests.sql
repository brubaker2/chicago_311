with source as (

    select * from {{ source('chicago_311', 'service_requests') }}

),

renamed as (

    select
        -- ids
        sr_number,
        parent_sr_number,

        -- request classification
        sr_type,
        sr_short_code,
        owner_department,
        origin,
        status,

        -- flags
        cast(duplicate as boolean)      as is_duplicate,
        cast(legacy_record as boolean)  as is_legacy_record,

        -- timestamps
        cast(created_date as timestamp)        as created_at,
        cast(last_modified_date as timestamp)  as last_modified_at,
        cast(closed_date as timestamp)         as closed_at,

        -- location
        street_address,
        zip_code,
        community_area,
        ward,
        latitude,
        longitude

    from source

)

select * from renamed
