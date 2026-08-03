{% snapshot snap_service_requests %}

{{
    config(
        unique_key='sr_number',
        strategy='timestamp',
        updated_at='last_modified_at',
    )
}}

-- SCD-2 history of each request. Because status and closed_at mutate in
-- place on a stable sr_number, the timestamp strategy on last_modified_at
-- captures a new version each time the city updates a request.
select
    sr_number,
    sr_type,
    status,
    owner_department,
    created_at,
    closed_at,
    last_modified_at
from {{ ref('stg_chicago_311__service_requests') }}

{% endsnapshot %}
