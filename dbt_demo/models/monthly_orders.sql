{{config(
    materialized='table'
)}}

{% set statuses = ['placed','shipped','completed','return_pending','returned' ] %}
with transformed_orders as (
    select
        order_id,
        date_trunc('month', order_date) as order_month,
        status
    from {{ ref('stg_orders') }}
),

final as(
    select
        order_month,
        {% for status in statuses %}
            count(
                case when status = '{{ status }}' 
                then order_id 
                end
            )as {{ status }}_count{% if not loop.last %},{% endif %}
        {% endfor %}
    from transformed_orders
    group by order_month
    order by order_month
)
select * from final