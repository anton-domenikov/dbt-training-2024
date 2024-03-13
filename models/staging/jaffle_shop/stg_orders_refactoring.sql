with orders as (
    select
        orders.id as order_id,
        orders.user_id as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
    from {{ source('jaffle_shop', 'orders') }}
)
select * from orders