with

orders as (
    select * from {{ ref('stg_orders_refactoring') }}
),

customers as (
    select * from {{ ref('stg_customers_refactoring') }}
),

customer_orders as (
    select
        customers.customer_id as customer_id,
        min(orders.order_placed_at) as first_order_date,
        max(orders.order_placed_at) as most_recent_order_date,
        count(orders.order_id) as number_of_orders
    from customers
    left join orders on orders.customer_id = customers.customer_id
    group by 1
)

select * from customer_orders