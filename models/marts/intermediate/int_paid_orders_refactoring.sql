with

orders as (
    select * from {{ ref('stg_orders_refactoring') }}
),

customers as (
    select * from {{ ref('stg_customers_refactoring') }}
),

payments as (
    select * from {{ ref('stg_payments_refactoring') }}
),

paid_orders as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        payments.total_amount_paid,
        payments.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name
    from orders
    left join payments using (order_id)
    left join customers using (customer_id)
)

select * from paid_orders