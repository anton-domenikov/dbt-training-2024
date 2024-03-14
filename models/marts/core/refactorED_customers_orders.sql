with

    paid_orders as (select * from {{ ref("int_paid_orders_refactoring") }}),

    customer_orders as (select * from {{ ref("int_customer_orders_refactoring") }}),


    final as (
        select
            paid_orders.*,

            -- sales transaction sequence
            row_number() over (order by paid_orders.order_id) as transaction_seq,
            
            -- customer sales sequence
            row_number() over (
                partition by customer_id order by paid_orders.order_id
            ) as customer_sales_seq,

            -- new vs returning customer
            case
                when customer_orders.first_order_date = paid_orders.order_placed_at
                then 'new'
                else 'return'
            end as nvsr,

            -- customer lifetime value
            sum(total_amount_paid) over (
                partition by paid_orders.customer_id
                order by paid_orders.order_placed_at
            ) as customer_lifetime_value,

            -- first day of sale
            customer_orders.first_order_date as fdos
        from paid_orders
        left join customer_orders using (customer_id)
        order by order_id
    )

select *
from final
