with

paid_orders as (
    select * from {{ ref('int_paid_orders_refactoring') }}
),

customer_orders as (
    select * from {{ ref('int_customer_orders_refactoring') }}
),

-- final CTE
final as (

    select
        p.*,
        row_number() over (order by p.order_id) as transaction_seq,
        row_number() over (
            partition by customer_id order by p.order_id
        ) as customer_sales_seq,
        case
            when c.first_order_date = p.order_placed_at then 'new' else 'return'
        end as nvsr,
        x.clv_bad as customer_lifetime_value,
        c.first_order_date as fdos
    from paid_orders p
    left join customer_orders as c using (customer_id)
    left outer join
        (
            select p.order_id, sum(t2.total_amount_paid) as clv_bad
            from paid_orders p
            left join
                paid_orders t2
                on p.customer_id = t2.customer_id
                and p.order_id >= t2.order_id
            group by 1
            order by p.order_id
        ) x
        on x.order_id = p.order_id
    order by order_id
)

-- simple select statement
select * from final