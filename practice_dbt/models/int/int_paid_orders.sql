with payments as (
    select 
        order_id,
        max(payment_date) as payment_finalized_date,
        sum(payment_amount) / 100.0 as total_amount_paid
    from {{ ref('stg_stripe__payments') }}
    where payment_status <> 'fail'
    group by 1
),
final as (
select 
    orders.order_id,
    orders.customer_id,
    orders.order_placed_at,
    orders.order_status,
    payments.total_amount_paid,
    payments.payment_finalized_date,
    customers.customer_first_name,
    customers.customer_last_name
from {{ ref('stg_jaffle_shop__orders') }} as orders
left join payments 
    on orders.order_id = payments.order_id
left join {{ ref('stg_jaffle_shop__customers') }} customers
    on orders.customer_id = customers.customer_id
)
select * from final
