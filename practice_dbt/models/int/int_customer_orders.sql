select 
    customers.customer_id,
    min(order_placed_at) as first_order_date,
    max(order_placed_at) as most_recent_order_date,
    count(order_id) AS number_of_orders
from {{ ref('stg_jaffle_shop__customers') }} customers
left join  {{ ref('stg_jaffle_shop__orders') }} as orders
    on orders.customer_id = customers.customer_id
group by 1