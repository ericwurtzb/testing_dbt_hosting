select 
    orderid as order_id, 
    created as payment_date,
    amount as payment_amount,
    status as payment_status
from dbt_dataset.payments
