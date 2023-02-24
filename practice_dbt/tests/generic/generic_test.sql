{% test is_true(model, column_name, new_arg) %}

select *
from {{ model }}
where {{ column_name }} is {{ new_arg }}


{% endtest %}



