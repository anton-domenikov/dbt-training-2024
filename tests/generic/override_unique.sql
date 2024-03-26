{% test unique( model, column_name ) %}
select *
from (

   select
       {{ column_name }}

   from {{ model }}
   where {{ column_name }} != '11111' and {{ column_name }} !='00000'
   group by {{ column_name }}
   having count(*) > 1

) validation_errors
{% endtest %}