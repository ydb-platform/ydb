USE plato;
pragma DisableSimpleColumns;

select
c1.customer_id, c2.customer_id
from
plato.customers1 as c1
join
plato.customers1 as c2
on c1.country_id = c2.country_id order by c1.customer_id, c2.customer_id;
