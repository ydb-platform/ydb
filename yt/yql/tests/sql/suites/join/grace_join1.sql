USE plato;
pragma DisableSimpleColumns;

select
cust.customer_id, cntr.country_name
from
plato.countries1 as cntr
join
plato.customers1 as cust
on cntr.country_id = cust.country_id
where cntr.country_id = "11";
