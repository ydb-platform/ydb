--!syntax_v1 
select
    hashOrderDate, 
    orderDate,
    orderId as f1
from personal_storefront
where hashOrderDate = Digest::MurMurHash('2023-03-01') 
    and orderDate = '2023-03-01'
order by hashOrderDate asc, orderDate asc, f1 asc
limit 1000;
