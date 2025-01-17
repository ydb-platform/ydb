select
r,x,
nth_value(x,1) over w as nr1,
nth_value(x,1) ignore nulls over w as ni1,
nth_value(x,2) over w as nr2,
nth_value(x,2) ignore nulls over w as ni2,
nth_value(x,3) over w as nr3,
nth_value(x,3) ignore nulls over w as ni3,
nth_value(x,4) over w as nr4,
nth_value(x,4) ignore nulls over w as ni4
from (values (1,3),(2,null),(3,4),(4,5)) as a(r,x)
window w as (order by r)
order by r

