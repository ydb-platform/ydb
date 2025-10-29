select 
Pg::string_agg(x,','p)
from (values ('a'p),('b'p),('c'p)) as a(x);

select 
Pg::string_agg(x,','p) over (order by x),
from (values ('a'p),('b'p),('c'p)) as a(x);

$agg_string_agg = AggregationFactory("Pg::string_agg");

select 
AggregateBy((x,','p),$agg_string_agg)
from (values ('a'p),('b'p),('c'p)) as a(x);

select 
AggregateBy((x,','p),$agg_string_agg) over (order by x),
from (values ('a'p),('b'p),('c'p)) as a(x);

$agg_max = AggregationFactory("Pg::max");

select 
AggregateBy(x,$agg_max)
from (values ('a'p),('b'p),('c'p)) as a(x);

select 
AggregateBy(x,$agg_max) over (order by x),
from (values ('a'p),('b'p),('c'p)) as a(x);
