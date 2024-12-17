$p = (select p_partkey, p_name
from
    `/Root/part`
where FIND(p_name, 'rose') IS NOT NULL);

$j1 = (select ps_partkey, ps_suppkey, ps_supplycost
from
    `/Root/partsupp` as ps
join $p as p
on ps.ps_partkey = p.p_partkey);

$j2 = (select l_suppkey, l_partkey, l_orderkey, l_extendedprice, l_discount, ps_supplycost, l_quantity
from
    `/Root/lineitem` as l
join $j1 as j
on l.l_suppkey = j.ps_suppkey AND l.l_partkey = j.ps_partkey);

$j3 = (select l_orderkey, s_nationkey, l_extendedprice, l_discount, ps_supplycost, l_quantity
from
    `/Root/supplier` as s
join $j2 as j
on j.l_suppkey = s.s_suppkey);

$j4 = (select o_orderdate, l_extendedprice, l_discount, ps_supplycost, l_quantity, s_nationkey
from
    `/Root/orders` as o
join $j3 as j
on o.o_orderkey = j.l_orderkey);

$j5 = (select n_name, o_orderdate, l_extendedprice, l_discount, ps_supplycost, l_quantity
from
    `/Root/nation` as n
join $j4 as j
on j.s_nationkey = n.n_nationkey
);

$profit = (select 
    n_name as nation,
    DateTime::GetYear(cast(o_orderdate as timestamp)) as o_year,
    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
from $j5);

select
    nation,
    o_year,
    sum(amount) as sum_profit
from $profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;
