
-- TPC-H/TPC-R Suppliers Who Kept Orders Waiting Query (Q21)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$n = select n_nationkey from plato.nation
where n_name = 'EGYPT';

$s = select s_name, s_suppkey from plato.supplier as supplier
join $n as nation
on supplier.s_nationkey = nation.n_nationkey;

$l = select l_suppkey, l_orderkey from plato.lineitem
where l_receiptdate > l_commitdate;

$j1 = select s_name, l_suppkey, l_orderkey from $l as l1
join $s as supplier
on l1.l_suppkey = supplier.s_suppkey;

$j1_1 = select l1.l_orderkey as l_orderkey from $j1 as l1
join $l as l3
on l1.l_orderkey = l3.l_orderkey
where l3.l_suppkey <> l1.l_suppkey;

$j2 = select s_name, l_suppkey, l_orderkey from $j1 as l1
left only join $j1_1 as l3
on l1.l_orderkey = l3.l_orderkey;

$j2_1 = select l1.l_orderkey as l_orderkey from $j2 as l1
join plato.lineitem as l2
on l1.l_orderkey = l2.l_orderkey
where l2.l_suppkey <> l1.l_suppkey;

$j3 = select s_name, l1.l_suppkey as l_suppkey, l1.l_orderkey as l_orderkey from $j2 as l1
left semi join $j2_1 as l2
on l1.l_orderkey = l2.l_orderkey;

$j4 = select s_name from $j3 as l1
join plato.orders as orders
on orders.o_orderkey = l1.l_orderkey
where o_orderstatus = 'F';

select s_name,
    count(*) as numwait from $j4
group by
    s_name
order by
    numwait desc,
    s_name
limit 100;

