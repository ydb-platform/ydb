-- $ID$
-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$match = Pire::Match(".*Customer.*Complaints.*");

$wo_complaints = (
    select
        s_suppkey
    from
        `$DBROOT$/supplier`
    where
        not $match(s_comment)
);

$join1 = (
    select
        ps.ps_suppkey as ps_suppkey,
        ps.ps_partkey as ps_partkey
    from
        `$DBROOT$/partsupp` as ps join $wo_complaints as w on w.s_suppkey = ps.ps_suppkey
);

select
    p.p_brand as p_brand,
    p.p_type as p_type,
    p.p_size as p_size,
    count(distinct j.ps_suppkey) as supplier_cnt
from
    $join1 as j join `$DBROOT$/part` as p on p.p_partkey = j.ps_partkey
where
    p.p_brand <> 'Brand#41'
    and (not StartsWith(p.p_type, 'LARGE BURNISHED'))
    and (p.p_size = 28 or p.p_size = 14 or p.p_size = 11 or p.p_size = 26 or p.p_size = 4 or p.p_size = 8 or p.p_size = 19 or p.p_size = 10)
group by
    p.p_brand,
    p.p_type,
    p.p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size
;
