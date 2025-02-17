{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R National Market Share Query (Q8)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    o_year,
    sum(case
        when nation = 'MOZAMBIQUE' then volume
        else 0
    end) / sum(volume) as mkt_share
from
    (
        select
            DateTime::GetYear(o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            {{part}}
            cross join {{supplier}}
            cross join {{lineitem}}
            cross join {{orders}}
            cross join {{customer}}
            cross join {{nation}} n1
            cross join {{nation}} n2
            cross join {{region}}
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = 'AFRICA'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date('1995-01-01') and date('1996-12-31')
            and p_type = 'ECONOMY PLATED COPPER'
    ) as all_nations
group by
    o_year
order by
    o_year;

