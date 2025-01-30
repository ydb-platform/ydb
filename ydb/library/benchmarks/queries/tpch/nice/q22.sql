{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            Substring(CAST(c_phone as String), 0u, 2u) as cntrycode,
            c_acctbal
        from
            {{customer}}
            cross join (
                select
                    avg(c_acctbal) as avg_acctbal
                from
                    {{customer}}
                where
                    c_acctbal > 0.00
                    and Substring(CAST (c_phone as String), 0u, 2u) in
                        ('31', '29', '30', '26', '28', '25', '15')
            ) avg_customer
            left only join 
                {{orders}}
            on {{orders}}.o_custkey = {{customer}}.c_custkey
        where
            Substring(CAST (c_phone as String), 0u, 2u) in
                ('31', '29', '30', '26', '28', '25', '15')
            and c_acctbal > avg_acctbal
    ) as custsale
group by
    cntrycode
order by
    cntrycode;
