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
            substring(c_phone from 1 for 2) as cntrycode,
            c_acctbal
        from
            {{customer}}
        where
            substring(c_phone from 1 for 2) in
                ('31', '29', '30', '26', '28', '25', '15')
            and c_acctbal > (
                select
                    avg(c_acctbal)
                from
                    {{customer}}
                where
                    c_acctbal > 0.00::numeric
                    and substring(c_phone from 1 for 2) in
                        ('31', '29', '30', '26', '28', '25', '15')
            )
            and not exists (
                select
                    *
                from
                    {{orders}}
                where
                    o_custkey = c_custkey
            )
    ) as custsale
group by
    cntrycode
order by
    cntrycode;
