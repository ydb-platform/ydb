/* syntax version 1 */
/* postgres can not */
USE plato;

$data = select 1 as n, AsList(4, 5, 6) as l, AsStruct(10 as n, AsList(1, 2, 3) as l) as s union all
        select 2 as n, AsList(4, 5)    as l, AsStruct(20 as n, AsList(1, 2)    as l) as s;

select n,l from $data      flatten by s.l as l order by n,l;
select n,l from $data      flatten by (s.l as l) order by n,l;
select n,l from $data      flatten by (ListExtend(s.l, AsList(100)) as l) order by n,l;

select n,l,sl from $data flatten by (l, s.l as sl) order by n,l,sl  
