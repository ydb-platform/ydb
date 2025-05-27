/* syntax version 1 */
/* postgres can not */
USE plato;

$data = select 1 as n, AsList(4, 5, 6) as l, AsStruct(10 as n, AsList(1, 2, 3) as l) as s union all
        select 2 as n, AsList(4, 5)    as l, AsStruct(20 as n, AsList(1, 2)    as l) as s;

select n,l  from $data as l flatten by l   order by n,l;
select n,l  from $data as l flatten by l.l order by n,l;

select n,l    from $data as s flatten by s.l order by n,l;
select n,newl from $data as s flatten by (s.l as newl) order by n,newl;

select n,l from $data as s flatten by (s.s.l as l) order by n,l;

