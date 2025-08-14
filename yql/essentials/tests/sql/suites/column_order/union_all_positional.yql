/* postgres can not */
/* syntax version 1 */
pragma PositionalUnionAll;
pragma warning("disable", "1107");

select (1,1u) as z, (2,2u) as y, (3,3u) as x
union all
select (1u,1) as a, (2u,2) as b, (3u,3) as c;

