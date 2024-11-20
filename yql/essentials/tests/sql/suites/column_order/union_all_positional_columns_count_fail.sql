/* postgres can not */
/* syntax version 1 */
pragma PositionalUnionAll;

select 1 as c, 2 as b, 3 as a
union all
select 1 as c, 2 as b;
