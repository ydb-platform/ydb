/* postgres can not */
/* syntax version 1 */
/* custom error:Different column counts in UNION ALL inputs: input #0 has 3 column, input #1 has 2 columns*/
pragma PositionalUnionAll;

select 1 as c, 2 as b, 3 as a
union all
select 1 as c, 2 as b;
