/* custom error:Input #1 does not have ordered columns. Consider making column order explicit by using SELECT with column names*/
pragma PositionalUnionAll;

select 1 as c, 2 as b, 3 as a
union all
select * from as_table([<|c:1, b:2, a:3|>]);

