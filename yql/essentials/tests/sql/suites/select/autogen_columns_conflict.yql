/* dq can not */
/* dqfile can not */
/* yt can not */
/* postgres can not */

$src = select <|column0:1|> as x;

$src = select * from $src flatten columns;

select 2, a.* from $src as a;

select 2, 1 as column0;

select 2, 1 as column0
union all
select 4, 3 as column0;


select * from (select 1) as a join (select 1) as b using(column0);



select

  1 as a,
  2 as b,
  3, -- should be column2
  4 as column1;

