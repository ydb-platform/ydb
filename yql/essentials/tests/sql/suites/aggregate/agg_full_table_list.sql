/* syntax version 1 */
/* postgres can not */
select agglist(x) from (
select 1 as x
);


select agglist(x) from (
select 1 as x
limit 0
);
