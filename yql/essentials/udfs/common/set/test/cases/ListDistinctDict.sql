/* syntax version 1 */
select AGGREGATE_LIST_DISTINCT(x) from 
(select [{1,2},{1},{1,2}] as x)
flatten list by x;

select AGGREGATE_LIST_DISTINCT(x) from 
(select [{1:2},{1:3},{1:2}] as x)
flatten list by x;
