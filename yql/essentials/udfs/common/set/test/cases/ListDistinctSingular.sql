/* syntax version 1 */
select AGGREGATE_LIST_DISTINCT(x) from 
(select [null, null] as x)
flatten list by x;

select AGGREGATE_LIST_DISTINCT(x) from 
(select [void(), void()] as x)
flatten list by x;

select AGGREGATE_LIST_DISTINCT(x) from 
(select [[], []] as x)
flatten list by x;

select AGGREGATE_LIST_DISTINCT(x) from 
(select [{}, {}] as x)
flatten list by x;
