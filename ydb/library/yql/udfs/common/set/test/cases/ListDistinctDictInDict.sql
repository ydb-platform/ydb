/* syntax version 1 */
select AGGREGATE_LIST_DISTINCT(x) from 
(select [{{1,2},{1}},{{1}},{{1,2},{1}}] as x)
flatten list by x;
