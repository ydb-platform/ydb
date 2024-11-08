/* syntax version 1 */
select AGGREGATE_LIST_DISTINCT(x) from 
(select [{<|a:1,b:2|>},{<|a:1,b:3|>},{<|a:1,b:2|>}] as x)
flatten list by x;
