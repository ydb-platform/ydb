/* syntax version 1 */
/* postgres can not */
$x = 1.0/length(CAST(Unicode::ToUpper("ab"u) AS String));
select Percentile(key,$x) from (
select 1 as key
union all
select 2 as key)
