/* syntax version 1 */
/* postgres can not */
/* yt can not */
pragma warning("disable", "4510");

$d1 = ToDict([(1, 1u)]);
$d2 = AsDict((2, 2u));
$d3 = YQL::Dict(Dict<Int32, Uint32>);
$d4 = YQL::Dict(Dict<Int32, Uint32>, (3, 3u));

$s1 = ToSet([1u]);
$s2 = AsSet(2u);

select AsList({100u:100}, $d1, $d2, $d3, $d4);
select AsList({100}, $s1, $s2);
