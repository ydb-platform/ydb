/* syntax version 1 */
/* postgres can not */
-- count example
$create = ($_item, $_parent) -> { return 1 };
$add = ($state, $_item, $_parent) -> { return 1 + $state };
$merge = ($state1, $state2) -> { return $state1 + $state2 };
$get_result = ($state) -> { return $state };
$serialize = ($state) -> { return $state };
$deserialize = ($state) -> { return $state };
$default = 0;

$f = AGGREGATION_FACTORY("udaf", $create, $add, $merge, $get_result, $serialize, $deserialize, $default);
$t = AsList(AsStruct(1 as a),AsStruct(2 as a), AsStruct(1 as a));

use plato;
insert into @a select * from as_table($t);
commit;

select AGGREGATE_BY(distinct cast(Unicode::ToLower(cast(a as Utf8) || "00"u) as Int), $f) from @a;
