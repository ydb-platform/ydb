/* syntax version 1 */
/* postgres can not */
/* syntax version 1 */

$create = ($item, $_parent) -> { return AsList($item) };
$add = ($state, $item, $_parent) -> { return Yql::Append($state, $item) };
$merge = ($state1, $state2) -> { return ListExtend($state1,$state2) };
$get_result = ($state) -> { return $state };
$serialize = ($state) -> { return $state };
$deserialize = ($state) -> { return $state };
$default = ($result_type)->{ return Yql::List($result_type) };

$udaf_factory = AGGREGATION_FACTORY(
    "UDAF",
    $create,
    $add,
    $merge,
    $get_result,
    $serialize,
    $deserialize,
    $default
);

select ListAggregate(AsList(1,2),$udaf_factory);
select ListAggregate(ListCreate(Int32),$udaf_factory);
