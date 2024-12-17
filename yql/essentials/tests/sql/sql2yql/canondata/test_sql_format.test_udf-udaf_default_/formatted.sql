/* syntax version 1 */
/* postgres can not */
/* syntax version 1 */
$create = ($item, $_parent) -> {
    RETURN AsList($item);
};

$add = ($state, $item, $_parent) -> {
    RETURN Yql::Append($state, $item);
};

$merge = ($state1, $state2) -> {
    RETURN ListExtend($state1, $state2);
};

$get_result = ($state) -> {
    RETURN $state;
};

$serialize = ($state) -> {
    RETURN $state;
};

$deserialize = ($state) -> {
    RETURN $state;
};

$default = ($result_type) -> {
    RETURN Yql::List($result_type);
};

$udaf_factory = AGGREGATION_FACTORY(
    'UDAF',
    $create,
    $add,
    $merge,
    $get_result,
    $serialize,
    $deserialize,
    $default
);

SELECT
    ListAggregate(AsList(1, 2), $udaf_factory)
;

SELECT
    ListAggregate(ListCreate(Int32), $udaf_factory)
;
