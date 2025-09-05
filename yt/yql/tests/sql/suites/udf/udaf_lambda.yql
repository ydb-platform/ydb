/* postgres can not */
$create = ($item, $_parent) -> { return $item };
$add = ($state, $item, $_parent) -> { return $item + $state };
$merge = ($state1, $state2) -> { return $state1 + $state2 };
$get_result = ($state) -> { return $state };
$serialize = ($state) -> { return $state };
$deserialize = ($state) -> { return $state };

SELECT UDAF(
    length(key),
    $create,
    $add,
    $merge,
    $get_result,
    $serialize,
    $deserialize,
    0u
) FROM plato.Input;
