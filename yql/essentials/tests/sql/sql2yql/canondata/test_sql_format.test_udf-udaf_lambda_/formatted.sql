/* postgres can not */
$create = ($item, $_parent) -> {
    RETURN $item
};
$add = ($state, $item, $_parent) -> {
    RETURN $item + $state
};
$merge = ($state1, $state2) -> {
    RETURN $state1 + $state2
};
$get_result = ($state) -> {
    RETURN $state
};
$serialize = ($state) -> {
    RETURN $state
};
$deserialize = ($state) -> {
    RETURN $state
};

SELECT
    UDAF(
        length(key),
        $create,
        $add,
        $merge,
        $get_result,
        $serialize,
        $deserialize,
        0u
    )
FROM
    plato.Input
;
