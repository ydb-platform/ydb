/* syntax version 1 */
/* postgres can not */
USE plato;

$empty = ($list) -> {
    RETURN ListCreate(TypeOf($list[0]));
};

SELECT
    $empty(AGGREGATE_LIST(key))
FROM Input
GROUP BY value;
