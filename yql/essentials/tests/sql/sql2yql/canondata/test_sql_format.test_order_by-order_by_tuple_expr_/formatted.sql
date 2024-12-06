/* postgres can not */
/* multirun can not */
/* syntax version 1 */
USE plato;
$keys = ($key) -> {
    RETURN AsTuple($key, $key);
};

SELECT
    *
FROM
    Input
ORDER BY
    $keys(value)
;
