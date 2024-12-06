/* syntax version 1 */
/* kikimr can not */
USE plato;

$udf_stream = ($input) -> {
    RETURN $input
};

$res =
    REDUCE Input0
    ON
        key
    USING ALL $udf_stream(TableRows());

SELECT
    *
FROM
    $res
ORDER BY
    value
;
