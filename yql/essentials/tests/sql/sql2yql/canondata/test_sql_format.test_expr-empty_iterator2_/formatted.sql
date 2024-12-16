/* syntax version 1 */
/* postgres can not */
USE plato;

$train =
    SELECT
        key,
        value
    FROM
        Input
    GROUP BY
        key,
        value
    HAVING
        key > "900"
;

$method = ($stream) -> {
    $func = CALLABLE (
        CallableType(0, TypeOf($stream), TypeOf($stream)),
        ($_1) -> {
            RETURN $_1;
        }
    );
    RETURN $func($stream);
};

$prediction =
    PROCESS $train
    USING $method(TableRows())
;

SELECT
    *
FROM
    $prediction
;
