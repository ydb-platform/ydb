/* syntax version 1 */
/* postgres can not */
USE plato;

$train =
SELECT * FROM Input WHERE key > "900" GROUP BY value;

$method = ($stream) -> {
    $func = Callable(
        CallableType(0, TypeOf($stream), TypeOf($stream)),
        ($_1) -> { return $_1; }
    );
    RETURN $func($stream);
};

$prediction =
    PROCESS $train
    USING $method(TableRows());

SELECT * FROM $prediction;
