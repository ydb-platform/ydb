/* postgres can not */
USE plato;

$x = ($key) -> {
    RETURN 'aa' || $key;
};

SELECT
    $x(key)
FROM
    Input
;

SELECT
    $x(key)
FROM
    Input
;
