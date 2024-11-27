/* postgres can not */
USE plato;
$func = ($x, $y) -> {
    $y, $x = AsTuple($x, $y);
    RETURN $x || "_" || $y;
};

--INSERT INTO Output
SELECT
    $func(key, subkey) AS func
FROM Input;
