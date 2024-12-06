/* custom error:Expected tuple type of size: 2, but got: 3*/
USE plato;
$func = ($x, $y) -> {
    $y, $x = AsTuple($x, $y, $x);
    RETURN $x || "_" || $y;
};

--INSERT INTO Output
SELECT
    $func(key, subkey) AS func
FROM
    Input
;
