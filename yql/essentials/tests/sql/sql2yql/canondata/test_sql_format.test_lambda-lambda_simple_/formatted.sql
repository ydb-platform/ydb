/* postgres can not */
USE plato;

PRAGMA DisableSimpleColumns;

$shiftSteps = 1;

$linear = ($x, $z) -> {
    $v = 10 * $z + $x;
    $shift = ($item) -> {
        RETURN $item << $shiftSteps;
    };
    RETURN $shift($v);
};

--INSERT INTO Output
SELECT
    t.*,
    $linear(CAST(key AS uint64), CAST(subkey AS uint64))
FROM
    Input AS t
;
