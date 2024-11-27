/* postgres can not */
USE plato;
PRAGMA DisableSimpleColumns;
$shiftSteps = ($item) -> {
    RETURN CAST($item % 4 AS Uint8) ?? 0
};
$linear = ($x, $z, $func) -> {
    $v = 10 * $z + $x;
    $shift = ($item, $sk) -> {
        RETURN $item << $func($sk)
    };
    RETURN $shift($v, $z)
};

--INSERT INTO Output
SELECT
    t.*,
    $linear(CAST(key AS uint64), CAST(subkey AS uint64), $shiftSteps)
FROM Input
    AS t;
