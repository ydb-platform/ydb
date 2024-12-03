/* postgres can not */
/* syntax version 1 */
USE plato;
$shiftSteps = 1;
$linear = ($x, $z) -> {
    $v = 10 * $z + $x;
    $shift = ($item) -> {
        RETURN $item << $shiftSteps
    };
    $res = Math::Floor(Math::Pi() * $shift($v));
    RETURN $res
};

--INSERT INTO Output
SELECT
    t.*,
    $linear(CAST(key AS uint64), CAST(subkey AS uint64)) AS linear
FROM Input
    AS t;
