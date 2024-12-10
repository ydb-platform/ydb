/* syntax version 1 */
/* postgres can not */
$init = ($row) -> (AsStruct($row.ts ?? 0 AS value, 1 AS count));
$calculate = ($_row, $state) -> ($state.value);

-- split partition into two-element grooups, make session key to be cumulative sum of ts from partition start
$update = ($row, $state) -> {
    $state = AsStruct($state.count + 1 AS count, $state.value AS value);
    $state = AsStruct($state.count AS count, $state.value + ($row.ts ?? 0) AS value);
    RETURN AsTuple(Unwrap($state.count % 2) == 1, $state);
};

$src =
    SELECT
        t.*,
        (ts ?? 0, payload) AS sort_col
    FROM
        plato.Input AS t
;

SELECT
    COUNT(1) AS session_len,
FROM
    $src
GROUP BY
    user,
    SessionWindow(sort_col, $init, $update, $calculate)
ORDER BY
    session_len
;
