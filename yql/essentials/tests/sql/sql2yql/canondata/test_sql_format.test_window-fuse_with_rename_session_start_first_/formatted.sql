PRAGMA config.flags('OptimizerFlags', 'PayloadRenameOverWindow');

$src = (
    SELECT
        *
    FROM
        as_table([
            <|user: NULL, ts: NULL, payload: 1|>,
            <|user: 'u1', ts: NULL, payload: 2|>,
            <|user: NULL, ts: NULL, payload: 3|>,
            <|user: 'u1', ts: NULL, payload: 4|>,
            <|user: NULL, ts: 1, payload: 5|>,
            <|user: 'u1', ts: 2, payload: 6|>,
            <|user: NULL, ts: 2, payload: 7|>,
            <|user: 'u1', ts: 3, payload: 8|>,
            <|user: NULL, ts: 3, payload: 9|>,
            <|user: 'u1', ts: 4, payload: 10|>,
            <|user: NULL, ts: 10, payload: 11|>,
            <|user: 'u1', ts: 11, payload: 12|>,
            <|user: NULL, ts: 21, payload: 13|>,
            <|user: 'u1', ts: 22, payload: 14|>,
            <|user: NULL, ts: 31, payload: 15|>,
            <|user: 'u1', ts: 32, payload: 16|>,
            <|user: NULL, ts: 50, payload: 17|>,
            <|user: 'u1', ts: 51, payload: 18|>,
        ])
);

$init = ($row) -> (AsStruct($row.ts ?? 0 AS value, 1 AS count));
$calculate = ($_row, $state) -> ($state.value);

-- split partition into two-element groups, make session key to be cumulative sum of ts from partition start
$update = ($row, $state) -> {
    $state = AsStruct($state.count + 1 AS count, $state.value AS value);
    $state = AsStruct($state.count AS count, $state.value + ($row.ts ?? 0) AS value);
    RETURN AsTuple(Unwrap($state.count % 2) == 1, $state);
};

$src = (
    SELECT
        t.*,
        AGGREGATE_LIST(CAST(ts AS string) ?? 'null') OVER w AS ts_session,
        COUNT(1) OVER w AS session_len,
        SessionStart() OVER w AS session_start,
        SessionState() OVER w AS session_state,
    FROM
        $src AS t
    WINDOW
        w AS (
            PARTITION BY
                user,
                SessionWindow(ts + 1, $init, $update, $calculate)
            ORDER BY
                ts
        )
);

$src = (
    SELECT
        t.*,
        AGGREGATE_LIST(CAST(ts AS string) ?? 'null') OVER w AS ts_session2,
        COUNT(1) OVER w AS session_len2,
    FROM
        $src AS t
    WINDOW
        w AS (
            PARTITION BY
                user,
                SessionWindow(ts + 1, $init, $update, $calculate)
            ORDER BY
                ts
        )
);

SELECT
    *
FROM
    $src
ORDER BY
    user,
    payload
;
