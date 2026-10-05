PRAGMA config.flags('OptimizerFlags', 'PushdownFiltersOverWindow');

$src = (
    SELECT
        *
    FROM
        as_table([
            <|user: Nothing(String?), ts: Nothing(Int32?), payload: 1|>,
            <|user: 'u1', ts: Nothing(Int32?), payload: 2|>,
            <|user: Nothing(String?), ts: Nothing(Int32?), payload: 3|>,
            <|user: 'u1', ts: Nothing(Int32?), payload: 4|>,
            <|user: Nothing(String?), ts: 1, payload: 5|>,
            <|user: 'u1', ts: 2, payload: 6|>,
            <|user: Nothing(String?), ts: 2, payload: 7|>,
            <|user: 'u1', ts: 3, payload: 8|>,
            <|user: Nothing(String?), ts: 3, payload: 9|>,
            <|user: 'u1', ts: 4, payload: 10|>,
            <|user: Nothing(String?), ts: 10, payload: 11|>,
            <|user: 'u1', ts: 11, payload: 12|>,
            <|user: Nothing(String?), ts: 21, payload: 13|>,
            <|user: 'u1', ts: 22, payload: 14|>,
            <|user: Nothing(String?), ts: 31, payload: 15|>,
            <|user: 'u1', ts: 32, payload: 16|>,
            <|user: Nothing(String?), ts: 50, payload: 17|>,
            <|user: 'u1', ts: 51, payload: 18|>,
        ])
);

$src = (
    SELECT
        user,
        ts,
        SessionStart() OVER w AS ss,
    FROM
        $src
    WINDOW
        w AS (
            PARTITION BY
                user,
                SessionWindow(ts, 10)
            ORDER BY
                ts
        )
);

SELECT
    *
FROM
    $src
WHERE
    ss IS NOT NULL
ORDER BY
    user,
    ts
;
