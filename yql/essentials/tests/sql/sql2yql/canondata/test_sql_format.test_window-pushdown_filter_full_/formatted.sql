PRAGMA config.flags('OptimizerFlags', 'PushdownFiltersOverWindow');

$src = (
    SELECT
        *
    FROM
        as_table([
            <|key: 1, subkey: 1, a: 3, b: 4, c: 5, d: 6|>,
            <|key: 1, subkey: 2, a: 7, b: 8, c: 9, d: 10|>,
            <|key: 2, subkey: 1, a: 11, b: 12, c: 13, d: 14|>,
            <|key: 2, subkey: 2, a: 15, b: 16, c: 17, d: 18|>,
            <|key: 3, subkey: 1, a: 19, b: 20, c: 21, d: 22|>,
            <|key: 3, subkey: 2, a: 23, b: 24, c: 25, d: 26|>,
        ])
);

$src = (
    SELECT
        t.*,
        lead(a) OVER w AS next_a,
        lag(b) OVER w AS prev_b
    FROM
        $src AS t
    WINDOW
        w AS (
            PARTITION BY
                key
            ORDER BY
                subkey
        )
);

SELECT
    *
FROM
    $src
WHERE
    key == 1 AND Opaque(1 < 2)
;
