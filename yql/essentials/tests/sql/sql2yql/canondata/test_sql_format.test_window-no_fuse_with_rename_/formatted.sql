PRAGMA config.flags('OptimizerFlags', 'PayloadRenameOverWindow');

$src = (
    SELECT
        *
    FROM
        as_table([
            <|key: 3, subkey: 1, a: 19, b: 20, c: 21, d: 22|>,
            <|key: 3, subkey: 2, a: 23, b: 24, c: 25, d: 26|>,
            <|key: 1, subkey: 1, a: 3, b: 4, c: 5, d: 6|>,
            <|key: 1, subkey: 2, a: 7, b: 8, c: 9, d: 10|>,
            <|key: 2, subkey: 1, a: 11, b: 12, c: 13, d: 14|>,
            <|key: 2, subkey: 2, a: 15, b: 16, c: 17, d: 18|>,
        ])
);

$src = (
    SELECT
        t.*,
        sum(a) OVER w AS sum_a
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

$src = (
    SELECT
        t.*,
        lead(c) OVER w AS next_c,
        lag(d) OVER w AS prev_d
    FROM
        $src AS t
    WINDOW
        w AS (
            PARTITION BY
                key
            ORDER BY
                sum_a
        )
);

SELECT
    *
FROM
    $src
ORDER BY
    key,
    subkey
;
