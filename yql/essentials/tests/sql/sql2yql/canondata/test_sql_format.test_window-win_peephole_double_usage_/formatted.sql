/* syntax version 1 */
/* postgres can not */
$input = (
    SELECT
        *
    FROM
        as_table([<|key: 1|>, <|key: 1|>])
);

$src = (
    SELECT
        key,
        MIN(key) OVER w AS curr_min
    FROM
        $input
    WINDOW
        w AS (
            ORDER BY
                key
        )
);

SELECT
    *
FROM
    $src
UNION ALL
SELECT
    *
FROM
    $src
;
