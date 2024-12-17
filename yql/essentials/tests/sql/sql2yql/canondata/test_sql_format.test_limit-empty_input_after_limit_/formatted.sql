/* postgres can not */
$in = (
    SELECT
        *
    FROM
        plato.Input
    WHERE
        key == '150'
    UNION ALL
    SELECT
        *
    FROM
        plato.Input
    WHERE
        key == '075'
);

SELECT
    *
FROM
    $in
ORDER BY
    key
LIMIT 100 OFFSET 90;
