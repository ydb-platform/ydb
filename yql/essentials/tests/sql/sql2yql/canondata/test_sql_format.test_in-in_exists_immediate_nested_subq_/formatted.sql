/* postgres can not */
/* ignore runonopt plan diff */
USE plato;

$q = (
    SELECT
        key
    FROM
        Input
    ORDER BY
        key
    LIMIT 100
);

$q1 = (
    SELECT
        *
    FROM
        $q
    ORDER BY
        key
    LIMIT 100
);

SELECT
    *
FROM
    Input
WHERE
    key IN (
        SELECT
            *
        FROM
            $q
        ORDER BY
            key
        LIMIT 100
    )
ORDER BY
    key
;

SELECT
    *
FROM
    Input
WHERE
    key IN $q1
ORDER BY
    key
;

SELECT
    EXISTS (
        SELECT
            key
        FROM
            $q
    )
FROM
    Input
;

SELECT
    $q
;
