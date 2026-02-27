/* postgres can not */
$input = (
    SELECT
        1 AS key,
        '1' AS value
);

$x = (
    SELECT
        *
    FROM
        $input
    ORDER BY
        value
);

SELECT
    *
FROM
    $x
;

SELECT
    *
FROM (
    SELECT
        *
    FROM
        $input
    ORDER BY
        value
);

SELECT
    *
FROM (
    SELECT
        1
    FROM
        $input AS a
    JOIN
        $input AS b
    USING (key)
    ORDER BY
        a.value
);

SELECT
    *
FROM (
    SELECT
        *
    FROM
        $input
    UNION ALL
    SELECT
        *
    FROM
        $input
    ORDER BY
        key
);

SELECT
    *
FROM (
    SELECT
        key,
        prefix,
        COUNT(*) AS cnt,
        grouping(key, prefix) AS agrouping
    FROM
        $input
    GROUP BY
        ROLLUP (key AS key, Substring(value, 1, 1) AS prefix)
    ORDER BY
        key
);
