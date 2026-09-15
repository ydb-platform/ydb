$sub = (
    SELECT
        x
    FROM (
        SELECT
            ListReplicate((1, 2), 10) AS x
    )
        FLATTEN LIST BY x
);

$c = (
    SELECT
        (1, 2)
);

SELECT
    *
FROM
    $sub
WHERE
    x IN $c
;
