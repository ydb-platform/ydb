$sub = (
    SELECT
        x
    FROM (
        SELECT
            ListReplicate((1,), 10) AS x
    )
        FLATTEN LIST BY x
);

$c = (
    SELECT
        (1,)
);

SELECT
    *
FROM
    $sub
WHERE
    x IN $c
;
