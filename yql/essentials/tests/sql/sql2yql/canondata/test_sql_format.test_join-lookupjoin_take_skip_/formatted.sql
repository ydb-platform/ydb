USE plato;

PRAGMA yt.LookupJoinMaxRows = "3";
PRAGMA yt.LookupJoinLimit = '10M';

INSERT INTO @big
SELECT
    *
FROM (
    SELECT
        ListMap(ListFromRange(1, 100), ($x) -> (Unwrap(CAST($x AS String)))) AS key
)
    FLATTEN LIST BY key
ORDER BY
    key
;

COMMIT;

$small =
    SELECT
        substring(key, 0, 2) AS key,
        subkey || '000' AS subkey
    FROM
        Input
    ORDER BY
        key
    LIMIT 5 OFFSET 8;

SELECT
    *
FROM
    @big AS a
JOIN
    $small AS b
USING (key)
ORDER BY
    key
;
