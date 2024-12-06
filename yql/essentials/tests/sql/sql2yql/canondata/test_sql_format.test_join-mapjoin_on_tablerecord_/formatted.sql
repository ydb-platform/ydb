USE plato;
PRAGMA yt.MapJoinLimit = "1M";

$i =
    SELECT
        TableRecordIndex() AS ind,
        t.*
    FROM
        Input AS t
;

$filter =
    SELECT
        min(ind) AS ind
    FROM
        $i
    GROUP BY
        subkey
;

SELECT
    *
FROM
    Input
WHERE
    TableRecordIndex() IN $filter
;
