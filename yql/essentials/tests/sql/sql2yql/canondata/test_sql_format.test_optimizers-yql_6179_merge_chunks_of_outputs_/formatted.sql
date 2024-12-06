/* syntax version 1 */
/* postgres can not */
/* kikimr can not - yt pragma */
PRAGMA yt.MinPublishedAvgChunkSize = "0";
PRAGMA yt.MinTempAvgChunkSize = "0";

USE plato;

$i = (
    SELECT
        subkey AS s
    FROM
        Input
    WHERE
        key == "112"
    LIMIT 1
);

$j = (
    SELECT
        subkey AS s
    FROM
        Input
    WHERE
        key == "113"
    LIMIT 1
);

SELECT
    *
FROM
    Input
WHERE
    CAST(TableRecordIndex() AS String) == $i
    OR CAST(TableRecordIndex() AS String) == $j
;
