USE plato;

PRAGMA yt.JoinMergeTablesLimit = '10';
PRAGMA yt.JoinMergeUnsortedFactor = '2';

SELECT
    *
FROM
    Input
WHERE
    key IN (
        SELECT
            '023' AS key
        UNION ALL
        SELECT
            '911' AS key
        UNION ALL
        SELECT
            '911' AS key
    )
;
