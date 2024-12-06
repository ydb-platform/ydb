/* postgres can not */
/* kikimr can not */
USE plato;
PRAGMA DisableSimpleColumns;
PRAGMA yt.LookupJoinLimit = "64k";
PRAGMA yt.LookupJoinMaxRows = "100";
PRAGMA yt.QueryCacheMode = "normal";
PRAGMA yt.QueryCacheUseForCalc = "true";

INSERT INTO @tmp WITH truncate
SELECT
    *
FROM
    Input
WHERE
    subkey == "bbb"
ORDER BY
    key
;
COMMIT;

SELECT
    *
FROM
    Input AS a
INNER JOIN
    @tmp AS b
ON
    a.key == b.key
ORDER BY
    a.key,
    a.subkey
;
