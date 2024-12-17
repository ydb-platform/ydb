/* postgres can not */
/* kikimr can not */
USE plato;

PRAGMA yt.MapJoinLimit = '1M';
PRAGMA yt.TmpFolder = '//custom_tmp';

-- MapJoin with table content
$input = (
    SELECT
        CAST(a.key AS Uint64) AS key
    FROM
        Input AS a
    CROSS JOIN
        Input AS b
);

-- ResFill with table content
SELECT
    sum(key)
FROM
    $input
;
