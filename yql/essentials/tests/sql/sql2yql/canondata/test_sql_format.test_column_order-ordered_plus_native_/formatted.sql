/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA OrderedColumns;
PRAGMA yt.UseNativeYtTypes;

INSERT INTO Output WITH TRUNCATE
SELECT
    aggr_list(subkey) AS subkey,
    key,
FROM Input
    AS a
GROUP BY
    a.key AS key;
