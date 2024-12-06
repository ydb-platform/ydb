/* postgres can not */
/* hybridfile can not YQL-17743 */
USE plato;

PRAGMA OrderedColumns;
PRAGMA yt.UseNativeYtTypes;

INSERT INTO @tmp
SELECT
    key,
    AsList(subkey),
    value
FROM
    Input
WHERE
    key > '000'
ORDER BY
    value DESC
;
