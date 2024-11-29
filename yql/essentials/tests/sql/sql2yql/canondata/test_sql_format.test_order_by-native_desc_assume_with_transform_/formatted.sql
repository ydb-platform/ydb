/* postgres can not */
/* multirun can not */
/* syntax version 1 */
USE plato;
PRAGMA yt.UseNativeDescSort;

INSERT INTO Output
SELECT
    -(CAST(key AS Int32) ?? 0) AS key,
    subkey,
    value
FROM Input
ASSUME ORDER BY
    key DESC;
