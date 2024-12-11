/* postgres can not */
/* multirun can not */
/* syntax version 1 */
USE plato;
pragma yt.UseNativeDescSort;

INSERT INTO Output
SELECT
    -(CAST(key as Int32) ?? 0) as key,
    subkey,
    value
FROM Input
ASSUME ORDER BY key DESC;
