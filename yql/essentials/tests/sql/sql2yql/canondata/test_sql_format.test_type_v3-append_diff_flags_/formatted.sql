/* syntax version 1 */
/* multirun can not */
/* postgres can not */
USE plato;

PRAGMA yt.UseNativeYtTypes = "1";
PRAGMA yt.NativeYtTypeCompatibility = "date";

INSERT INTO Output
SELECT
    key || "a" AS key,
    subkey
FROM
    Input
ORDER BY
    key
;
