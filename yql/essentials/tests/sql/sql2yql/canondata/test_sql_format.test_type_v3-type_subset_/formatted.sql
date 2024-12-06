/* syntax version 1 */
/* multirun can not */
/* postgres can not */
USE plato;

PRAGMA yt.UseNativeYtTypes = "1";
PRAGMA yt.NativeYtTypeCompatibility = "complex";
PRAGMA yt.MaxInputTables = "2";

INSERT INTO Output WITH truncate
SELECT
    key
FROM
    range("")
WHERE
    key > "000"
;
