/* syntax version 1 */
/* postgres can not */
USE plato;
PRAGMA yt.IgnoreTypeV3;

SELECT
    key,
    Yson::LookupString(subkey, "a") AS a,
FROM
    Input
;
