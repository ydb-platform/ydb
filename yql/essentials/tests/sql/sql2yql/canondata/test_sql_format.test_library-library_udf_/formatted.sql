/* postgres can not */
/* kikimr can not - range not supported */
PRAGMA Library('udf.sql');

IMPORT udf SYMBOLS $f;

SELECT
    $f
;
