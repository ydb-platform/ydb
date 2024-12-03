/* syntax version 1 */
/* multirun can not */
/* postgres can not */
USE plato;
PRAGMA yt.UseNativeYtTypes = "1";
PRAGMA yt.NativeYtTypeCompatibility = "complex";

INSERT INTO Output
SELECT
    t.*,
    CAST(TableRecordIndex() AS UInt64) AS record_idx
FROM Input
    AS t
ORDER BY
    record_idx;
