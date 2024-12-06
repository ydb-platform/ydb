/* syntax version 1 */
/* postgres can not */
USE plato;

PRAGMA yt.UseNativeYtTypes = "1";
PRAGMA yt.NativeYtTypeCompatibility = "uuid";

INSERT INTO Output
SELECT
    *
FROM (
    SELECT
        Uuid("00000000-0000-0000-0000-100000000000")
    UNION ALL
    SELECT
        Uuid("00000000-0000-0000-0000-200000000000")
    UNION ALL
    SELECT
        Uuid("00000000-0000-0000-0000-400000000000")
    UNION ALL
    SELECT
        Uuid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF")
    UNION ALL
    SELECT
        Uuid("3200ec12-4ded-4f6c-a981-4b0ff18bbdd5")
);

COMMIT;

SELECT
    *
FROM
    Output
;
