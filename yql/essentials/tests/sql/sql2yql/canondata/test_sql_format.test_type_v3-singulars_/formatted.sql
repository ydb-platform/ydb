/* postgres can not */
USE plato;
PRAGMA yt.UseNativeYtTypes = "1";
PRAGMA yt.NativeYtTypeCompatibility = "null;void";

INSERT INTO @tmp
SELECT
    NULL AS ttt,
    Yql::Void AS v
;
COMMIT;

SELECT
    *
FROM
    @tmp
;
