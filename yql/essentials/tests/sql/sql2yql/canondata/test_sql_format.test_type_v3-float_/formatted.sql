/* syntax version 1 */
/* postgres can not */
USE plato;

PRAGMA yt.UseNativeYtTypes = "1";

INSERT INTO Output
SELECT
    *
FROM (
    SELECT
        Float("3.14") AS f
    UNION ALL
    SELECT
        Float("1.2") AS f
);

COMMIT;

SELECT
    *
FROM
    Output
WHERE
    f != Float("5.3")
;
