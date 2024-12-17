/* syntax version 1 */
/* postgres can not */
USE plato;

PRAGMA yt.UseNativeYtTypes = '1';

INSERT INTO Output
SELECT
    *
FROM (
    SELECT
        Json(@@{"a": 4.7, "c": "abc"}@@) AS j
    UNION ALL
    SELECT
        Json(@@{"d": "fff"}@@) AS j
);

COMMIT;

SELECT
    *
FROM
    Output
WHERE
    ToBytes(j) != ''
;
