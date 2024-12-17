/* syntax version 1 */
/* multirun can not */
/* postgres can not */
/* kikimr can not */
USE plato;

PRAGMA yt.UseNativeYtTypes = '0';

INSERT INTO Output WITH truncate
SELECT
    *
FROM
    concat(Input1, Input2)
;

COMMIT;

INSERT INTO Output WITH truncate
SELECT
    *
FROM
    Input1
;

COMMIT;

INSERT INTO Output
SELECT
    *
FROM
    Input2
;

COMMIT;

INSERT INTO Output
SELECT
    *
FROM
    concat(Input1, Input2)
;
