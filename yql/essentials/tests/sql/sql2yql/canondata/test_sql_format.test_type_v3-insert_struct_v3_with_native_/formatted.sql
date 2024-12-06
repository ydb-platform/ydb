/* syntax version 1 */
/* multirun can not */
/* postgres can not */
/* kikimr can not */
USE plato;

PRAGMA yt.UseNativeYtTypes = "1";

INSERT INTO @a WITH truncate
SELECT
    *
FROM
    concat(Input1, Input2)
;

COMMIT;

INSERT INTO @a WITH truncate
SELECT
    *
FROM
    Input1
;

COMMIT;

INSERT INTO @a
SELECT
    *
FROM
    Input2
;

COMMIT;

INSERT INTO @a
SELECT
    *
FROM
    concat(Input1, Input2)
;
