/* postgres can not */
/* multirun can not */
INSERT INTO plato.Output WITH truncate
SELECT
    *
FROM
    plato.Input
ORDER BY
    key
;

INSERT INTO plato.Output
SELECT
    *
FROM
    plato.Input
;

COMMIT;

----------------------------------------
INSERT INTO plato.Output WITH truncate
SELECT
    *
FROM
    plato.Input
;

INSERT INTO plato.Output
SELECT
    *
FROM
    plato.Input
ORDER BY
    key
;

COMMIT;

----------------------------------------
INSERT INTO plato.Output WITH truncate
SELECT
    *
FROM
    plato.Input
ORDER BY
    key,
    subkey
;

INSERT INTO plato.Output
SELECT
    *
FROM
    plato.Input
ORDER BY
    key,
    subkey
;

COMMIT;

----------------------------------------
INSERT INTO plato.Output WITH truncate
SELECT
    *
FROM
    plato.Input
ORDER BY
    key
;

INSERT INTO plato.Output
SELECT
    *
FROM
    plato.Input
ORDER BY
    key DESC
;

COMMIT;

----------------------------------------
INSERT INTO plato.Output WITH truncate
SELECT
    *
FROM
    plato.Input
ORDER BY
    key
;

INSERT INTO plato.Output
SELECT
    *
FROM
    plato.Input
ORDER BY
    key || subkey
;

COMMIT;

----------------------------------------
INSERT INTO plato.Output WITH truncate
SELECT
    *
FROM
    plato.Input
ORDER BY
    key DESC
;

INSERT INTO plato.Output
SELECT
    *
FROM
    plato.Input
ORDER BY
    key DESC
;

COMMIT;

----------------------------------------
INSERT INTO plato.Output WITH truncate
SELECT
    *
FROM
    plato.Input
ORDER BY
    key || subkey
;

INSERT INTO plato.Output
SELECT
    *
FROM
    plato.Input
ORDER BY
    key || subkey
;
