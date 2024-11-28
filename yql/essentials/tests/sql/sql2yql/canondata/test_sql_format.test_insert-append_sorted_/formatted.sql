/* multirun can not */
INSERT INTO plato.Output1
SELECT
    *
FROM plato.Input
ORDER BY
    key;

INSERT INTO plato.Output2
SELECT
    *
FROM plato.Input
ORDER BY
    key,
    subkey;

INSERT INTO plato.Output3
SELECT
    *
FROM plato.Input
ORDER BY
    key,
    subkey,
    value;

INSERT INTO plato.Output4
SELECT
    *
FROM plato.Input;

INSERT INTO plato.Output5
SELECT
    *
FROM plato.Input
ORDER BY
    subkey;

INSERT INTO plato.Output6
SELECT
    *
FROM plato.Input
ORDER BY
    key DESC;

INSERT INTO plato.Output7
SELECT
    *
FROM plato.Input
ORDER BY
    key DESC,
    subkey;

INSERT INTO plato.Output8
SELECT
    *
FROM plato.Input
ORDER BY
    key || subkey;
