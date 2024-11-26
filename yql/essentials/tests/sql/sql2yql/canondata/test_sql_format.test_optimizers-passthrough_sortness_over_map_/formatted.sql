/* postgres can not */
/* kikimr can not - anon tables */
USE plato;

INSERT INTO @a
SELECT
    *
FROM Input0
ORDER BY
    key,
    subkey;
COMMIT;

INSERT INTO @c
SELECT
    *
FROM @a
WHERE key < "100"
ORDER BY
    key,
    subkey;

INSERT INTO @d
SELECT
    key AS key,
    "" AS subkey,
    "value:" || value AS value
FROM @a
WHERE key < "100"
ORDER BY
    key;
COMMIT;

SELECT
    *
FROM @c;

SELECT
    *
FROM @d;
