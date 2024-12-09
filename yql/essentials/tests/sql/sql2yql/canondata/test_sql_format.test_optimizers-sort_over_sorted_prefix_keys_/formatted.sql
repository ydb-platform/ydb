/* postgres can not */
/* kikimr can not - anon tables */
USE plato;

INSERT INTO @a
SELECT
    *
FROM Input0
ORDER BY
    key ASC,
    subkey ASC;
COMMIT;

SELECT
    *
FROM @a
ORDER BY
    key ASC;
