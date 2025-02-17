/* postgres can not */
/* kikimr can not - anon tables */
USE plato;

INSERT INTO @a
SELECT
    *
FROM Input0
ORDER BY key, subkey;

commit;

INSERT INTO @c
SELECT * FROM @a
WHERE key < "100"
ORDER BY key, subkey;

INSERT INTO @d
SELECT
    key as key,
    "" as subkey,
    "value:" || value as value
FROM @a
WHERE key < "100"
ORDER BY key;

commit;

select * from @c;

select * from @d;
