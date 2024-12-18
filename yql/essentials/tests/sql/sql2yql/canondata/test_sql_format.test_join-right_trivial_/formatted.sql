PRAGMA DisableSimpleColumns;

SELECT
    coalesce(Input1.key, '_null') AS a,
    coalesce(Input1.subkey, '_null') AS b,
    coalesce(Input3.value, '_null') AS c
FROM
    plato.Input1
RIGHT JOIN
    plato.Input3
USING (key)
ORDER BY
    a,
    b,
    c
;
