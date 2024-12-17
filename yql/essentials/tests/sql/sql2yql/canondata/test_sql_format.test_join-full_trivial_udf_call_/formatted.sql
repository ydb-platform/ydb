PRAGMA DisableSimpleColumns;

/* postgres can not */
SELECT
    Input1.key,
    Input1.subkey,
    SimpleUdf::Concat(coalesce(Input1.value, ''), coalesce(Input3.value, '')) AS value
FROM
    plato.Input1
FULL JOIN
    plato.Input3
USING (key)
ORDER BY
    Input1.key,
    Input1.subkey,
    value
;
