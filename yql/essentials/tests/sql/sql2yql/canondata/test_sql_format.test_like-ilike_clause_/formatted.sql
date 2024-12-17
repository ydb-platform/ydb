SELECT
    key,
    subkey,
    value
FROM
    plato.Input
WHERE
    value ILIKE 'q_Z' OR value ILIKE '%Q'
;
