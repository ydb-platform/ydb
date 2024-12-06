SELECT
    key,
    subkey,
    value
FROM plato.Input
WHERE value LIKE "q_z" OR value LIKE "%q";
