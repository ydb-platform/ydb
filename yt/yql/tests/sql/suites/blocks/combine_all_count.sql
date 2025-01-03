USE plato;

SELECT
    count(*),
    count(key + 1u/(4u-subkey)),
    count(key),
    count(1u),
    count(1u/0u)
FROM Input