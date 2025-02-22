USE plato;

SELECT
    key,
    count(*),
    count(1u/(4u-subkey)),
    count(subkey),
    count(1u),
    count(1u/0u)
FROM Input
GROUP by key
ORDER by key