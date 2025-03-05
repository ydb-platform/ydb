USE plato;

SELECT
    key,
    sum(1u/(4u-subkey)),
    sum(subkey),
    sum(1u),
    sum(1u/0u)
FROM Input
GROUP by key
ORDER by key