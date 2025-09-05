USE plato;

$src =
SELECT
    if (key != '075', AsTuple(Just(Just(key)), 123)) as k,
    subkey,
    value
FROM Input;

SELECT
    k,
    min(subkey) as min,
    max(value)  as max,
FROM $src
GROUP by k
ORDER by k
