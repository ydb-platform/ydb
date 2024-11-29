USE plato;

$src =
    SELECT
        if(key != '075', AsTuple(Just(Just(key)), 123)) AS k,
        subkey,
        value
    FROM Input;

SELECT
    k,
    min(subkey) AS min,
    max(value) AS max,
FROM $src
GROUP BY
    k
ORDER BY
    k;
