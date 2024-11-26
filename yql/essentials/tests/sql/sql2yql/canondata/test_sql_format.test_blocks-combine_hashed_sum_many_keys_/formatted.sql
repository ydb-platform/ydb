USE plato;

SELECT
    k1,
    sum(subkey)
FROM Input
GROUP BY
    key + 1u AS k1
ORDER BY
    k1;

SELECT
    k1,
    k2,
    sum(subkey)
FROM Input
GROUP BY
    key + 1u AS k1,
    key + 2u AS k2
ORDER BY
    k1,
    k2;

SELECT
    k1,
    k2,
    k3,
    sum(subkey)
FROM Input
GROUP BY
    key + 1u AS k1,
    key + 2u AS k2,
    key + 3u AS k3
ORDER BY
    k1,
    k2,
    k3;

SELECT
    k1,
    k2,
    k3,
    k4,
    sum(subkey)
FROM Input
GROUP BY
    key + 1u AS k1,
    key + 2u AS k2,
    key + 3u AS k3,
    key + 4u AS k4
ORDER BY
    k1,
    k2,
    k3,
    k4;

SELECT
    k1,
    k2,
    k3,
    k4,
    k5,
    sum(subkey)
FROM Input
GROUP BY
    key + 1u AS k1,
    key + 2u AS k2,
    key + 3u AS k3,
    key + 4u AS k4,
    key + 5u AS k5
ORDER BY
    k1,
    k2,
    k3,
    k4,
    k5;
