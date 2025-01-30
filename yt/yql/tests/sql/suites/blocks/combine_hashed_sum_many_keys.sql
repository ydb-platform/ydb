USE plato;

SELECT
    k1,
    sum(subkey)
FROM Input
GROUP by key + 1u as k1
ORDER by k1;

SELECT
    k1,
    k2,
    sum(subkey)
FROM Input
GROUP by key + 1u as k1, key + 2u as k2
ORDER by k1, k2;

SELECT
    k1,
    k2,
    k3,
    sum(subkey)
FROM Input
GROUP by key + 1u as k1, key + 2u as k2, key + 3u as k3
ORDER by k1, k2, k3;

SELECT
    k1,
    k2,
    k3,
    k4,
    sum(subkey)
FROM Input
GROUP by key + 1u as k1, key + 2u as k2, key + 3u as k3, key + 4u as k4
ORDER by k1, k2, k3, k4;

SELECT
    k1,
    k2,
    k3,
    k4,
    k5,
    sum(subkey)
FROM Input
GROUP by key + 1u as k1, key + 2u as k2, key + 3u as k3, key + 4u as k4, key + 5u as k5
ORDER by k1, k2, k3, k4, k5;
