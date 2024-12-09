/* syntax version 1 */
/* postgres can not */
USE plato;
PRAGMA yt.UseNativeDescSort;

SELECT
    key,
    subkey
FROM Input1 -- YtReduce
GROUP COMPACT BY
    key,
    subkey
ORDER BY
    key,
    subkey;

SELECT
    key,
    subkey
FROM Input1 -- YtReduce
GROUP COMPACT BY
    subkey,
    key
ORDER BY
    subkey,
    key;

SELECT
    key
FROM Input1 -- YtReduce
GROUP COMPACT BY
    key
ORDER BY
    key;

SELECT
    subkey
FROM Input1 -- YtMapReduce
GROUP COMPACT BY
    subkey
ORDER BY
    subkey;

SELECT
    key,
    subkey
FROM concat(Input1, Input2) -- YtMapReduce, mix of ascending/descending
GROUP COMPACT BY
    key,
    subkey
ORDER BY
    key,
    subkey;
