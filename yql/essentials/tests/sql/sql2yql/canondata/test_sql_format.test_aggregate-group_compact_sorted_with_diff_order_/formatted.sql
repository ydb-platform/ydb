/* syntax version 1 */
/* postgres can not */
USE plato;

INSERT INTO @ksv
SELECT
    *
FROM
    Input
ORDER BY
    key,
    subkey,
    value
;

INSERT INTO @vsk
SELECT
    *
FROM
    Input
ORDER BY
    value,
    subkey,
    key
;

INSERT INTO @vs
SELECT
    *
FROM
    Input
ORDER BY
    value,
    subkey
;

COMMIT;

SELECT
    key,
    subkey,
    value
FROM
    @ksv -- YtReduce
GROUP COMPACT BY
    key,
    subkey,
    value
ORDER BY
    key,
    subkey,
    value
;

SELECT
    key,
    subkey,
    value
FROM
    @vsk -- YtReduce
GROUP /*+ compact() */ BY
    key,
    subkey,
    value
ORDER BY
    key,
    subkey,
    value
;

SELECT
    key,
    subkey,
    some(value) AS value
FROM
    @ksv -- YtReduce
GROUP COMPACT BY
    key,
    subkey
ORDER BY
    key,
    subkey,
    value
;

SELECT
    key,
    subkey,
    some(value) AS value
FROM
    @vsk -- YtMapReduce
GROUP COMPACT BY
    key,
    subkey
ORDER BY
    key,
    subkey,
    value
;

SELECT
    key,
    subkey,
    value
FROM
    concat(@ksv, @vsk) -- YtMapReduce
GROUP COMPACT BY
    key,
    subkey,
    value
ORDER BY
    key,
    subkey,
    value
;

SELECT
    some(key) AS key,
    subkey,
    value
FROM
    concat(@vs, @vsk) -- YtReduce
GROUP COMPACT BY
    subkey,
    value
ORDER BY
    key,
    subkey,
    value
;
