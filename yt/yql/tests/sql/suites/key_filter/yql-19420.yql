/* postgres can not */
USE plato;

PRAGMA yt.DropUnusedKeysFromKeyFilter="1";

$src = (
    SELECT
        key1,
        key2
    FROM RANGE("", "Input1", "Input2")
);

SELECT
    key2
FROM $src
WHERE key1 BETWEEN '2' AND '5';

SELECT
    key2
FROM $src;
