/* postgres can not */
USE plato;

SELECT
    "input1-value1" AS src,
    key,
    WeakField(value1, "String", "funny") AS ozer
FROM
    Input1
UNION ALL
SELECT
    "input2-value1" AS src,
    key,
    WeakField(value1, "String", "funny") AS ozer
FROM
    Input2
UNION ALL
SELECT
    "input1-value2" AS src,
    key,
    WeakField(value2, "String") AS ozer
FROM
    Input1
UNION ALL
SELECT
    "input2-value2" AS src,
    key,
    WeakField(value2, "String") AS ozer
FROM
    Input2
;
