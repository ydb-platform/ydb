USE plato;

SELECT
    *
FROM
    InputInt8
WHERE
    value < -4 AND value > 10
;

SELECT
    *
FROM
    InputInt16
WHERE
    value < -4 AND value > 10
;

SELECT
    *
FROM
    InputInt32
WHERE
    value < -4 AND value > 10
;

SELECT
    *
FROM
    InputInt64
WHERE
    value < -4 AND value > 10
;

SELECT
    *
FROM
    InputUint8
WHERE
    value < 4 AND value > 10
;

SELECT
    *
FROM
    InputUint16
WHERE
    value < 4 AND value > 10
;

SELECT
    *
FROM
    InputUint32
WHERE
    value < 4 AND value > 10
;

SELECT
    *
FROM
    InputUint64
WHERE
    value < 4 AND value > 10
;

SELECT
    *
FROM
    InputFloat
WHERE
    value < 4.1 AND value > 10.5
;

SELECT
    *
FROM
    InputDouble
WHERE
    value < 4.1 AND value > 10.5
;

SELECT
    *
FROM
    InputString
WHERE
    value < 'a' AND value > 'c'
;

SELECT
    *
FROM
    InputOptString
WHERE
    value < 'a' AND value > 'c'
;
