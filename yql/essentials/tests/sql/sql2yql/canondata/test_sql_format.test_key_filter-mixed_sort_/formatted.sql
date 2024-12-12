USE plato;

/* postgres can not */
SELECT
    *
FROM
    concat(Input, Input1, Input2, Input3)
WHERE
    key == "150" AND subkey == "1" AND value >= "aaa"
;

SELECT
    *
FROM
    concat(Input, Input1, Input2, Input3)
WHERE
    subkey == "1" AND value >= "aaa"
;
