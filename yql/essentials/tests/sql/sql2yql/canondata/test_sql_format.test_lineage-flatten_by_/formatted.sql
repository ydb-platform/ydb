USE plato;

$subquery1 =
    SELECT
        key,
        subkey,
        z
    FROM Input
        FLATTEN LIST BY value AS z;

$subquery2 =
    SELECT
        key,
        subkey,
        value AS z,
        value2
    FROM Input
        FLATTEN LIST BY (
            value,
            value2
        );

INSERT INTO @tmp1 WITH TRUNCATE
SELECT
    *
FROM $subquery1;

INSERT INTO @tmp2 WITH TRUNCATE
SELECT
    *
FROM $subquery2;
