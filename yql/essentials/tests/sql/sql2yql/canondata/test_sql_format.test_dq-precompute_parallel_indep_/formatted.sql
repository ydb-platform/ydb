USE plato;

INSERT INTO Output
SELECT
    sum(CAST(key AS int32))
FROM Input1;

INSERT INTO Output
SELECT
    sum(CAST(key AS int32))
FROM Input2;

INSERT INTO Output
SELECT
    sum(CAST(key AS int32))
FROM Input3;
