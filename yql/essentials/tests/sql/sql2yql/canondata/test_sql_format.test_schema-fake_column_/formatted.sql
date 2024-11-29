/* postgres can not */
USE plato;

INSERT INTO @tmp
SELECT
    *
FROM (
    SELECT
        <||> AS a
)
    FLATTEN COLUMNS;
COMMIT;

SELECT
    1 AS a
FROM @tmp;
