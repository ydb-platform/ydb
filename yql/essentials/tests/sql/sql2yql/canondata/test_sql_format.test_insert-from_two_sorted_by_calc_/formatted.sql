/* postgres can not */
USE plato;

INSERT INTO @f1
SELECT
    *
FROM Input
ORDER BY
    key || "1";

INSERT INTO @f2
SELECT
    *
FROM Input
ORDER BY
    key || "2";
COMMIT;

INSERT INTO Output
SELECT
    *
FROM (
    SELECT
        *
    FROM @f1
    UNION ALL
    SELECT
        *
    FROM @f2
);
