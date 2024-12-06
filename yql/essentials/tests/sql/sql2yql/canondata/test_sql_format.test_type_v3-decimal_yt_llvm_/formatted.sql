/* syntax version 1 */
/* postgres can not */
USE plato;
PRAGMA yt.UseNativeYtTypes = "1";
PRAGMA config.flags("LLVM", "");

INSERT INTO @a
SELECT
    *
FROM (
    SELECT
        Decimal("3.14", 3, 2) AS d3,
        Decimal("2.9999999999", 12, 10) AS d12,
        Decimal("2.12345678900876543", 35, 10) AS d35
    UNION ALL
    SELECT
        Decimal("inf", 3, 2) AS d3,
        Decimal("inf", 12, 10) AS d12,
        Decimal("inf", 35, 10) AS d35
    UNION ALL
    SELECT
        Decimal("-inf", 3, 2) AS d3,
        Decimal("-inf", 12, 10) AS d12,
        Decimal("-inf", 35, 10) AS d35
    UNION ALL
    SELECT
        Decimal("nan", 3, 2) AS d3,
        Decimal("nan", 12, 10) AS d12,
        Decimal("nan", 35, 10) AS d35
);
COMMIT;

SELECT
    *
FROM
    @a
WHERE
    d3 != Decimal("5.3", 3, 2)
;

SELECT
    *
FROM
    Input
;
