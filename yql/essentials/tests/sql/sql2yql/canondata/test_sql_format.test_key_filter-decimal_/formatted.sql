/* postgres can not */
/* syntax version 1 */
USE plato;

$asIs = Python::asIs(
    Callable<(String) -> String>,
    @@
def asIs(arg):
    return arg
@@
);

INSERT INTO @decimal
SELECT
    CAST(value AS Decimal (15, 10)) AS value
FROM
    Input
ORDER BY
    value
;

COMMIT;

SELECT
    *
FROM (
    SELECT
        *
    FROM
        @decimal
    WHERE
        value < Decimal('4.1', 15, 10) AND value > Decimal('10.5', 15, 10) -- empty
    UNION ALL
    SELECT
        *
    FROM
        @decimal
    WHERE
        value > Decimal('inf', 15, 10) -- empty
    UNION ALL
    SELECT
        *
    FROM
        @decimal
    WHERE
        value < Decimal('-inf', 15, 10) -- empty
    UNION ALL
    SELECT
        *
    FROM
        @decimal
    WHERE
        value == Decimal('nan', 15, 10) -- empty
    UNION ALL
    SELECT
        *
    FROM
        @decimal
    WHERE
        value == Decimal('inf', 15, 10)
    UNION ALL
    SELECT
        *
    FROM
        @decimal
    WHERE
        value == Decimal('-inf', 15, 10)
    UNION ALL
    SELECT
        *
    FROM
        @decimal
    WHERE
        value > Decimal('3.3', 15, 10) OR value >= Decimal('3.30001', 15, 10)
)
ORDER BY
    value
;

-- Don't union all to calc nodes separatelly
SELECT
    *
FROM
    @decimal
WHERE
    value == CAST('6.6' AS Decimal (15, 10))
; -- Safe key filter calc

SELECT
    *
FROM
    @decimal
WHERE
    value == CAST($asIs('3.3') AS Decimal (15, 10))
; -- Unsafe key filter calc

SELECT
    *
FROM
    @decimal
WHERE
    value == CAST($asIs('bad') AS Decimal (15, 10))
; -- Unsafe key filter calc
