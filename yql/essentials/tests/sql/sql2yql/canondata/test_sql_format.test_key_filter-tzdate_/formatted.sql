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

INSERT INTO @tzdate
SELECT
    CAST(value AS TzDate) AS value
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
        @tzdate
    WHERE
        value < TzDate('1999-01-01,Europe/Moscow') AND value > TzDate('2011-01-01,Europe/Moscow') -- empty
    UNION ALL
    SELECT
        *
    FROM
        @tzdate
    WHERE
        value > TzDate('2105-12-30,posixrules') -- empty
    UNION ALL
    SELECT
        *
    FROM
        @tzdate
    WHERE
        value < TzDate('1970-01-01,GMT') -- empty
    UNION ALL
    SELECT
        *
    FROM
        @tzdate
    WHERE
        value == TzDate('2018-02-01,GMT')
    UNION ALL
    SELECT
        *
    FROM
        @tzdate
    WHERE
        value > TzDate('1999-01-01,GMT') OR value >= TzDate('1999-01-01,Europe/Moscow')
    UNION ALL
    SELECT
        *
    FROM
        @tzdate
    WHERE
        value >= TzDate('2018-02-01,Europe/Moscow') AND value <= TzDate('2105-12-30,America/Los_Angeles') -- Should include 2018-02-01,GMT and 2105-12-31,posixrules
)
ORDER BY
    value
;

-- Don't union all to calc nodes separatelly
SELECT
    *
FROM
    @tzdate
WHERE
    value == CAST('1999-01-01,Europe/Moscow' AS TzDate)
; -- Safe key filter calc

SELECT
    *
FROM
    @tzdate
WHERE
    value == CAST($asIs('2105-12-30,America/Los_Angeles') AS TzDate)
; -- Unsafe key filter calc

SELECT
    *
FROM
    @tzdate
WHERE
    value == CAST($asIs('bad') AS TzDate)
; -- Unsafe key filter calc
