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

INSERT INTO @uuid
SELECT
    CAST(value AS Uuid) AS value
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
        @uuid
    WHERE
        value < Uuid('00000000-0000-0000-0000-100000000000') AND value > Uuid('00000000-0000-0000-0000-400000000000') -- empty
    UNION ALL
    SELECT
        *
    FROM
        @uuid
    WHERE
        value > Uuid('FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF') -- empty
    UNION ALL
    SELECT
        *
    FROM
        @uuid
    WHERE
        value < Uuid('00000000-0000-0000-0000-000000000000') -- empty
    UNION ALL
    SELECT
        *
    FROM
        @uuid
    WHERE
        value > Uuid('00000000-0000-0000-0000-100000000000') OR value >= Uuid('00000000-0000-0000-0000-200000000000') --(00000000-0000-0000-0000-100000000000,)
    UNION ALL
    SELECT
        *
    FROM
        @uuid
    WHERE
        value >= Uuid('00000000-0000-0000-0000-100000000000') OR value > Uuid('00000000-0000-0000-0000-200000000000') --[00000000-0000-0000-0000-100000000000,)
    UNION ALL
    SELECT
        *
    FROM
        @uuid
    WHERE
        value == Uuid('00000000-0000-0000-0000-100000000000') OR value < Uuid('00000000-0000-0000-0000-200000000000') --(,00000000-0000-0000-0000-200000000000)
    UNION ALL
    SELECT
        *
    FROM
        @uuid
    WHERE
        value < Uuid('00000000-0000-0000-0000-100000000000') OR value <= Uuid('00000000-0000-0000-0000-200000000000') --(,00000000-0000-0000-0000-200000000000]
    UNION ALL
    SELECT
        *
    FROM
        @uuid
    WHERE
        value < Uuid('00000000-0000-0000-0000-100000000000') OR value <= Uuid('00000000-0000-0000-0000-200000000000') --(,00000000-0000-0000-0000-200000000000]
    UNION ALL
    SELECT
        *
    FROM
        @uuid
    WHERE
        value > Uuid('00000000-0000-0000-0000-100000000000') AND value <= Uuid('00000000-0000-0000-0000-400000000000') --(00000000-0000-0000-0000-100000000000,00000000-0000-0000-0000-400000000000]
)
ORDER BY
    value
;

-- Don't union all to calc nodes separatelly
SELECT
    *
FROM
    @uuid
WHERE
    value == CAST('00000000-0000-0000-0000-100000000000' AS Uuid)
; -- Safe key filter calc

SELECT
    *
FROM
    @uuid
WHERE
    value == CAST($asIs('00000000-0000-0000-0000-200000000000') AS Uuid)
; -- Unsafe key filter calc

SELECT
    *
FROM
    @uuid
WHERE
    value == CAST($asIs('bad') AS Uuid)
; -- Unsafe key filter calc
