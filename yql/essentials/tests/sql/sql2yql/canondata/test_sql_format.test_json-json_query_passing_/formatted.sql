/* syntax version 1 */
/* postgres can not */
$json = CAST(
    @@{
    "key": 123
}@@ AS Json
);

-- Check all supported types for variables
SELECT
    -- Numeric types
    JSON_QUERY (
        $json,
        'strict $var1 + $var2 + $var3 + $var4 + $var5 + $var6 + $var7 + $var8 + $var9 + $var10' PASSING CAST(1 AS Int8) AS var1,
        CAST(2 AS Uint8) AS var2,
        CAST(3 AS Int16) AS var3,
        CAST(4 AS Uint16) AS var4,
        CAST(5 AS Int32) AS var5,
        CAST(6 AS Uint32) AS var6,
        CAST(7 AS Int64) AS var7,
        CAST(8 AS Uint64) AS var8,
        CAST(9 AS Double) AS var9,
        CAST(10 AS Float) AS var10 WITH UNCONDITIONAL ARRAY WRAPPER
    ),

    -- Time types
    JSON_QUERY (
        $json,
        'strict $var' PASSING CAST(1582044622 AS Datetime) AS var WITH UNCONDITIONAL ARRAY WRAPPER
    ),
    JSON_QUERY (
        $json,
        'strict $var' PASSING CAST(1582044622 AS Timestamp) AS var WITH UNCONDITIONAL ARRAY WRAPPER
    ),
    JSON_QUERY (
        $json,
        'strict $var' PASSING CAST('2020-02-18' AS Date) AS var WITH UNCONDITIONAL ARRAY WRAPPER
    ),

    -- Utf8
    JSON_QUERY (
        $json,
        'strict $var' PASSING CAST('привет' AS Utf8) AS var WITH UNCONDITIONAL ARRAY WRAPPER
    ),

    -- Bool
    JSON_QUERY (
        $json,
        'strict $var' PASSING TRUE AS var WITH UNCONDITIONAL ARRAY WRAPPER
    ),

    -- Json
    JSON_QUERY (
        $json,
        'strict $var' PASSING $json AS var
    ),

    -- Nulls
    JSON_QUERY (
        $json,
        'strict $var' PASSING Nothing(Int64?) AS var WITH UNCONDITIONAL ARRAY WRAPPER
    ),
    JSON_QUERY (
        $json,
        'strict $var' PASSING NULL AS var WITH UNCONDITIONAL ARRAY WRAPPER
    )
;

-- Check various ways to pass variable name
SELECT
    JSON_QUERY (
        $json, 'strict $var1' PASSING 123 AS var1 WITH UNCONDITIONAL ARRAY WRAPPER
    ),

    -- NOTE: VaR1 is not casted to upper-case VAR1 as standard expects
    JSON_QUERY (
        $json, 'strict $VaR1' PASSING 123 AS VaR1 WITH UNCONDITIONAL ARRAY WRAPPER
    ),
    JSON_QUERY (
        $json, 'strict $var1' PASSING 123 AS 'var1' WITH UNCONDITIONAL ARRAY WRAPPER
    ),
    JSON_QUERY (
        $json, 'strict $VaR1' PASSING 123 AS 'VaR1' WITH UNCONDITIONAL ARRAY WRAPPER
    )
;
