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
    JSON_EXISTS (
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
        CAST(10 AS Float) AS var10
    ),

    -- Time types
    JSON_EXISTS (
        $json,
        'strict $var' PASSING CAST(1582044622 AS Datetime) AS var
    ),
    JSON_EXISTS (
        $json,
        'strict $var' PASSING CAST(1582044622 AS Timestamp) AS var
    ),
    JSON_EXISTS (
        $json,
        'strict $var' PASSING CAST('2020-02-18' AS Date) AS var
    ),

    -- Utf8
    JSON_EXISTS (
        $json,
        'strict $var' PASSING CAST('привет' AS Utf8) AS var
    ),

    -- Bool
    JSON_EXISTS (
        $json,
        'strict $var' PASSING TRUE AS var
    ),

    -- Json
    JSON_EXISTS (
        $json,
        'strict $var' PASSING $json AS var
    ),

    -- Nulls
    JSON_EXISTS (
        $json,
        'strict $var' PASSING Nothing(Int64?) AS var
    ),
    JSON_EXISTS (
        $json,
        'strict $var' PASSING NULL AS var
    )
;

-- Check various ways to pass variable name
SELECT
    JSON_EXISTS (
        $json, 'strict $var1' PASSING 123 AS var1
    ),

    -- NOTE: VaR1 is not casted to upper-case VAR1 as standard expects
    JSON_EXISTS (
        $json, 'strict $VaR1' PASSING 123 AS VaR1
    ),
    JSON_EXISTS (
        $json, 'strict $var1' PASSING 123 AS 'var1'
    ),
    JSON_EXISTS (
        $json, 'strict $VaR1' PASSING 123 AS 'VaR1'
    )
;
