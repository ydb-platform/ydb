--------------------
-- Explicit casts --
--------------------
$strs = AsList('-1', '0', '1', '2.4', '99.77', '999.5', '-7777');

$data = ListMap(
    $strs, ($str) -> {
        RETURN AsStruct($str AS str);
    }
);

SELECT
    dataset.*,

    -- Changing range
    CAST(dec_3_1 AS Decimal (5, 1)) AS upcast_3_1,
    CAST(dec_5_2 AS Decimal (4, 2)) AS downcast_5_2,

    -- Changing precision
    CAST(dec_5_2 AS Decimal (3, 0)) AS rounded_5_2,
    CAST(dec_7_0 AS Decimal (9, 2)) AS extended_7_0,

    -- Combinations
    CAST(dec_5_2 AS Decimal (6, 0)) AS upcast_rounded_5_2,
    CAST(dec_3_1 AS Decimal (7, 3)) AS upcast_extended_3_1,
    CAST(dec_5_2 AS Decimal (3, 1)) AS downcast_rounded_5_2,
    CAST(dec_7_0 AS Decimal (5, 2)) AS downcast_extended_7_0
FROM (
    SELECT
        str AS a_str,
        CAST(str AS Decimal (3, 1)) AS dec_3_1,
        CAST(str AS Decimal (5, 2)) AS dec_5_2,
        CAST(str AS Decimal (7, 0)) AS dec_7_0
    FROM
        AS_TABLE($data)
) AS dataset;

--------------------
-- Implicit  cast --
--------------------
$lambda = ($big_dec) -> {
    RETURN $big_dec;
};

$func = Callable(Callable<(Decimal (10, 5)) -> Decimal (10, 5)>, $lambda);

$small_dec = Decimal('3.0', 2, 1);

SELECT
    $func($small_dec)
;
