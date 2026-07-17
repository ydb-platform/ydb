PRAGMA config.flags('DecimalCommonTypeConversionMode', 'with_common_type_fixup');

SELECT
    AsList(CAST(1 AS Decimal (4, 3)), CAST(2 AS Decimal (5, 4))),
    AsList(CAST(1 AS Decimal (4, 3)), CAST(2 AS Decimal (9, 2))),
    AsList(CAST(1 AS Decimal (33, 16)), CAST(2 AS Decimal (33, 18))),
    AsList(CAST(1 AS Decimal (30, 10)), CAST(2 AS Decimal (20, 15)))
;
