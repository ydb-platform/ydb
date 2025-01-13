/* syntax version 1 */
DECLARE $x1 AS int;
DECLARE $x2 AS Double;
DECLARE $x3 AS Bool;
DECLARE $x4 AS Float;
DECLARE $x5 AS String; -- unicode
DECLARE $x6 AS Int64;
DECLARE $x7 AS Uint64;
DECLARE $x8 AS String; -- binary
DECLARE $x9 AS Utf8;
DECLARE $x10 AS Uuid;
DECLARE $x11 AS String?; -- null
DECLARE $x12 AS String?; -- not null
DECLARE $x13 AS Yson;
DECLARE $x14 AS Json;
DECLARE $x15 AS datetime;
DECLARE $x16 AS date;
DECLARE $x17 AS timestamp;
DECLARE $x18 AS interval;
DECLARE $x19 AS tzdatetime;
DECLARE $x20 AS tztimestamp;
DECLARE $x21 AS tzdate;
DECLARE $x22 AS Void;
DECLARE $x23 AS Decimal (21, 8);

SELECT
    2 * $x1,
    -$x2 * 10,
    NOT $x3,
    -$x4,
    $x5 || '2018',
    -$x6,
    $x7 + 1,
    Length($x8),
    Unicode::GetLength($x9),
    CAST($x10 AS string),
    coalesce($x11, 'XYZ'),
    coalesce($x12, 'XYZ'),
    Yson::LookupInt64($x13, 'a'),
    Yson::ConvertToInt64($x14.b),
    CAST($x15 AS string),
    CAST($x16 AS string),
    CAST($x17 AS string),
    CAST($x18 AS string),
    CAST($x19 AS string),
    CAST($x20 AS string),
    CAST($x21 AS string),
    $x22,
    CAST($x23 AS string)
;
