/* syntax version 1 */
declare $x1 as int;
declare $x2 as Double;
declare $x3 as Bool;
declare $x4 as Float;
declare $x5 as String; -- unicode
declare $x6 as Int64;
declare $x7 as Uint64;
declare $x8 as String; -- binary
declare $x9 as Utf8;
declare $x10 as Uuid;
declare $x11 as String?; -- null
declare $x12 as String?; -- not null
declare $x13 as Yson;
declare $x14 as Json;
declare $x15 as datetime;
declare $x16 as date;
declare $x17 as timestamp;
declare $x18 as interval;
declare $x19 as tzdatetime;
declare $x20 as tztimestamp;
declare $x21 as tzdate;
declare $x22 as Void;
declare $x23 as Decimal(21,8);
select 2 * $x1, -$x2 * 10, not $x3, -$x4, $x5 || "2018", -$x6, $x7 + 1, Length($x8),
    Unicode::GetLength($x9), cast($x10 as string), coalesce($x11, "XYZ"), coalesce($x12, "XYZ"),
    Yson::LookupInt64($x13, 'a'), Yson::ConvertToInt64($x14.b),
    cast($x15 as string), cast($x16 as string), cast($x17 as string), cast($x18 as string),
    cast($x19 as string), cast($x20 as string), cast($x21 as string),
    $x22, cast($x23 as string);
