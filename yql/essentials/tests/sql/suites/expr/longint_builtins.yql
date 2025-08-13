/* postgres can not */
-- not supported on windows
$value = "1000000000000";
$negative = -1000000000000;
$longint = YQL::StrictFromString($value, AsAtom("Decimal"), AsAtom("32"), AsAtom("0"));
$negative_longint = CAST($negative AS Decimal(32,0));
$add = $longint + CAST("1111111111111111111111111111111" AS Decimal(32,0));
$div = $longint / CAST(1111 AS Decimal(32,0));
$mod = $longint % CAST(1111 AS Decimal(32,0));
$mul = $longint * CAST(333333333333333333 AS Decimal(32,0));
$sub = $longint - CAST("1111111111111111111111111111111" AS Decimal(32,0));
SELECT 
    $longint AS binary,
    YQL::ToString(YQL::Dec($longint)) AS to_string,
    CAST(YQL::Inc(Abs($negative_longint)) AS String) AS abs,
    CAST(YQL::Minus($add) AS String) AS add,
    CAST($div AS String) AS div,
    CAST($mod AS String) AS mod,
    CAST($mul AS String) AS mul,
    CAST($sub AS String) AS sub,
    $longint == YQL::Abs($negative_longint) AS eq,
    $div <= $longint AS cmp;
