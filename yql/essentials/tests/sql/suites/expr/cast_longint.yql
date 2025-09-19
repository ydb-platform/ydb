/* postgres can not */
$value = CAST(-7 AS Decimal(10, 0));
SELECT 
    $value AS binary,
    CAST($value AS String) AS to_string,
    CAST("+123" AS Decimal(10,0)) AS from_string,
    CAST("bad" AS Decimal(10,0)) AS bad_cast;
