/* syntax version 1 */
$format = DateTime::Format("%Y-%m-%d %H:%M:%S %Z");

select
    $format(DateTime::EndOfMonth(TzDateTime('2023-07-07T01:02:03,Europe/Moscow'))),
    $format(DateTime::EndOfMonth(Date('2023-08-08'))),
    $format(DateTime::EndOfMonth(Date('2023-09-09'))),
    $format(DateTime::EndOfMonth(Date('2023-02-02'))),
    $format(DateTime::EndOfMonth(Date('2024-02-02')))
into result `Normal cases`;

$tsMin = '1970-01-01T00:00:00.000000';
$tsMax = '2105-12-31T23:59:59.999999';
$tsBelow = '1969-12-31T23:59:59.999999';
$tsAbove = '2106-01-01T00:00:00.000000';

select $format(cast($tsMin || 'Z' as Timestamp))
    , $format(DateTime::EndOfMonth(cast($tsMin || 'Z' as Timestamp)))
    , $format(DateTime::EndOfMonth(cast($tsMin || ',Atlantic/Madeira' as Timestamp)))
into result `Minimal timestamp value`;

select $format(cast($tsMax || 'Z' as Timestamp))
    , $format(DateTime::EndOfMonth(cast($tsMax || 'Z' as Timestamp)))
    , $format(DateTime::EndOfMonth(cast('2105-12-12T00:00:00Z' as Timestamp)))
    , $format(DateTime::EndOfMonth(cast($tsMax || ',Atlantic/Azores' as Timestamp)))
into result `Maximum timestamp value`;

select $format(cast($tsBelow || ',Atlantic/Azores' as TzTimestamp))
    , $format(DateTime::EndOfMonth(cast($tsBelow || ',Atlantic/Azores' as TzTimestamp)))
into result `Timestamp below minimum`;

select $format(cast($tsAbove || ',Atlantic/Madeira' as TzTimestamp))
    , $format(DateTime::EndOfMonth(cast($tsAbove || ',Atlantic/Madeira' as TzTimestamp)))
into result `Timestamp above maximum`;

$check = ($arg) -> {
    return <|
        soyear:    DateTime::EndOfYear($arg),
        soquarter: DateTime::EndOfQuarter($arg),
        somonth:   DateTime::EndOfMonth($arg),
        soweek:    DateTime::EndOfWeek($arg),
        soday:     DateTime::EndOfDay($arg),
        sopt13h:   DateTime::EndOf($arg, Interval("PT13H")),
        sopt4h:    DateTime::EndOf($arg, Interval("PT4H")),
        sopt15m:   DateTime::EndOf($arg, Interval("PT15M")),
        sopt20s:   DateTime::EndOf($arg, Interval("PT20S")),
        sopt7s:    DateTime::EndOf($arg, Interval("PT7S")),
    |>
};

$typeDispatcher = ($row) -> {
    $tm = $row.tm;
    return <|
        explicit: $check(DateTime::Split($tm)),
        implicit: $check($tm),
    |>;
};

$input = SELECT CAST(ftztimestamp as TzTimestamp) as tm FROM Input;

PROCESS $input USING $typeDispatcher(TableRow()) into result `Other cases`;
