$rows = [
    <|ftztimestamp: Just(TzTimestamp("1969-12-31T23:00:00.000000,Canada/Central"))|>,
];

$format = DateTime::Format("%Y-%m-%d %H:%M:%S %Z");

select
    $format(DateTime::StartOfYear(ftztimestamp)),
    $format(DateTime::StartOfQuarter(ftztimestamp)),
    $format(DateTime::StartOfMonth(ftztimestamp)),
    $format(DateTime::StartOfWeek(ftztimestamp)),
    $format(DateTime::StartOfDay(ftztimestamp)),
    $format(DateTime::StartOf(ftztimestamp, Interval("PT13H"))),
    $format(DateTime::StartOf(ftztimestamp, Interval("PT4H"))),
    $format(DateTime::StartOf(ftztimestamp, Interval("PT15M"))),
    $format(DateTime::StartOf(ftztimestamp, Interval("PT20S"))),
    $format(DateTime::StartOf(ftztimestamp, Interval("PT7S"))),
    DateTime::TimeOfDay(ftztimestamp)
FROM AS_TABLE($rows);
