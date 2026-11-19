$rows = [
    <|ftztimestamp: Just(TzTimestamp("1970-01-01T00:00:00,GMT"))|>,
    <|ftztimestamp: Just(TzTimestamp("1970-01-01T00:00:00,UTC"))|>,
    <|ftztimestamp: Just(TzTimestamp("2018-12-01T01:02:03.456789,Europe/Moscow"))|>,
    <|ftztimestamp: Just(TzTimestamp("2011-03-13T02:15:00,America/Los_Angeles"))|>,
    <|ftztimestamp: Just(TzTimestamp("2011-11-06T01:15:00,America/Los_Angeles"))|>,
];

$format = DateTime::Format("%% year %Y monthFullName %B monthShortName %b month %m day %d hours %H minutes %M seconds %S tz %z tzname %Z text");

select
    $format(DateTime::Split(ftztimestamp))
FROM AS_TABLE($rows)
