$rows = [
    <|fseconds: Just(1542844800l)|>,
    <|fseconds: Just(2160000000l)|>,
];

select
    DateTime::IntervalFromSeconds(fseconds) as interval_from_seconds
FROM AS_TABLE($rows);
