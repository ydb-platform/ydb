$rows = [
    <|tztimestamp: Just(TzTimestamp("1970-01-01T05:00:00.000000,Europe/Moscow"))|>,
    <|tztimestamp: Just(TzTimestamp("2018-12-15T01:02:03.456789,Europe/Moscow"))|>,
    <|tztimestamp: Just(TzTimestamp("2105-12-31T16:23:45.000000,GMT"))|>,
    <|tztimestamp: Just(TzTimestamp("2106-01-01T01:00:00.000000,Europe/Moscow"))|>,
    <|tztimestamp: Just(TzTimestamp("2019-07-24T12:00:00,Europe/Moscow"))|>,
];

pragma UseBlocks;

select
    DateTime::StartOfYear(`tztimestamp`),

    DateTime::StartOfQuarter(`tztimestamp`),

    DateTime::StartOfMonth(`tztimestamp`),

    DateTime::StartOfWeek(`tztimestamp`),

    DateTime::StartOfDay(`tztimestamp`),

    DateTime::StartOf(`tztimestamp`, Interval("PT13H")),

    DateTime::StartOf(`tztimestamp`, Interval("PT4H")),
    DateTime::StartOf(`tztimestamp`, Interval("PT15M")),
    DateTime::StartOf(`tztimestamp`, Interval("PT20S")),
    DateTime::StartOf(`tztimestamp`, Interval("PT7S")),
    DateTime::TimeOfDay(`tztimestamp`)
FROM AS_TABLE($rows);
