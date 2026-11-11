$rows = [
    <|fdate: Just(Date("2018-11-22")), finterval: Just(Interval("P2DT10H"))|>,
    <|fdate: Just(Date("2018-11-22")), finterval: Just(Interval("P25000D"))|>,
];

select
    DateTime::ToSeconds(fdate) as date_to_seconds,
    DateTime::ToSeconds(finterval) as interval_to_seconds
FROM AS_TABLE($rows);
