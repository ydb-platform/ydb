-- Cast all non-NULL interval inputs so invalid values produce NULL.
$rows = [
    <|finterval1: CAST("P0Y2M" AS Interval), finterval2: Nothing(Optional<Interval>), finterval3: CAST("P4DT5H" AS Interval), finterval4: CAST("P6Y7M" AS Interval)|>,
    <|finterval1: CAST("P20000Y4M" AS Interval), finterval2: CAST("PT6H12M" AS Interval), finterval3: Nothing(Optional<Interval>), finterval4: CAST("P9Y10M" AS Interval)|>,
    <|finterval1: Nothing(Optional<Interval>), finterval2: CAST("PT9H18M" AS Interval), finterval3: CAST("P100000D" AS Interval), finterval4: CAST("P12Y14M" AS Interval)|>,
    <|finterval1: CAST("P4Y8M" AS Interval), finterval2: Nothing(Optional<Interval>), finterval3: CAST("P13DT14H" AS Interval), finterval4: Nothing(Optional<Interval>)|>,
];

pragma UseBlocks;
select
    DateTime::ToDays(finterval1) as `interval1`,
    DateTime::ToDays(finterval2) as `interval2`,
    DateTime::ToDays(finterval3) as `interval3`,
    DateTime::ToDays(finterval4) as `interval4`
FROM AS_TABLE($rows);
