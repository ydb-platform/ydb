/* syntax version 1 */
/* postgres can not */
$ages = [
    <|suffix: "0-0.5"u, begin:  0.f, end: 0.5f|>,
    <|suffix: "0.5-1"u, begin: 0.5f, end:  1.f|>,
    <|suffix: "1-3"u  , begin:  1.f, end:  3.f|>,
    <|suffix: "3-5"u  , begin:  3.f, end:  5.f|>,
    <|suffix: "5-7"u  , begin:  5.f, end:  7.f|>,
    <|suffix: "7-9"u  , begin:  7.f, end:  9.f|>,
    <|suffix: "9-12"u , begin:  9.f, end: 12.f|>,
    <|suffix: "12-14"u, begin: 12.f, end: 14.f|>,
    <|suffix: "14-16"u, begin: 14.f, end: 16.f|>,
    <|suffix: "16+"u  , begin: 16.f, end: 18.f|>,
];

$interval_fits_in = ($interval, $other) -> {
    $length = $interval.end - $interval.begin;
    RETURN IF(
        $interval.end <= $other.begin OR $interval.begin >= $other.end,
        0.f,
        IF(
            $interval.begin >= $other.begin AND $interval.end <= $other.end,  -- interval is completely within other
            1.f,
            IF(
                $interval.begin <= $other.begin AND $interval.end >= $other.end,  -- other is completely within the interval
                ($other.end - $other.begin) / $length,
                IF(
                    $interval.begin < $other.begin,
                    ($interval.end - $other.begin) / $length,
                    ($other.end - $interval.begin) / $length
                )
            )
        )
    );
};

$age_suffixes = ($interval, $age_segments) -> {
    RETURN IF(
        $interval.end - $interval.begin > 10.f OR $interval.end - $interval.begin < 1e-4f,
        [NULL],
        ListFilter(
            ListMap(
                $age_segments,
                ($i) -> {
                    RETURN <|age_suffix: ":Age:"u || $i.suffix, age_weight: $interval_fits_in($interval, $i)|>
                }
            ),
            ($i) -> {
                RETURN $i.age_weight > 1e-4f;
            }
        )
    );
};

$data = (
    SELECT
        *
    FROM
    (
        SELECT
            puid,
            ts,
            boys ?? False AS boys,
            girls ?? False AS girls,
            min_age ?? 0.f AS min_age,
            max_age ?? 18.f AS max_age
        FROM

        AS_TABLE([
            <|puid: 1, ts: 123, boys: True, girls: False, min_age: 1.f, max_age: 2.f|>,
            <|puid: 2, ts: 123, boys: True, girls: False, min_age: NULL, max_age: NULL|>,
            <|puid: 3, ts: 123, boys: NULL, girls: NULL, min_age: 1.f, max_age: 2.f|>,
            <|puid: 4, ts: 123, boys: True, girls: True, min_age: 1.f, max_age: 2.f|>,
            <|puid: 5, ts: 123, boys: True, girls: True, min_age: 1.f, max_age: 5.f|>,
            <|puid: 6, ts: 123, boys: True, girls: False, min_age: 1.f, max_age: 2.f|>,
        ])

    )
    WHERE boys OR girls OR min_age > 0.f OR max_age < 18.f
);

SELECT
    puid,
    $age_suffixes(<|begin: min_age, end: max_age|>, $ages) AS age_suffixes,
    <|begin: min_age, end: max_age|> as interval
FROM $data;
