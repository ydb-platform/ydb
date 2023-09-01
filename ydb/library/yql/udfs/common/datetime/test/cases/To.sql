/* syntax version 0 */
$value = CAST(CAST(value AS Uint64) AS Timestamp);
$half_value = CAST(CAST(value AS Uint64) / 2 AS Timestamp);

SELECT
    DateTime::ToHours($value) AS hours,
    DateTime::ToSecondsFloat($half_value) AS timestamp,
    DateTime::IntervalToSeconds($value - $half_value) AS interval,
    DateTime::IntervalToMicroSeconds($half_value - $value) AS negative_interval,
    DateTime::IntervalToSecondsFloat($half_value - $value) AS negative_interval_double
FROM Input;

