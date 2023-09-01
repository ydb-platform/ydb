/* syntax version 0 */
$value = CAST(value AS Uint64);
SELECT
    DateTime::FromHours($value) AS uint,
    DateTime::TimestampFromHours($value) AS timestamp,
    DateTime::IntervalFromHours($value) AS interval
FROM Input;

