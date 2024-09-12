/* syntax version 1 */
/* postgres can not */
/* ytfile can not */
/* yt can not */

$input = SELECT * FROM AS_TABLE([
    <|"time":"2024-01-01T00:00:01Z"|>,
    <|"time":"2024-01-02T00:00:01Z"|>,
    <|"time":"2024-01-03T00:00:01Z"|>
]);

SELECT
    COUNT(*),
    -- HOP_START()
FROM $input
GROUP BY HOP(CAST(time as Timestamp), 'PT60S', 'PT86400S', 'PT60S')
