/* syntax version 1 */
/* postgres can not */
/* ytfile can not */
/* yt can not */

$input = SELECT * FROM AS_TABLE([
    <|"time":"2024-01-01T00:00:01Z", "user": 1|>,
    <|"time":"2024-01-01T00:00:02Z", "user": 1|>,
    <|"time":"2024-01-01T00:00:03Z", "user": 1|>,
    <|"time":"2024-01-01T00:00:01Z", "user": 2|>,
    <|"time":"2024-01-01T00:00:02Z", "user": 2|>,
    <|"time":"2024-01-01T00:00:03Z", "user": 2|>,
    <|"time":"2024-01-01T00:00:01Z", "user": 2|>,
    <|"time":"2024-01-01T00:00:02Z", "user": 2|>,
    <|"time":"2024-01-01T00:00:03Z", "user": 2|>,
    <|"time":"2024-01-01T00:00:01Z", "user": 3|>,
    <|"time":"2024-01-01T00:00:02Z", "user": 3|>,
    <|"time":"2024-01-01T00:00:03Z", "user": 3|>
]);

SELECT
    user,
    COUNT(*) as count,
    HOP_START() as start,
FROM $input
GROUP BY HOP(CAST(time as Timestamp), 'PT1S', 'PT1S', 'PT1S'), user;
