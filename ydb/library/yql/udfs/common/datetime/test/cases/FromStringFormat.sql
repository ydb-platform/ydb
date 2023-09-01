/* syntax version 0 */
SELECT
    CAST(DateTime::TimestampFromStringFormat(value, '%Y%m') AS String),
    CAST(DateTime::TimestampFromStringFormat(value, '%Y') as String)
FROM Input;
