/* syntax version 0 */
$result = DateTime::ToString(DateTime::FromString(value));

SELECT
    value,
    $result AS result
FROM Input;

SELECT
    DateTime::ToSeconds(
        DateTime::FromSeconds(123456789)
    ) AS seconds;
