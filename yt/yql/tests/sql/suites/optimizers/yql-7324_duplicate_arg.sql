/* postgres can not */
/* syntax version 1 */
USE plato;

$to_int = ($x) -> { return cast($x as Int32) };
$to_in_list = ($col) -> { return ListMap(String::SplitToList($col, ","), $to_int) };

$input = (
    SELECT
        $to_in_list(key) AS event_ids,
        $to_in_list(subkey) AS test_ids
    FROM
        Input
    WHERE
        value = "aaa"
);

SELECT
    event_id,
    test_ids
FROM
    $input
FLATTEN BY event_ids AS event_id;
