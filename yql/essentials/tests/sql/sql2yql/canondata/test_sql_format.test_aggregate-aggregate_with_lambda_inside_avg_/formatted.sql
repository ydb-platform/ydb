/* syntax version 1 */
/* postgres can not */
USE plato;
$cast_to_double = ($column) -> {
    RETURN CAST($column AS Double);
};
$column_name = 'key';

SELECT
    AVG($cast_to_double($column_name))
FROM
    Input
;
