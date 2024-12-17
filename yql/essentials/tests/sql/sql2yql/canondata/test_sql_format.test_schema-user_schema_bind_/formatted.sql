/* syntax version 1 */
USE plato;

$table = 'In' || 'put';

SELECT
    *
FROM
    $table WITH SCHEMA Struct<a: Int64?>
;
