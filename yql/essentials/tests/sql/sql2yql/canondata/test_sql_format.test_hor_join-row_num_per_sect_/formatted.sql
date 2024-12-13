/* postgres can not */
/* kikimr can not */
/* syntax version 1 */
USE plato;

$with_row1 = (
    SELECT
        t.*,
        ROW_NUMBER() OVER () AS row_num
    FROM
        Input1 AS t
);

$with_row2 = (
    SELECT
        t.*,
        ROW_NUMBER() OVER () AS row_num
    FROM
        Input2 AS t
);

SELECT
    a.key AS key,
    b.subkey AS subkey,
    b.value AS value
FROM
    $with_row1 AS a
LEFT JOIN
    $with_row2 AS b
USING (row_num);
