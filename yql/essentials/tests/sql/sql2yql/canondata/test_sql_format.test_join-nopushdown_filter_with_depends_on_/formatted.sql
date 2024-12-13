/* postgres can not */
/* hybridfile can not  */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 4 */
USE plato;

-- should not pushdown
SELECT
    *
FROM
    Input1 AS a
LEFT SEMI JOIN
    Input2 AS b
USING (key)
WHERE
    Random(TableRow()) < 0.1
ORDER BY
    key
;
