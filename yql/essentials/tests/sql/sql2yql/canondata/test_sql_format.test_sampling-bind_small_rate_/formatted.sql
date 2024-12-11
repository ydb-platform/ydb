/* syntax version 1 */
/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 2 */
$a =
    SELECT
        *
    FROM
        plato.Input
;

SELECT
    *
FROM
    $a
    TABLESAMPLE BERNOULLI (0.1)
;
