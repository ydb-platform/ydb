/* postgres can not */
/* kikimr can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 12 */
USE plato;

SELECT * FROM (
    SELECT CAST(key AS int) as key, '' as subkey, '' as value FROM plato.Input1 SAMPLE 0.1
    UNION ALL
    SELECT 1 as key, subkey, '' as value from plato.Input2
    UNION ALL
    SELECT 1 as key, '' as subkey, value from plato.Input3
) AS x
ORDER BY key, subkey, value
;