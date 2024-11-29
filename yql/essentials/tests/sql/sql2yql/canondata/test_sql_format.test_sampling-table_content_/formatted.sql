/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) == 1 */
USE plato;

$key = (
    SELECT
        key
    FROM plato.Input
        SAMPLE (0.5)
);

SELECT
    *
FROM Input
WHERE key == $key;
