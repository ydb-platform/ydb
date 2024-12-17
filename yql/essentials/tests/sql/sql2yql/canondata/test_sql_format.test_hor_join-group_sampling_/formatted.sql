/* postgres can not */
/* kikimr can not */
/* hybridfile can not YQL-17284 */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 16 */
USE plato;

SELECT
    *
FROM (
    SELECT
        CAST(key AS int) AS key,
        '' AS subkey,
        '' AS value
    FROM
        plato.Input1
        SAMPLE 0.1
    UNION ALL
    SELECT
        CAST(key AS int) AS key,
        subkey,
        '' AS value
    FROM
        plato.Input2
        SAMPLE 0.1
    UNION ALL
    SELECT
        1 AS key,
        subkey,
        '' AS value
    FROM
        plato.Input3
    UNION ALL
    SELECT
        1 AS key,
        '' AS subkey,
        value
    FROM
        plato.Input4
) AS x
ORDER BY
    key,
    subkey,
    value
;
