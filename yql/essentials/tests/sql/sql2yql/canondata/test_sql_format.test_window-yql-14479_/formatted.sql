/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) < 4 */
/* ignore plan diff */
USE plato;

SELECT
    a.*,
    row_number() OVER (
        PARTITION BY
            key,
            subkey
    ) AS rn,
    TableName() AS tn
FROM Input
    AS a
    TABLESAMPLE BERNOULLI (10.0) REPEATABLE (1);
