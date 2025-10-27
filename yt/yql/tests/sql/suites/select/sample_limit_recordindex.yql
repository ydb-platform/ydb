/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) <= 5 */
USE plato;

SELECT
    key,
    subkey,
    TableRecordIndex() AS index
FROM
    Input
SAMPLE 1.0 / 5
LIMIT 5
;
