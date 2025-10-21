/* postgres can not */
/* kikimr can not */
/* custom check: len(yt_res_yson[0][b'Write'][0][b'Data']) == 10 and len(yt_res_yson[1][b'Write'][0][b'Data']) < 10 and len(yt_res_yson[2][b'Write'][0][b'Data']) < 10 and len(yt_res_yson[3][b'Write'][0][b'Data']) == 10 */
USE plato;

SELECT key, some(value) FROM Input GROUP BY key;

SELECT key, sum(cast(subkey as Int32)) FROM Input SAMPLE 0.3 GROUP BY key;

SELECT key, some(subkey) FROM Input SAMPLE 0.3 GROUP BY key;

SELECT key, sum(length(value)) FROM Input GROUP BY key;
