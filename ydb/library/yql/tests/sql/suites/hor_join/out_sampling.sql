/* postgres can not */
/* kikimr can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) == 10 and len(yt_res_yson[1]['Write'][0]['Data']) < 10 and len(yt_res_yson[2]['Write'][0]['Data']) < 10 and len(yt_res_yson[3]['Write'][0]['Data']) == 10 */
USE plato;

SELECT key, some(value) FROM Input GROUP BY key;

SELECT key, sum(cast(subkey as Int32)) FROM Input SAMPLE 0.3 GROUP BY key;

SELECT key, some(subkey) FROM Input SAMPLE 0.3 GROUP BY key;

SELECT key, sum(length(value)) FROM Input GROUP BY key;
