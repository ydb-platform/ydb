/* syntax version 1 */
USE plato;
PRAGMA yt.InferSchema;

SELECT
    ListSort(DictItems(_other)) AS _other,
    key,
    subkey
FROM
    Input WITH COLUMNS Struct<key: Utf8, subkey: String?>
;
