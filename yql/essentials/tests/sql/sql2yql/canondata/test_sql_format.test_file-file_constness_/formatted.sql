/* postgres can not */
/* syntax version 1 */
SELECT
    key,
    FileContent("keyid.lst") AS content,
    ListCollect(ParseFile('int32', "keyid.lst")) AS content_list,
FROM plato.Input
GROUP BY
    key
ORDER BY
    key;
