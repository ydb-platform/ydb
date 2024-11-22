/* syntax version 1 */
/* postgres can not */
SELECT
    Yson::ConvertTo("[1,2]"j, ParseType("List<Int32>")) as x
FROM AS_TABLE([<|key:1|>, <|key:2|>])
GROUP BY key;

