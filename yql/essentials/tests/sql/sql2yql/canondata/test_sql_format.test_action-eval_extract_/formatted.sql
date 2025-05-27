/* syntax version 1 */
/* postgres can not */
$list = AsList(AsStruct(1 AS One, '2' AS Two), AsStruct(3 AS One, '4' AS Two));

SELECT
    ListExtract($list, CAST(Unicode::ToUpper("o"u) AS String) || 'ne')
;
