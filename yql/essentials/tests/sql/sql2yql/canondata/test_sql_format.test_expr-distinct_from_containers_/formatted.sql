/* syntax version 1 */
/* postgres can not */
$vt1 = ParseType('Variant<Int32,Int32?>');
$vt2 = ParseType('Variant<Int64,Null>');
$svt1 = ParseType('Variant<a:Int32,b:Int32?>');
$svt2 = ParseType('Variant<a:Int64,b:Null>');

SELECT
    (1, 2) IS NOT DISTINCT FROM (1, 2, 1 / 0), --true
    <|a: 1 / 0, b: Nothing(String?), c: 1|> IS NOT DISTINCT FROM <|c: 1u, d: 1u / 0u, e: Nothing(Utf8?)|>, --true
    [1, 2, NULL] IS NOT DISTINCT FROM [1, 2, just(1 / 0)], --false
    {1: NULL} IS DISTINCT FROM {1u: 2 / 0}, --false
    VARIANT (1 / 0, '1', $vt1) IS DISTINCT FROM VARIANT (NULL, '1', $vt2), --false
    VARIANT (1 / 0, 'b', $svt1) IS NOT DISTINCT FROM VARIANT (NULL, 'b', $svt2), --true
;
