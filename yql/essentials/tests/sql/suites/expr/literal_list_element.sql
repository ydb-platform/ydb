/* syntax version 1 */
/* postgres can not */
$list = YQL::@@(AsList (String 'z) (String 'a))@@;
SELECT $list[0];
