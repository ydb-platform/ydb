/* syntax version 1 */
/* postgres can not */

USE plato;

$t = [<|"key":"1", "subkey":"a", "value":"value2_1"|>, <|"key":"4", "subkey":"d", "value":"value2_4"|>, <|"key":"-5", "subkey":"e", "value":"value2_5"|>];

SELECT *
FROM
    Input AS A
LEFT ONLY JOIN
    AS_TABLE($t) AS B
ON
    AsTuple(cast(A.key as uint64), cast(A.subkey as String)) = AsTuple(cast(B.key as int64), cast(B.subkey as Utf8))
ORDER BY `key`;
