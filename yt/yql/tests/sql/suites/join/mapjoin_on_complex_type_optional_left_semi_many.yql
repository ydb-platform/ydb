/* syntax version 1 */
/* postgres can not */

USE plato;

$t = [<|"key1":"1", "subkey1":"a", "key":"1", "subkey":"a", "value":"value2_1"|>, <|"key1":"4", "subkey1":"d", "key":"4", "subkey":"d", "value":"value2_4"|>, <|"key1":"-5", "subkey1":"e", "key":"-5", "subkey":"e", "value":"value2_5"|>];

SELECT *
FROM
    Input AS A
LEFT SEMI JOIN
    AS_TABLE($t) AS B
ON
    AsTuple(cast(A.key as uint64), cast(A.subkey as String)) = AsTuple(cast(B.key as int64), cast(B.subkey as Utf8))
    AND AsTuple(cast(A.key1 as uint64), cast(A.subkey1 as String)) = AsTuple(cast(B.key1 as int64), cast(B.subkey1 as Utf8))
ORDER BY `key`;
