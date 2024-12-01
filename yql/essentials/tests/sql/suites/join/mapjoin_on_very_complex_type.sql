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
    AsTuple(A.subkey, AsTuple(A.subkey1, AsTuple(CAST(A.key AS Int64), CAST(A.key1 AS Uint64)))) = AsTuple(B.subkey, AsTuple(B.subkey1, AsTuple(CAST(B.key AS Uint64), CAST(B.key1 AS Int64))))
ORDER BY `key`;
