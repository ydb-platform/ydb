/* syntax version 1 */

$data = [<|key:1, subkeys:[1,2,2,3,4,5]|>, <|key:2, subkeys:[1,2,3,5,6,8]|>];

SELECT 
    subkey,
    COUNT(key) as cnt
FROM AS_TABLE($data)
FLATTEN LIST BY subkeys as subkey
GROUP BY CAST(subkey as String) as subkey
ORDER BY subkey;
