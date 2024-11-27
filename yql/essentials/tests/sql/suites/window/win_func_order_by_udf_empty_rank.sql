/* syntax version 1 */
/* postgres can not */
PRAGMA DisableAnsiRankForNullableKeys;

USE plato;

SELECT 
   key,
   RANK() over w as ix,
   subkey,
   String::Base64Encode(subkey) as subkey_enc
FROM Input
WINDOW w AS (
    PARTITION BY key
    ORDER BY String::Base64Encode(subkey) DESC
)
ORDER BY key, ix;
