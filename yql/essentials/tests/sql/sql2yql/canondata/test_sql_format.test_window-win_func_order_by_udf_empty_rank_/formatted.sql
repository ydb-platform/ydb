/* syntax version 1 */
/* postgres can not */
PRAGMA DisableAnsiRankForNullableKeys;
USE plato;

SELECT
    key,
    RANK() OVER w AS ix,
    subkey,
    String::Base64Encode(subkey) AS subkey_enc
FROM
    Input
WINDOW
    w AS (
        PARTITION BY
            key
        ORDER BY
            String::Base64Encode(subkey) DESC
    )
ORDER BY
    key,
    ix
;
