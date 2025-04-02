/* syntax version 1 */
/* postgres can not */

PRAGMA DistinctOverWindow;

$input = AsList(
    AsStruct(1 AS key, 1001 AS subkey, "AAA" AS value),
    AsStruct(150 AS key, 150 AS subkey, "AAB" AS value),
    AsStruct(3 AS key, 3003 AS subkey, "AAC" AS value),
    AsStruct(150 AS key, 150 AS subkey, "AAD" AS value),
    AsStruct(5 AS key, 5005 AS subkey, "AAE" AS value),
);

SELECT
    count(DISTINCT i1.key) OVER (PARTITION BY i1.subkey) AS cnt,
FROM AS_TABLE($input) AS i1
CROSS JOIN AS_TABLE($input) AS i2
ORDER BY cnt;
