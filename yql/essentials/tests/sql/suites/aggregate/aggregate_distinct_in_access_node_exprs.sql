/* syntax version 1 */
/* postgres can not */
SELECT
    key,
    AGGREGATE_LIST(DISTINCT cast(subkey as Int32))[COUNT(DISTINCT cast(subkey as Uint64)) - 1] as foo
FROM
    AS_TABLE([<|key:1, subkey:"1"|>,
              <|key:2, subkey:"2"|>,
              <|key:1, subkey:"1"|>,
              <|key:2, subkey:"2"|>])
GROUP BY key
ORDER BY key;
