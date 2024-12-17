/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    key,
    sum(DISTINCT CAST(Unicode::ToLower(CAST(subkey AS Utf8)) AS Int32)) + sum(DISTINCT CAST(Unicode::ToUpper(CAST(subkey AS Utf8)) AS Uint64)) AS sks,
    aggregate_list(DISTINCT key || '') AS kl
FROM
    Input3
GROUP BY
    key || 'foo' AS key
ORDER BY
    key
;
