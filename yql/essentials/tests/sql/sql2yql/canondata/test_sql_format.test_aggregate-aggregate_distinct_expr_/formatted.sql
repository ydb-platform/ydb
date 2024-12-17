/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    sum(DISTINCT CAST(Unicode::ToLower(CAST(subkey AS Utf8)) AS Int32)) + sum(DISTINCT CAST(Unicode::ToUpper(CAST(subkey AS Utf8)) AS Uint64)) AS sks,
    ListSort(aggregate_list(DISTINCT key || '_')) AS kl
FROM
    Input3
;
