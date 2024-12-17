/* syntax version 1 */
/* postgres can not */
$hum_gr_kv = ($grouping) -> {
    RETURN CASE $grouping
        WHEN 1 THEN 'Total By First digit key'
        WHEN 2 THEN 'Total By First char value'
        WHEN 3 THEN 'Grand Total'
        ELSE 'Group'
    END;
};

SELECT
    count(1) AS elements,
    key_first,
    val_first,
    $hum_gr_kv(grouping(key_first, val_first)) AS group
FROM
    plato.Input
GROUP BY
    CUBE (CAST(key AS uint32) / 100u AS key_first, Substring(value, 1, 1) AS val_first)
ORDER BY
    elements,
    key_first,
    val_first
;
