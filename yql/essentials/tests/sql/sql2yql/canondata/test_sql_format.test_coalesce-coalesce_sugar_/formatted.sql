/* postgres can not */
$data = (
    SELECT
        key,
        CAST(key AS Int64) / 100 AS eval
    FROM plato.Input
);

SELECT
    CASE
        WHEN eval < 5
            THEN eval
        ELSE CAST(NULL AS Int64)
    END ?? -1,
    key
FROM $data;
