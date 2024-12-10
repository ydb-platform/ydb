/* syntax version 1 */
USE plato;

PRAGMA AnsiInForEmptyOrNullableItemsCollections;

$src = (
    SELECT
        CAST(key AS Int32)
    FROM
        Input
);

SELECT
    ListFilter(
        ListFromRange(1, 100), ($i) -> {
            RETURN $i IN $src;
        }
    )
;
