PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;

SELECT
    1 IN if(1 > 0, (1, 10, 301, 310,), (311,))
;
