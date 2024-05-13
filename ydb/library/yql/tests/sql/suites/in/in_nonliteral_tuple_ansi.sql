PRAGMA AnsiInForEmptyOrNullableItemsCollections;

select 1 in if(1 > 0, (1, 10, 301, 310,), (311,))
