/* postgres can not */

PRAGMA AnsiInForEmptyOrNullableItemsCollections;

$list = AsList(
    AsTuple(Just("aa"), Just("aaa")),
    AsTuple(Just("bb"), Just("bbb")),
);

SELECT ListMap($list, ($item) -> { RETURN 'bb' IN $item; });
