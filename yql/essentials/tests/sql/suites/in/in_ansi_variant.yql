/* postgres can not */
/* syntax version 1 */
PRAGMA AnsiInForEmptyOrNullableItemsCollections;
Select Enum("foo",Enum<"foo","bar">) in (
            AsEnum("foo")
        ),
        Enum("foo",Enum<"foo","bar">) in (
            AsEnum("bar")
        ),
        Enum("foo",Enum<"foo","bar">) in (
            AsEnum("foo"),
            AsEnum("bar")
        ),
        Enum("foo",Enum<"foo","bar">) in (
            AsEnum("bar"),
            AsEnum("baz")
        ),
        Enum("foo",Enum<"foo","bar">) in [
            AsEnum("foo"),
            AsEnum("bar")
        ],
        Enum("foo",Enum<"foo","bar">) in {
            AsEnum("bar"),
            AsEnum("baz")
        };        