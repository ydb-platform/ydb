/* postgres can not */
/* syntax version 1 */
Select Enum("foo",Enum<"foo","bar">) == AsEnum("foo"),
       Enum("foo",Enum<"foo","bar">) == AsEnum("bar"),
       Enum("foo",Enum<"foo","bar">) == AsEnum("baz");
