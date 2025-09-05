/* postgres can not */
-- right side depends on row
select * from plato.Input where AsTuple(key) in (
                                AsTuple("075"),
                                AsTuple(key)) order by key, subkey;

-- left side is not a prefix of sort columns
select * from plato.Input where AsTuple(subkey, value) in (
                                AsTuple("1",    "aaa"),
                                AsTuple("3",    "aaa")) order by key, subkey;
-- not a member on left side
select * from plato.Input where AsTuple(subkey, AsTuple(key,   1), value, key || "x") in (
                                AsTuple("1",    AsTuple("075", 1), "abc", "075x"),
                                AsTuple("3",    AsTuple("023", 1), "aaa", "023x")) order by key, subkey;
