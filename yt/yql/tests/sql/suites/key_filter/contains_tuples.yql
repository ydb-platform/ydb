/* postgres can not */

select * from plato.Input where AsTuple(key) in (
                                AsTuple("075"),
                                AsTuple("037")) order by key;

select * from plato.Input where AsTuple(key,   subkey) in (
                                AsTuple("075", "1"),
                                AsTuple("023", "3")) order by key;

select * from plato.Input where AsTuple(key,   subkey, 1 + 2, value) in (
                                AsTuple("075", "1",    3u,    "abc"),
                                AsTuple("023", "3",    1+1+1, "aaa")) order by key;

select * from plato.Input where AsTuple(subkey, AsTuple(key,   1), value, key) in (
                                AsTuple("1",    AsTuple("075", 1), "abc", "075"),
                                AsTuple("3",    AsTuple("023", 1), "aaa", "023")) order by key;
