
$a = [
    AsStruct(
        'a' as x,
        1 as y
    ),
    AsStruct(
        'a' as x,
        1 as y
    ),
    AsStruct(
        'a' as x,
        2 as y
    ),
    AsStruct(
        'a' as x,
        3 as y
    ),
    AsStruct(
        'b' as x,
        1 as y
    ),
    AsStruct(
        'b' as x,
        2 as y
    ),
    AsStruct(
        'b' as x,
        3 as y
    ),
    AsStruct(
        'c' as x,
        1 as y
    ),
];

$a = select x as bar, y as foo from AS_TABLE($a);

$b =
SELECT
    a.bar as bar, count(*) as cnt
from $a as a
inner join (
    select bar, min(foo) as foo
    from $a
    group by bar
) as b using (foo, bar)
group by a.bar;


select * from $a as a left join $b as b using (bar);
