/* syntax version 1 */
PRAGMA DisableSimpleColumns;

$one = [<|'a':{"1"}, 'b':2|>, <|'a':{"2"}, 'b':3|>, <|'a':{"3"}, 'b':4|>, <|'a':{"1"}, 'b':8|>];
$two = [<|'c':{Just("1")}, 'd':2|>, <|'c':{}, 'd':3|>, <|'c':null, 'd':4|>, <|'c':{Just("1")}, 'd':9|>];
$foo = [<|'e':{"1"u}, 'f':-2|>, <|'e':{"1"u,"2"u}, 'f':-3|>, <|'e':null, 'f':-4|>, <|'e':{"1"u}, 'f':-9|>];
$bar = [<|'g':{Just("1"u)}, 'h':1.|>, <|'g':{}, 'h':2.2|>, <|'g':{Just("1"u),Just("2"u)}, 'h':3.3|>, <|'g':{Just("1"u)}, 'h':4.4|>];

SELECT
ListSort(DictItems(bar.g)) as bar_g, bar.h,
ListSort(DictItems(foo.e)) as foo_e, foo.f,
ListSort(DictItems(one.a)) as one_a, one.b,
ListSort(DictItems(two.c)) as two_c, two.d
FROM ANY AS_TABLE($one) AS one
INNER JOIN ANY AS_TABLE($two) AS two
ON one.a == two.c
LEFT JOIN ANY AS_TABLE($foo) AS foo
ON foo.e == two.c
FULL JOIN ANY AS_TABLE($bar) AS bar
ON bar.g == one.a
ORDER BY bar_g, bar.h, foo_e, foo.f, one_a, one.b, two_c, two.d
