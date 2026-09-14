SELECT
    q,ListSort(ListFilter(YqlLang::RuleFreq(q),($x)->($x.0 in ("INSERT_HINT"))))
FROM (VALUES
    ("insert into plato.foo with truncate select 1"),
    ("insert into plato.foo with schema(a int32) select 1"),
    ("insert into plato.foo with columns struct<a:int32> select 1"),
    ("insert into plato.foo with (truncate,user_attrs='{}') select 1")
) AS a(q)
order by q
