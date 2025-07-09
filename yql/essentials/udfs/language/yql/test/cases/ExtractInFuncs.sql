SELECT
    q,ListSort(ListFilter(YqlLang::RuleFreq(q),($x)->($x.0 in ("FUNC","MODULE","MODULE_FUNC"))))
FROM (VALUES
    ("select 1 in f()"),
    ("select 1 in f::g()"),
    ("select 1 in python3::x()"),
    ("select 1 in javascript::y()")
) AS a(q)
order by q
