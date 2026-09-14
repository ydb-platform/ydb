SELECT
    q,ListSort(ListFilter(YqlLang::RuleFreq(q),($x)->($x.0 in ("FUNC","MODULE","MODULE_FUNC"))))
FROM (VALUES
    ("select f()"),
    ("select f::g()"),
    ("select python3::x()"),
    ("select javascript::y()")
) AS a(q)
order by q
