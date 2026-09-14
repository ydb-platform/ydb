SELECT
    q,ListSort(ListFilter(YqlLang::RuleFreq(q),($x)->($x.0 in ("READ_HINT"))))
FROM (VALUES
    ("select * from foo.bar with xlock"),
    ("select * from foo.bar with (inline, infer_scheme)")
) AS a(q)
order by q
