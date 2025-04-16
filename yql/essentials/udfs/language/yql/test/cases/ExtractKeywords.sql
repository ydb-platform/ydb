SELECT
    q,ListSort(ListFilter(YqlLang::RuleFreq(q),($x)->($x.0 in ("KEYWORD"))))
FROM (VALUES
    ("select 1 as x union all select 2 as x")
) AS a(q)
order by q
