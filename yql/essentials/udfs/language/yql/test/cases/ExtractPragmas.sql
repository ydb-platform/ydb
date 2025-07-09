SELECT
    q,ListSort(ListFilter(YqlLang::RuleFreq(q),($x)->($x.0 in ("PRAGMA"))))
FROM (VALUES
    ("pragma warningmsg('foo')"),
    ("pragma dq.Foo"),
    ("pragma yt.Bar")
) AS a(q)
order by q
