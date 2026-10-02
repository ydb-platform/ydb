SELECT
    q,ListSort(ListFilter(YqlLang::RuleFreq(q),($x)->($x.0 in ("TYPE"))))
FROM (VALUES
    ("select cast(1 as Int32)"),
    ("select listcreate(String)"),
    ("select formattype(List<String>)"),
    ("select formattype(Struct<a:String, b:Int32>)")
) AS a(q)
order by q
