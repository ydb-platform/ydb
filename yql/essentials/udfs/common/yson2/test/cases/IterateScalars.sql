$dump = ($x)->((Way($x),$x.Key,Yson::Serialize($x.PreValue),Yson::Serialize($x.Value),Yson::Serialize($x.PostValue)));
select ListMap(Yson::Iterate("#"y),$dump),
       ListMap(Yson::Iterate("%true"y),$dump),
       ListMap(Yson::Iterate("1"y),$dump),
       ListMap(Yson::Iterate("2u"y),$dump),
       ListMap(Yson::Iterate("3.0"y),$dump),
       ListMap(Yson::Iterate("str"y),$dump);

