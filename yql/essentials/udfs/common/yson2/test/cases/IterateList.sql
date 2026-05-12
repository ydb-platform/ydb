$dump = ($x)->((Way($x),$x.Key,Yson::Serialize($x.PreValue),Yson::Serialize($x.Value),Yson::Serialize($x.PostValue)));
select ListMap(Yson::Iterate("[1;[2;3];4]"y),$dump)

