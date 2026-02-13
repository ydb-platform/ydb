$dump = ($x)->((Way($x),$x.Key,Yson::Serialize($x.PreValue),Yson::Serialize($x.Value),Yson::Serialize($x.PostValue)));
select ListMap(Yson::Iterate("{a=1;b={c=3;d=4};e=5}"y),$dump)

