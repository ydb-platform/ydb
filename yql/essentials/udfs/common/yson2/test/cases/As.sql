select
    Yson::AsBool('%true'y),
    Yson::AsInt64('1'y),
    Yson::AsUint64('2u'y),
    Yson::AsDouble('3.4'y),
    Yson::AsString('foo'y),
    Yson::AsList('[]'y),
    Yson::AsList('[1;foo]'y),
    Yson::AsDict('{}'y),
    ListSort(DictItems(Yson::AsDict('{a=1;b=foo}'y)),($x)->($x.0));

