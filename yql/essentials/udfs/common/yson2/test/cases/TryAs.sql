select
    Yson::TryAsBool('%true'y),
    Yson::TryAsInt64('1'y),
    Yson::TryAsUint64('2u'y),
    Yson::TryAsDouble('3.4'y),
    Yson::TryAsString('foo'y),
    Yson::TryAsList('[]'y),
    Yson::TryAsList('[1;foo]'y),
    Yson::TryAsDict('{}'y),
    ListSort(DictItems(Yson::TryAsDict('{a=1;b=foo}'y)),($x)->($x.0));

