$parse = Unwrap(Yson::ParseJson('"foo"'));
$from = Yson::From('bar'u);
select ListMap([$parse, $from], ($x)->((Yson::IsUtf8($x), Yson::AsUtf8($x), Yson::TryAsUtf8($x), Yson::ConvertTo($x, Utf8))));

select Yson::IsUtf8('foo'y), Yson::TryAsUtf8('foo'y);

