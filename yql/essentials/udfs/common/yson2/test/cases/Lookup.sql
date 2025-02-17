/* syntax version 1 */
$no_strict = Yson::Options(false AS Strict);

select
Yson::ConvertToBool(Yson::Lookup(Yson::Parse('{a=%true}'), 'a')),
Yson::LookupBool(Yson::Parse('{a=%true}'), 'a'),

Yson::ConvertToInt64(Yson::Lookup(Yson::Parse('{a=1}'), 'a')),
Yson::LookupInt64(Yson::Parse('{a=1}'), 'a'),

Yson::ConvertToUint64(Yson::Lookup(Yson::Parse('{a=2u}'), 'a')),
Yson::LookupUint64(Yson::Parse('{a=2u}'), 'a'),

Yson::ConvertToDouble(Yson::Lookup(Yson::Parse('{a=3.0}'), 'a')),
Yson::LookupDouble(Yson::Parse('{a=3.0}'), 'a'),

Yson::ConvertToString(Yson::Lookup(Yson::Parse('{a=x}'), 'a')),
Yson::LookupString(Yson::Parse('{a=x}'), 'a'),

ListLength(Yson::ConvertToList(Yson::Lookup(Yson::Parse('{a=[1;2]}'), 'a'))),
ListLength(Yson::LookupList(Yson::Parse('{a=[1;2]}'), 'a')),

DictLength(Yson::ConvertToDict(Yson::Lookup(Yson::Parse('{a={b=c}}'), 'a'))),
DictLength(Yson::LookupDict(Yson::Parse('{a={b=c}}'), 'a')),

Yson::LookupString(Yson::Parse('[]'), '0'),

Yson::LookupString(Yson::Parse('{a=12345}'), 'a', $no_strict),
Yson::LookupDouble(Yson::Parse(@@{a="12345"}@@), 'a', $no_strict);
