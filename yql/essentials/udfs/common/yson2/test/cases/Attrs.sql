/* syntax version 1 */

$no_strict = Yson::Options(false AS Strict);

select
Yson::ConvertToBool(Yson::Parse(Yson('<a=1>#')), $no_strict),
Yson::ConvertToBool(Yson::Parse(Yson('<a=1>%true'))),
Yson::ConvertToBool(Yson::Parse(Yson('<a=1>1')), $no_strict),
Yson::ConvertToBool(Yson::Parse(Yson('<a=1>2u')), $no_strict),
Yson::ConvertToBool(Yson::Parse(Yson('<a=1>3.0')), $no_strict),
Yson::ConvertToBool(Yson::Parse(Yson('<a=1>foo')), $no_strict),
Yson::ConvertToBool(Yson::Parse(Yson('<a=1>"very loooooooooooooooooong string"')), $no_strict),

Yson::ConvertToInt64(Yson::Parse(Yson('<a=1>#')), $no_strict),
Yson::ConvertToInt64(Yson::Parse(Yson('<a=1>%true')), $no_strict),
Yson::ConvertToInt64(Yson::Parse(Yson('<a=1>1'))),
Yson::ConvertToInt64(Yson::Parse(Yson('<a=1>2u'))),
Yson::ConvertToInt64(Yson::Parse(Yson('<a=1>3.0')), $no_strict),
Yson::ConvertToInt64(Yson::Parse(Yson('<a=1>foo')), $no_strict),
Yson::ConvertToInt64(Yson::Parse(Yson('<a=1>"very loooooooooooooooooong string"')), $no_strict),

Yson::ConvertToUint64(Yson::Parse(Yson('<a=1>#')), $no_strict),
Yson::ConvertToUint64(Yson::Parse(Yson('<a=1>%true')), $no_strict),
Yson::ConvertToUint64(Yson::Parse(Yson('<a=1>1'))),
Yson::ConvertToUint64(Yson::Parse(Yson('<a=1>2u'))),
Yson::ConvertToUint64(Yson::Parse(Yson('<a=1>3.0')), $no_strict),
Yson::ConvertToUint64(Yson::Parse(Yson('<a=1>foo')), $no_strict),
Yson::ConvertToUint64(Yson::Parse(Yson('<a=1>"very loooooooooooooooooong string"')), $no_strict),

Yson::ConvertToDouble(Yson::Parse(Yson('<a=1>#')), $no_strict),
Yson::ConvertToDouble(Yson::Parse(Yson('<a=1>%true')), $no_strict),
Yson::ConvertToDouble(Yson::Parse(Yson('<a=1>1'))),
Yson::ConvertToDouble(Yson::Parse(Yson('<a=1>2u'))),
Yson::ConvertToDouble(Yson::Parse(Yson('<a=1>3.0'))),
Yson::ConvertToDouble(Yson::Parse(Yson('<a=1>foo')), $no_strict),
Yson::ConvertToDouble(Yson::Parse(Yson('<a=1>"very loooooooooooooooooong string"')), $no_strict),

Yson::ConvertToString(Yson::Parse(Yson('<a=1>#')), $no_strict),
Yson::ConvertToString(Yson::Parse(Yson('<a=1>%true')), $no_strict),
Yson::ConvertToString(Yson::Parse(Yson('<a=1>1')), $no_strict),
Yson::ConvertToString(Yson::Parse(Yson('<a=1>2u')), $no_strict),
Yson::ConvertToString(Yson::Parse(Yson('<a=1>3.0')), $no_strict),
Yson::ConvertToString(Yson::Parse(Yson('<a=1>foo'))),
Yson::ConvertToString(Yson::Parse(Yson('<a=1>"very loooooooooooooooooong string"'))),

ListMap(Yson::ConvertToList(Yson::Parse(Yson('<a=1>[1;2;3]'))), Yson::ConvertToInt64),
DictKeys(Yson::ConvertToDict(Yson::Parse(Yson('<a=1>{b=1;c=2}')))),

DictKeys(Yson::Attributes(Yson::Parse(Yson('<a=1;b=2>#')))),
ListMap(DictPayloads(Yson::Attributes(Yson::Parse(Yson('<a=1;b=2>#')))), Yson::ConvertToInt64),

DictKeys(Yson::Attributes(Yson::Parse(Yson('<a=1;b=2>[]')))),
ListMap(DictPayloads(Yson::Attributes(Yson::Parse(Yson('<a=1;b=2>[]')))), Yson::ConvertToInt64),

DictKeys(Yson::Attributes(Yson::Parse(Yson('<a=1;b=2>{}')))),
ListMap(DictPayloads(Yson::Attributes(Yson::Parse(Yson('<a=1;b=2>{}')))), Yson::ConvertToInt64);
