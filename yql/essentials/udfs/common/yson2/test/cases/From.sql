/* syntax version 1 */
select
Yson::IsEntity(Yson::From(NULL)),
Yson::IsEntity(Yson::Parse(Yson("#"))),
Yson::IsEntity(Yson::Parse(Yson("1"))),

Yson::SerializeText(Yson::FromBool(true)),
Yson::SerializeText(Yson::FromBool(Nothing(Bool?))),
Yson::SerializeText(Yson::FromInt64(1l)),
Yson::SerializeText(Yson::FromInt64(Nothing(Int64?))),
Yson::SerializeText(Yson::FromUint64(2ul)),
Yson::SerializeText(Yson::FromUint64(Nothing(Uint64?))),
Yson::SerializeText(Yson::FromDouble(3.)),
Yson::SerializeText(Yson::FromDouble(Nothing(Double?))),
Yson::SerializeText(Yson::FromString("foo")),
Yson::SerializeText(Yson::FromString("fooooooooooooooooooooooooooooooooo")),
Yson::SerializeText(Yson::FromString(Nothing(String?))),

Yson::SerializeText(Yson::FromList(Yson::ConvertToList(Yson::Parse(Yson("[1;2;3]"))))),
Yson::SerializeText(Yson::FromDict(Yson::ConvertToDict(Yson::Parse(Yson("{a=x;b=y}")))));

