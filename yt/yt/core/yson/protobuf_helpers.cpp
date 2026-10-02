#include "protobuf_helpers.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

void ToProto(TProtobufString* serialized, TYsonStringBuf original)
{
    *serialized = TProtobufString(original.AsStringBuf());
}

void ToProto(TProtobufString* serialized, const TYsonString& original)
{
    *serialized = original.ToString();
}

void FromProto(TYsonString* original, TProtobufString serialized)
{
    *original = TYsonString(NYT::FromProto<std::string>(std::move(serialized)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
