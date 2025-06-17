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

void FromProto(TYsonString* original, const TProtobufString& serialized)
{
    *original = TYsonString(serialized);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
