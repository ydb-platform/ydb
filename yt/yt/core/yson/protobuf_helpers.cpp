#include "protobuf_helpers.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

void ToProto(TProtoStringType* serialized, TYsonStringBuf original)
{
    *serialized = TProtoStringType(original.AsStringBuf());
}

void ToProto(TProtoStringType* serialized, const TYsonString& original)
{
    *serialized = original.ToString();
}

void FromProto(TYsonString* original, const TProtoStringType& serialized)
{
    *original = TYsonString(serialized);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
