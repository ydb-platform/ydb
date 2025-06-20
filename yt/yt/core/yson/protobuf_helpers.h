#pragma once

#include "public.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

void ToProto(TProtobufString* serialized, TYsonStringBuf original);
void ToProto(TProtobufString* serialized, const TYsonString& original);
void FromProto(TYsonString* original, const TProtobufString& serialized);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <>
struct TProtoTraits<NYson::TYsonString>
{
    using TSerialized = TProtobufString;
};

template <>
struct TProtoTraits<NYson::TYsonStringBuf>
{
    using TSerialized = TProtobufString;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
