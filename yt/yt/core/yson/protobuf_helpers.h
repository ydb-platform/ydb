#pragma once

#include "public.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

void ToProto(TProtoStringType* serialized, TYsonStringBuf original);
void ToProto(TProtoStringType* serialized, const TYsonString& original);
void FromProto(TYsonString* original, const TProtoStringType& serialized);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <>
struct TProtoTraits<NYson::TYsonString>
{
    using TSerialized = TProtoStringType;
};

template <>
struct TProtoTraits<NYson::TYsonStringBuf>
{
    using TSerialized = TProtoStringType;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
