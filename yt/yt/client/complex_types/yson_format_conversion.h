#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/formats/public.h>

#include <yt/yt/core/yson/pull_parser.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////
//
// There are two modes of representing complex types in yson: named and positional.
// They are similar for all types but Struct and VariantStruct.
//
// Consider Struct({{"field0", type0}, {"field1", type1}, ...})
// Its positional representation might be:
//   [v0; v1; ...] (v0 is of type0, v1 is of type1)
//
// Named representation of the same structure:
//   {field0=v0; field1=v1}
//
//
// Consider Struct({{"field0", type0}, {"field1", type1}, ...})
// Its positional representation might be:
//   [1, v1]
//
// Named representation of the same structure:
//   [field1; v1]
//
// Functions in this file create convertors between these two representations.

using TYsonClientToServerConverter = std::function<NTableClient::TUnversionedValue(NTableClient::TUnversionedValue value)>;
using TYsonServerToClientConverter = std::function<void(NTableClient::TUnversionedValue value, NYson::IYsonConsumer* consumer)>;

struct TYsonConverterConfig
{
    NFormats::EComplexTypeMode ComplexTypeMode = NFormats::EComplexTypeMode::Named;
    NFormats::EDictMode StringKeyedDictMode = NFormats::EDictMode::Positional;
    NFormats::EDecimalMode DecimalMode = NFormats::EDecimalMode::Binary;
    NFormats::ETimeMode TimeMode = NFormats::ETimeMode::Binary;
    NFormats::EUuidMode UuidMode = NFormats::EUuidMode::Binary;

    // When SkipNullValues is true converters doesn't write
    // structure fields that have `#` value (i.e. they are Null).
    bool SkipNullValues = false;
};

TYsonServerToClientConverter CreateYsonServerToClientConverter(
    const NTableClient::TComplexTypeFieldDescriptor& descriptor,
    const TYsonConverterConfig& config);

TYsonClientToServerConverter CreateYsonClientToServerConverter(
    const NTableClient::TComplexTypeFieldDescriptor& descriptor,
    const TYsonConverterConfig& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
