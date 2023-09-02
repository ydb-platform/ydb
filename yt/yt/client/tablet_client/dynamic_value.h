#pragma once

#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/unversioned_value.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

// NB: 4-aligned.
struct TDynamicString
{
    ui32 Length;
    char Data[1]; // the actual length is above
};

// NB: TDynamicValueData must be binary compatible with TUnversionedValueData for all simple types.
union TDynamicValueData
{
    //! |Int64| value.
    i64 Int64;
    //! |Uint64| value.
    ui64 Uint64;
    //! |Double| value.
    double Double;
    //! |Boolean| value.
    bool Boolean;
    //! String value for |String| type or YSON-encoded value for |Any| type.
    TDynamicString* String;
};

static_assert(
    sizeof(TDynamicValueData) == sizeof(NTableClient::TUnversionedValueData),
    "TDynamicValueData and TUnversionedValueData must be of the same size.");

struct TDynamicValue
{
    TDynamicValueData Data;
    ui32 Revision;
    bool Null;
    NTableClient::EValueFlags Flags;
    char Padding[2];
};

static_assert(
    sizeof(TDynamicValue) == 16,
    "Wrong TDynamicValue size.");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
