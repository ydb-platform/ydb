#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EVersionedIOMode,
    ((Default)              (0))
    ((LatestTimestamp)      (1))
);

struct TVersionedReadOptions
    : public NYTree::TYsonStructLite
{
    EVersionedIOMode ReadMode;

    REGISTER_YSON_STRUCT_LITE(TVersionedReadOptions);

    static void Register(TRegistrar registrar);
};

struct TVersionedWriteOptions
    : public NYTree::TYsonStructLite
{
    EVersionedIOMode WriteMode;

    REGISTER_YSON_STRUCT_LITE(TVersionedWriteOptions);

    static void Register(TRegistrar registrar);
};

void ToProto(
    NProto::TVersionedReadOptions* protoOptions,
    const NTableClient::TVersionedReadOptions& options);

void FromProto(
    NTableClient::TVersionedReadOptions* options,
    const NProto::TVersionedReadOptions& protoOptions);

void ToProto(
    NProto::TVersionedWriteOptions* protoOptions,
    const NTableClient::TVersionedWriteOptions& options);

void FromProto(
    NTableClient::TVersionedWriteOptions* options,
    const NProto::TVersionedWriteOptions& protoOptions);

std::optional<TString> GetTimestampColumnOriginalNameOrNull(TStringBuf name);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
