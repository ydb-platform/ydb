#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EVersionedIoMode,
    ((Default)              (0))
    ((LatestTimestamp)      (1))
);

struct TVersionedReadOptions
    : public NYTree::TYsonStructLite
{
    EVersionedIoMode ReadMode;

    REGISTER_YSON_STRUCT_LITE(TVersionedReadOptions);

    static void Register(TRegistrar registrar);
};

std::optional<TString> GetTimestampColumnOriginalNameOrNull(TStringBuf name);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
