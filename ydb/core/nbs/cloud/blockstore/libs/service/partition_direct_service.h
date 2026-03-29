#pragma once

#include "public.h"

#include <ydb/library/actors/wilson/wilson_span.h>

#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IPartitionDirectService
{
    virtual ~IPartitionDirectService() = default;

    [[nodiscard]] virtual TVolumeConfigPtr GetVolumeConfig() const = 0;

    [[nodiscard]] virtual NWilson::TSpan CreteRootSpan(TStringBuf name) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
