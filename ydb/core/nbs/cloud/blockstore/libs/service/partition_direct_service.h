#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/public.h>

#include <ydb/library/actors/wilson/wilson_span.h>

#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IPartitionDirectService
{
    virtual ~IPartitionDirectService() = default;

    [[nodiscard]] virtual TVolumeConfigPtr GetVolumeConfig() const = 0;

    [[nodiscard]] virtual NWilson::TSpan CreteRootSpan(TStringBuf name) = 0;

    virtual void ScheduleAfterDelay(
        TExecutorPtr executor,
        TDuration delay,
        TCallback callback) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
