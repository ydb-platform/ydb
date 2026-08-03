#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/public.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/public.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateDurableStorageWrapper(
    ILoggingServicePtr logging,
    IStoragePtr storage,
    ITimerPtr timer,
    ISchedulerPtr scheduler);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
