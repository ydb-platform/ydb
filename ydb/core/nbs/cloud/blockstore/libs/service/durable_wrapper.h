#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/public.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateDurableStorageWrapper(
    IStoragePtr storage,
    ITimerPtr timer,
    ISchedulerPtr scheduler);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
