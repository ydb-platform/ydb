#pragma once

#include "public.h"

#include "storage.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/public.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/public.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IDurableStorage: public IStorage
{
    virtual void RestartRequests(ui32 generation) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IDurableStoragePtr CreateDurableStorageWrapper(
    ILoggingServicePtr logging,
    IStoragePtr storage,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ui32 generation);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
