
#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TVolumeRequestCounters
{
private:
    NMonitoring::TDynamicCounters::TCounterPtr Requests;
    NMonitoring::TDynamicCounters::TCounterPtr ReplyOk;
    NMonitoring::TDynamicCounters::TCounterPtr ReplyErr;
    NMonitoring::TDynamicCounters::TCounterPtr Bytes;

public:
    explicit TVolumeRequestCounters(NMonitoring::TDynamicCounterPtr parent);

    void RequestStarted(ui32 bytes);
    void RequestFinished(bool ok);
};

////////////////////////////////////////////////////////////////////////////////
class TVolumeCounters
{
private:
    TVolumeRequestCounters ReadBlocks;
    TVolumeRequestCounters WriteBlocks;
    TVolumeRequestCounters ZeroBlocks;

public:
    explicit TVolumeCounters(NMonitoring::TDynamicCounterPtr parent);

    void RequestStarted(EBlockStoreRequest requestType, ui32 bytes);
    void RequestFinished(EBlockStoreRequest requestType, bool ok);

private:
    TVolumeRequestCounters& Get(EBlockStoreRequest requestType);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
