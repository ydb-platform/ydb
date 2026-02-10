#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>

#include <util/datetime/base.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TMetricRequest
{
    const EBlockStoreRequest RequestType;
    const TString ClientId;
    const TString DiskId;
    const ui32 BlockSize;
    const TBlockRange64 Range;
    TInstant RequestTimestamp;
    const bool Unaligned = false;

    TMetricRequest(
        EBlockStoreRequest requestType,
        const TString& clientId,
        const TString& diskId,
        ui64 start,
        ui64 size,
        ui32 blockSize);

    ui32 GetRequestBytes() const;
};

////////////////////////////////////////////////////////////////////////////////

struct IVHostStats
{
    virtual ~IVHostStats() = default;

    virtual void RequestStarted(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext) = 0;

    virtual void RequestCompleted(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext,
        const NProto::TError& error) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
