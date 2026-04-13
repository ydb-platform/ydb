#pragma once

#include "public.h"

#include "context.h"
#include "storage.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service.h>

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

#include <library/cpp/threading/future/future.h>

#include <functional>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TTestPartitionDirectService final: public IPartitionDirectService
{
public:
    // IPartitionDirectService implementation
    TVolumeConfigPtr GetVolumeConfig() const override
    {
        return nullptr;
    }

    NWilson::TSpan CreteRootSpan(TStringBuf name) override
    {
        Y_UNUSED(name);
        return NWilson::TSpan();
    }

    void ScheduleAfterDelay(
        TExecutorPtr executor,
        TDuration delay,
        TCallback callback) override
    {
        Y_UNUSED(delay);
        executor->ExecuteSimple(std::move(callback));
    }
};

struct TTestStorage: public IStorage
{
    using TReadBlocksLocalHandler =
        std::function<NThreading::TFuture<TReadBlocksLocalResponse>(
            TCallContextPtr callContext,
            std::shared_ptr<TReadBlocksLocalRequest>)>;

    using TWriteBlocksLocalHandler =
        std::function<NThreading::TFuture<TWriteBlocksLocalResponse>(
            TCallContextPtr callContext,
            std::shared_ptr<TWriteBlocksLocalRequest>)>;

    using TZeroBlocksLocalHandler =
        std::function<NThreading::TFuture<TZeroBlocksLocalResponse>(
            TCallContextPtr callContext,
            std::shared_ptr<TZeroBlocksLocalRequest>)>;

    ui32 ErrorCount = 0;
    bool DoAllocations = false;

    TReadBlocksLocalHandler ReadBlocksLocalHandler;
    TWriteBlocksLocalHandler WriteBlocksLocalHandler;
    TZeroBlocksLocalHandler ZeroBlocksLocalHandler;

    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request) override
    {
        return ReadBlocksLocalHandler(
            std::move(callContext),
            std::move(request));
    }

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request) override
    {
        return WriteBlocksLocalHandler(
            std::move(callContext),
            std::move(request));
    }

    NThreading::TFuture<TZeroBlocksLocalResponse> ZeroBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TZeroBlocksLocalRequest> request) override
    {
        return ZeroBlocksLocalHandler(
            std::move(callContext),
            std::move(request));
    }

    void ReportIOError() override
    {
        ++ErrorCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
