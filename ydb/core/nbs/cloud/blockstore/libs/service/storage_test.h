#pragma once

#include "public.h"

#include "context.h"
#include "storage.h"

#include <library/cpp/threading/future/future.h>

#include <functional>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

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
