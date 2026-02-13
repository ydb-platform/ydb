#pragma once

#include "public.h"

#include "request.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IStorage
{
    virtual ~IStorage() = default;

    virtual NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request) = 0;

    virtual NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request) = 0;

    virtual NThreading::TFuture<TZeroBlocksLocalResponse> ZeroBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TZeroBlocksLocalRequest> request) = 0;

    virtual void ReportIOError() = 0;
};

IStoragePtr CreateStorageStub();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
