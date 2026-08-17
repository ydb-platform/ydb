
#include "storage.h"

#include "context.h"

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// static
NThreading::TFuture<TReadBlocksLocalResponse> TStorageAdapter::Execute(
    IStorage* storage,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request)
{
    return storage->ReadBlocksLocal(std::move(callContext), std::move(request));
}

// static
NThreading::TFuture<TWriteBlocksLocalResponse> TStorageAdapter::Execute(
    IStorage* storage,
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request)
{
    return storage->WriteBlocksLocal(
        std::move(callContext),
        std::move(request));
}

// static
NThreading::TFuture<TZeroBlocksLocalResponse> TStorageAdapter::Execute(
    IStorage* storage,
    TCallContextPtr callContext,
    std::shared_ptr<TZeroBlocksLocalRequest> request)
{
    return storage->ZeroBlocksLocal(std::move(callContext), std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
