#include "fast_path_service.h"

namespace NYdb::NBS::NBlockStore {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TFastPathService::TFastPathService(
    ui64 tabletId,
    ui32 generation,
    TVector<NBsController::TDDiskId> ddiskIds,
    TVector<NBsController::TDDiskId> persistentBufferDDiskIds,
    ui32 blockSize,
    ui64 blocksCount)
    : DirectBlockGroup(
        tabletId,
        generation,
        std::move(ddiskIds),
        std::move(persistentBufferDDiskIds),
        blockSize,
        blocksCount)
{
    DirectBlockGroup.EstablishConnections();
}

NThreading::TFuture<TReadBlocksLocalResponse> TFastPathService::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request)
{
    with_lock (Lock) {
        return DirectBlockGroup.ReadBlocksLocal(std::move(callContext), std::move(request));
    }
}

NThreading::TFuture<TWriteBlocksLocalResponse> TFastPathService::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request)
{
    with_lock (Lock) {
        return DirectBlockGroup.WriteBlocksLocal(std::move(callContext), std::move(request));
    }
}

NThreading::TFuture<TZeroBlocksLocalResponse> TFastPathService::ZeroBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TZeroBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);
    Y_UNUSED(request);
    Y_ABORT_UNLESS(false, "ZeroBlocksLocal is not implemented");
    return NThreading::MakeFuture<TZeroBlocksLocalResponse>();
}

void TFastPathService::ReportIOError()
{
    // TODO: implement
}

}   // namespace NYdb::NBS::NBlockStore
