
#include "storage_gate.h"

#include "context.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

namespace {

NProto::TError MakeDetachedError()
{
    return MakeError(E_REJECTED, "storage is detached");
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TStorageGate::TStorageGate(IStoragePtr storage)
{
    Attach(std::move(storage));
}

TStorageGate::~TStorageGate() = default;

void TStorageGate::Attach(IStoragePtr storage)
{
    auto newHolder = MakeIntrusive<THolder>();
    newHolder->Storage = std::move(storage);
    Holder.AtomicStore(newHolder);
}

void TStorageGate::Detach()
{
    Holder.AtomicStore(nullptr);
}

NThreading::TFuture<TReadBlocksLocalResponse> TStorageGate::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request)
{
    auto holder = Holder.AtomicLoad();
    if (!holder) {
        TReadBlocksLocalResponse response;
        response.Error = MakeDetachedError();
        return NThreading::MakeFuture(std::move(response));
    }
    return holder->Storage->ReadBlocksLocal(
        std::move(callContext),
        std::move(request));
}

NThreading::TFuture<TWriteBlocksLocalResponse> TStorageGate::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request)
{
    auto holder = Holder.AtomicLoad();
    if (!holder) {
        TWriteBlocksLocalResponse response;
        response.Error = MakeDetachedError();
        return NThreading::MakeFuture(std::move(response));
    }
    return holder->Storage->WriteBlocksLocal(
        std::move(callContext),
        std::move(request));
}

NThreading::TFuture<TZeroBlocksLocalResponse> TStorageGate::ZeroBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TZeroBlocksLocalRequest> request)
{
    auto holder = Holder.AtomicLoad();
    if (!holder) {
        TZeroBlocksLocalResponse response;
        response.Error = MakeDetachedError();
        return NThreading::MakeFuture(std::move(response));
    }
    return holder->Storage->ZeroBlocksLocal(
        std::move(callContext),
        std::move(request));
}

void TStorageGate::ReportIOError()
{
    auto holder = Holder.AtomicLoad();
    if (holder) {
        holder->Storage->ReportIOError();
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
