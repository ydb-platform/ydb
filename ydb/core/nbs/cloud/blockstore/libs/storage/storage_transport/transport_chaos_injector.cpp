#include "transport_chaos_injector.h"

#include "ddisk_helpers.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error_utils.h>

#include <library/cpp/threading/future/future.h>

#include <util/system/yassert.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

using namespace NKikimrBlobStorage::NDDisk;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
NThreading::TFuture<T> MakeUndeliveredFuture()
{
    T result;
    SetErrorStatus(TReplyStatus::ERROR, UndeliveryErrorMessage, result);
    return NThreading::MakeFuture(std::move(result));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TTransportChaosInjector::TTransportChaosInjector(
    TStorageTransportPtr underlyingTransport)
    : UnderlyingTransport(std::move(underlyingTransport))
{
    Y_ABORT_UNLESS(UnderlyingTransport);
}

void TTransportChaosInjector::DisableNode(ui32 nodeId)
{
    const auto current = DisabledNodes.AtomicLoad();
    if (current->NodeIds.contains(nodeId)) {
        return;
    }

    auto next = MakeIntrusive<TDisabledNodes>();
    next->NodeIds = current->NodeIds;
    next->NodeIds.insert(nodeId);
    DisabledNodes.AtomicStore(next);
}

void TTransportChaosInjector::EnableNode(ui32 nodeId)
{
    const auto current = DisabledNodes.AtomicLoad();
    if (!current->NodeIds.contains(nodeId)) {
        return;
    }

    auto next = MakeIntrusive<TDisabledNodes>();
    next->NodeIds = current->NodeIds;
    next->NodeIds.erase(nodeId);
    DisabledNodes.AtomicStore(next);
}

IStorageTransport::TConnectResultFutures TTransportChaosInjector::Connect(
    const THostConnection& connection)
{
    if (!IsNodeDisabled(connection.DDiskId.NodeId)) {
        return UnderlyingTransport->Connect(connection);
    }

    const auto disconnectPromise = NThreading::NewPromise<ui32>();
    return {
        .ConnectFuture = MakeUndeliveredFuture<TEvConnectResult>(),
        .DisconnectFuture = disconnectPromise.GetFuture(),
    };
}

NThreading::TFuture<IStorageTransport::TEvReadPersistentBufferResult>
TTransportChaosInjector::ReadFromPBuffer(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const TPBufferKey pBufferKey,
    const NKikimr::NDDisk::TReadInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    if (IsNodeDisabled(connection.DDiskId.NodeId)) {
        return MakeUndeliveredFuture<TEvReadPersistentBufferResult>();
    }
    return UnderlyingTransport->ReadFromPBuffer(
        connection,
        selector,
        pBufferKey,
        instruction,
        data,
        span);
}

NThreading::TFuture<IStorageTransport::TEvReadResult>
TTransportChaosInjector::ReadFromDDisk(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const NKikimr::NDDisk::TReadInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    if (IsNodeDisabled(connection.DDiskId.NodeId)) {
        return MakeUndeliveredFuture<TEvReadResult>();
    }
    return UnderlyingTransport
        ->ReadFromDDisk(connection, selector, instruction, data, span);
}

NThreading::TFuture<IStorageTransport::TEvWritePersistentBufferResult>
TTransportChaosInjector::WriteToPBuffer(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const ui64 lsn,
    const NKikimr::NDDisk::TWriteInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    if (IsNodeDisabled(connection.DDiskId.NodeId)) {
        return MakeUndeliveredFuture<TEvWritePersistentBufferResult>();
    }
    return UnderlyingTransport
        ->WriteToPBuffer(connection, selector, lsn, instruction, data, span);
}

void TTransportChaosInjector::WriteToManyPBuffers(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const ui64 lsn,
    const NKikimr::NDDisk::TWriteInstruction instruction,
    TVector<NKikimrBlobStorage::NDDisk::TDDiskId> persistentBufferIds,
    TDuration replyTimeout,
    const TGuardedSgList& data,
    std::shared_ptr<NWilson::TSpan> span,
    TWriteToManyPBuffersCallback callback)
{
    Y_ABORT_UNLESS(!persistentBufferIds.empty());

    if (IsNodeDisabled(connection.DDiskId.NodeId)) {
        const auto response = MakeWritePersistentBuffersResult(
            TReplyStatus::ERROR,
            UndeliveryErrorMessage,
            std::span(persistentBufferIds.data(), 1));
        callback(response->Record, std::move(span));
        return;
    }
    UnderlyingTransport->WriteToManyPBuffers(
        connection,
        selector,
        lsn,
        instruction,
        std::move(persistentBufferIds),
        replyTimeout,
        data,
        std::move(span),
        std::move(callback));
}

NThreading::TFuture<IStorageTransport::TEvWriteResult>
TTransportChaosInjector::WriteToDDisk(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const NKikimr::NDDisk::TWriteInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    if (IsNodeDisabled(connection.DDiskId.NodeId)) {
        return MakeUndeliveredFuture<TEvWriteResult>();
    }
    return UnderlyingTransport
        ->WriteToDDisk(connection, selector, instruction, data, span);
}

NThreading::TFuture<IStorageTransport::TEvSyncResult>
TTransportChaosInjector::SyncWithPBuffer(
    const THostConnection& pbufferConnection,
    const THostConnection& ddiskConnection,
    TVector<NKikimr::NDDisk::TBlockSelector> selectors,
    TVector<TPBufferKey> pBufferKeys,
    NWilson::TSpan* span)
{
    if (IsNodeDisabled(pbufferConnection.DDiskId.NodeId) ||
        IsNodeDisabled(ddiskConnection.DDiskId.NodeId))
    {
        return MakeUndeliveredFuture<TEvSyncResult>();
    }
    return UnderlyingTransport->SyncWithPBuffer(
        pbufferConnection,
        ddiskConnection,
        std::move(selectors),
        std::move(pBufferKeys),
        span);
}

NThreading::TFuture<IStorageTransport::TEvErasePersistentBufferResult>
TTransportChaosInjector::BatchEraseFromPBuffer(
    const THostConnection& connection,
    TVector<TPBufferKey> pBufferKeys,
    NWilson::TSpan* span)
{
    if (IsNodeDisabled(connection.DDiskId.NodeId)) {
        return MakeUndeliveredFuture<TEvErasePersistentBufferResult>();
    }
    return UnderlyingTransport->BatchEraseFromPBuffer(
        connection,
        std::move(pBufferKeys),
        span);
}

NThreading::TFuture<IStorageTransport::TEvErasePersistentBufferResult>
TTransportChaosInjector::BarrierEraseFromPBuffer(
    const THostConnection& connection,
    const ui64 lsn,
    NWilson::TSpan* span)
{
    if (IsNodeDisabled(connection.DDiskId.NodeId)) {
        return MakeUndeliveredFuture<TEvErasePersistentBufferResult>();
    }
    return UnderlyingTransport->BarrierEraseFromPBuffer(connection, lsn, span);
}

NThreading::TFuture<IStorageTransport::TEvListPersistentBufferResult>
TTransportChaosInjector::ListPBufferEntries(const THostConnection& connection)
{
    if (IsNodeDisabled(connection.DDiskId.NodeId)) {
        return MakeUndeliveredFuture<TEvListPersistentBufferResult>();
    }
    return UnderlyingTransport->ListPBufferEntries(connection);
}

NThreading::TFuture<IStorageTransport::TEvDeleteTabletChunksResult>
TTransportChaosInjector::DeleteTabletChunks(const THostConnection& connection)
{
    if (IsNodeDisabled(connection.DDiskId.NodeId)) {
        return MakeUndeliveredFuture<TEvDeleteTabletChunksResult>();
    }
    return UnderlyingTransport->DeleteTabletChunks(connection);
}

bool TTransportChaosInjector::IsNodeDisabled(ui32 nodeId) const
{
    return DisabledNodes.AtomicLoad()->NodeIds.contains(nodeId);
}

////////////////////////////////////////////////////////////////////////////////

TTransportWithChaosInjectorControlPtr CreateTransportChaosInjector(
    TStorageTransportPtr underlyingTransport)
{
    return std::make_shared<TTransportChaosInjector>(
        std::move(underlyingTransport));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
