#include "storage_transport_mock.h"

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

using namespace NKikimrBlobStorage::NDDisk;

TEvConnectResult TStorageTransportMock::MakeConnectResult(
    ui64 ddiskInstanceGuid,
    TReplyStatusE status)
{
    TEvConnectResult result;
    result.SetStatus(status);
    result.SetDDiskInstanceGuid(ddiskInstanceGuid);
    return result;
}

TStorageTransportMock::TConnectPromise TStorageTransportMock::SetPendingConnect(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    auto promise = NThreading::NewPromise<TEvConnectResult>();
    PendingConnects[MakeKey(type, ddiskId)] = promise;
    return promise;
}

NThreading::TFuture<TEvConnectResult> TStorageTransportMock::Connect(
    const THostConnection& connection)
{
    const auto key = MakeKey(connection);
    if (auto it = PendingConnects.find(key); it != PendingConnects.end()) {
        return it->second.GetFuture();
    }
    return NThreading::MakeFuture(
        MakeConnectResult(DefaultDDiskInstanceGuid, DefaultConnectStatus));
}

NThreading::TFuture<TEvReadPersistentBufferResult>
TStorageTransportMock::ReadFromPBuffer(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const ui64 lsn,
    const NKikimr::NDDisk::TReadInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    Y_UNUSED(connection, selector, lsn, instruction, data, span);

    TEvReadPersistentBufferResult result;
    result.SetStatus(ReadFromPBufferStatus);
    return NThreading::MakeFuture(std::move(result));
}

NThreading::TFuture<TEvReadResult> TStorageTransportMock::ReadFromDDisk(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const NKikimr::NDDisk::TReadInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    Y_UNUSED(connection, selector, instruction, data, span);

    TEvReadResult result;
    result.SetStatus(ReadFromDDiskStatus);
    return NThreading::MakeFuture(std::move(result));
}

NThreading::TFuture<TEvWritePersistentBufferResult>
TStorageTransportMock::WriteToPBuffer(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const ui64 lsn,
    const NKikimr::NDDisk::TWriteInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    Y_UNUSED(connection, selector, lsn, instruction, data, span);

    TEvWritePersistentBufferResult result;
    result.SetStatus(WriteToPBufferStatus);
    return NThreading::MakeFuture(std::move(result));
}

void TStorageTransportMock::WriteToManyPBuffers(
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
    Y_UNUSED(
        connection,
        selector,
        lsn,
        instruction,
        persistentBufferIds,
        replyTimeout,
        data,
        span,
        callback);

    Y_ABORT("WriteToManyPBuffers is not expected in this test");
}

NThreading::TFuture<TEvWriteResult> TStorageTransportMock::WriteToDDisk(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const NKikimr::NDDisk::TWriteInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    Y_UNUSED(connection, selector, instruction, data, span);

    TEvWriteResult result;
    result.SetStatus(WriteToDDiskStatus);
    return NThreading::MakeFuture(std::move(result));
}

NThreading::TFuture<TEvSyncResult> TStorageTransportMock::SyncWithPBuffer(
    const THostConnection& pbufferConnection,
    const THostConnection& ddiskConnection,
    TVector<NKikimr::NDDisk::TBlockSelector> selectors,
    TVector<ui64> lsns,
    NWilson::TSpan* span)
{
    Y_UNUSED(pbufferConnection, ddiskConnection, selectors, lsns, span);

    Y_ABORT("SyncWithPBuffer is not expected in this test");
}

NThreading::TFuture<TEvErasePersistentBufferResult>
TStorageTransportMock::BatchEraseFromPBuffer(
    const THostConnection& connection,
    TVector<NKikimr::NDDisk::TBlockSelector> selectors,
    TVector<ui64> lsns,
    NWilson::TSpan* span)
{
    Y_UNUSED(connection, selectors, lsns, span);

    Y_ABORT("BatchEraseFromPBuffer is not expected in this test");
}

NThreading::TFuture<TEvErasePersistentBufferResult>
TStorageTransportMock::BarrierEraseFromPBuffer(
    const THostConnection& connection,
    ui64 lsn,
    NWilson::TSpan* span)
{
    Y_UNUSED(connection, lsn, span);

    Y_ABORT("BarrierEraseFromPBuffer is not expected in this test");
}

NThreading::TFuture<TEvListPersistentBufferResult>
TStorageTransportMock::ListPBufferEntries(const THostConnection& connection)
{
    Y_UNUSED(connection);

    TEvListPersistentBufferResult result;
    result.SetStatus(TReplyStatus::OK);
    return NThreading::MakeFuture(std::move(result));
}

TStorageTransportMock::TKey TStorageTransportMock::MakeKey(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    return TKey{
        .ConnectionType = static_cast<int>(type),
        .NodeId = ddiskId.NodeId,
        .PDiskId = ddiskId.PDiskId,
        .DDiskSlotId = ddiskId.DDiskSlotId};
}

TStorageTransportMock::TKey TStorageTransportMock::MakeKey(
    const THostConnection& connection)
{
    return MakeKey(connection.ConnectionType, connection.DDiskId);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
