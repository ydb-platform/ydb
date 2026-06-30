#include "storage_transport_mock.h"

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

using namespace NKikimrBlobStorage::NDDisk;

TStorageTransportMock::TStorageTransportMock(ui32 baseNodeId)
{
    // Same layout as the test fixture's MakeDDiskIds: every host is
    // distinguished by node/slot, PBuffers live on the next block of nodes.
    DDiskIds.reserve(DirectBlockGroupHostCount);
    PBufferIds.reserve(DirectBlockGroupHostCount);
    for (ui32 i = 0; i < DirectBlockGroupHostCount; ++i) {
        DDiskIds.emplace_back(baseNodeId + i, 1, i);
        PBufferIds.emplace_back(
            baseNodeId + DirectBlockGroupHostCount + i,
            1,
            i);
    }
}

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

TVector<NKikimr::NDDisk::TQueryCredentials>
TStorageTransportMock::GetConnectCredentials(
    EConnectionType type,
    const TDDiskId& ddiskId) const
{
    const auto key = MakeKey(type, ddiskId);
    if (auto it = ConnectCredentials.find(key); it != ConnectCredentials.end())
    {
        return it->second;
    }
    return {};
}

void TStorageTransportMock::FireDisconnect(
    EConnectionType type,
    const TDDiskId& ddiskId,
    ui32 nodeId)
{
    const auto key = MakeKey(type, ddiskId);

    if (auto it = StoredDisconnectCBs.find(key);
        it != StoredDisconnectCBs.end())
    {
        auto cb = std::move(it->second);
        StoredDisconnectCBs.erase(it);
        cb(nodeId);
    }

    if (auto it = PendingReadsFromDDisk.find(key);
        it != PendingReadsFromDDisk.end())
    {
        TEvReadResult err;
        err.SetStatus(TReplyStatus::OUTDATED);
        err.SetErrorReason("Session broken");
        auto promise = std::move(it->second);
        PendingReadsFromDDisk.erase(it);
        promise.SetValue(std::move(err));
    }

    if (auto it = PendingWritesToDDisk.find(key);
        it != PendingWritesToDDisk.end())
    {
        TEvWriteResult err;
        err.SetStatus(TReplyStatus::OUTDATED);
        err.SetErrorReason("Session broken");
        auto promise = std::move(it->second);
        PendingWritesToDDisk.erase(it);
        promise.SetValue(std::move(err));
    }
}

TStorageTransportMock::TReadPromise
TStorageTransportMock::SetPendingReadFromDDisk(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    auto promise = NThreading::NewPromise<TEvReadResult>();
    PendingReadsFromDDisk[MakeKey(type, ddiskId)] = promise;
    return promise;
}

TStorageTransportMock::TWritePromise
TStorageTransportMock::SetPendingWriteToDDisk(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    auto promise = NThreading::NewPromise<TEvWriteResult>();
    PendingWritesToDDisk[MakeKey(type, ddiskId)] = promise;
    return promise;
}

NThreading::TFuture<TEvConnectResult> TStorageTransportMock::Connect(
    const THostConnection& connection,
    TDisconnectCB disconnectCB)
{
    const auto key = MakeKey(connection);
    ConnectCredentials[key].push_back(connection.Credentials);
    if (disconnectCB) {
        StoredDisconnectCBs[key] = std::move(disconnectCB);
    }
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
    Y_UNUSED(selector, instruction, data, span);

    const auto key = MakeKey(connection);
    if (auto it = PendingReadsFromDDisk.find(key);
        it != PendingReadsFromDDisk.end())
    {
        return it->second.GetFuture();
    }

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
    Y_UNUSED(selector, instruction, data, span);

    const auto key = MakeKey(connection);
    if (auto it = PendingWritesToDDisk.find(key);
        it != PendingWritesToDDisk.end())
    {
        return it->second.GetFuture();
    }

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
    TVector<ui64> lsns,
    NWilson::TSpan* span)
{
    Y_UNUSED(connection, lsns, span);

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
