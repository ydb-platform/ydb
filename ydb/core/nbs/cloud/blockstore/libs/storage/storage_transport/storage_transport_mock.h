#pragma once

#include "storage_transport.h"

#include <ydb/core/protos/blobstorage_ddisk.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/map.h>
#include <util/generic/yexception.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

// Deterministic mock of IStorageTransport for DirectBlockGroup tests.
//
// Capabilities:
//  - Connect(): immediate successful TEvConnectResult by default (host becomes
//    Locked right away), or a deferred future via a TPromise the test resolves
//    later (host stays NotLocked until then).
//  - ReadFromDDisk/WriteToDDisk/ReadFromPBuffer/WriteToPBuffer: configurable
//    reply status (OK by default).
//  - ListPBufferEntries: empty successful result (otherwise Run() would hang
//    inside DoEstablishConnections -> DoListPBuffers).
//  - Everything else aborts until a test actually needs it.
class TStorageTransportMock: public IStorageTransport
{
public:
    using EConnectionType = THostConnection::EConnectionType;
    using TReplyStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;
    using TReplyStatusE = NKikimrBlobStorage::NDDisk::TReplyStatus_E;
    using TDDiskId = NKikimr::NBsController::TDDiskId;
    using TConnectPromise = NThreading::TPromise<TEvConnectResult>;

    // Default reply status used for immediate (non-pending) responses.
    TReplyStatusE DefaultConnectStatus = TReplyStatus::OK;
    TReplyStatusE ReadFromDDiskStatus = TReplyStatus::OK;
    TReplyStatusE WriteToDDiskStatus = TReplyStatus::OK;
    TReplyStatusE ReadFromPBufferStatus = TReplyStatus::OK;
    TReplyStatusE WriteToPBufferStatus = TReplyStatus::OK;

    // DDiskInstanceGuid reported in an immediate successful connect.
    ui64 DefaultDDiskInstanceGuid = 1;

    TStorageTransportMock() = default;
    ~TStorageTransportMock() override = default;

    // Builds a successful connect result with the given instance guid.
    [[nodiscard]] static TEvConnectResult MakeConnectResult(
        ui64 ddiskInstanceGuid = 1,
        TReplyStatusE status = TReplyStatus::OK);

    // Marks the connection for (type, ddiskId) as pending: the next Connect()
    // returns an unresolved future. The returned promise must be resolved by
    // the test (e.g. with MakeConnectResult(...)) to unblock the host.
    TConnectPromise SetPendingConnect(
        EConnectionType type,
        const TDDiskId& ddiskId);

    [[nodiscard]] TVector<NKikimr::NDDisk::TQueryCredentials>
    GetConnectCredentials(EConnectionType type, const TDDiskId& ddiskId) const;

    NThreading::TFuture<TEvConnectResult> Connect(
        const THostConnection& connection) override;

    NThreading::TFuture<TEvReadPersistentBufferResult> ReadFromPBuffer(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TReadInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan* span) override;

    NThreading::TFuture<TEvReadResult> ReadFromDDisk(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const NKikimr::NDDisk::TReadInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan* span) override;

    NThreading::TFuture<TEvWritePersistentBufferResult> WriteToPBuffer(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan* span) override;

    void WriteToManyPBuffers(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        TVector<NKikimrBlobStorage::NDDisk::TDDiskId> persistentBufferIds,
        TDuration replyTimeout,
        const TGuardedSgList& data,
        std::shared_ptr<NWilson::TSpan> span,
        TWriteToManyPBuffersCallback callback) override;

    NThreading::TFuture<TEvWriteResult> WriteToDDisk(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan* span) override;

    NThreading::TFuture<TEvSyncResult> SyncWithPBuffer(
        const THostConnection& pbufferConnection,
        const THostConnection& ddiskConnection,
        TVector<NKikimr::NDDisk::TBlockSelector> selectors,
        TVector<ui64> lsns,
        NWilson::TSpan* span) override;

    NThreading::TFuture<TEvErasePersistentBufferResult> BatchEraseFromPBuffer(
        const THostConnection& connection,
        TVector<ui64> lsns,
        NWilson::TSpan* span) override;

    NThreading::TFuture<TEvErasePersistentBufferResult> BarrierEraseFromPBuffer(
        const THostConnection& connection,
        ui64 lsn,
        NWilson::TSpan* span) override;

    NThreading::TFuture<TEvListPersistentBufferResult> ListPBufferEntries(
        const THostConnection& connection) override;

private:
    // (connection type, node, pdisk, slot) uniquely identifies a host.
    struct TKey
    {
        int ConnectionType = 0;
        ui32 NodeId = 0;
        ui32 PDiskId = 0;
        ui32 DDiskSlotId = 0;

        auto operator<=>(const TKey& other) const = default;
    };

    [[nodiscard]] static TKey MakeKey(
        EConnectionType type,
        const TDDiskId& ddiskId);

    [[nodiscard]] static TKey MakeKey(const THostConnection& connection);

    TMap<TKey, TConnectPromise> PendingConnects;
    // ConnectCredentials stores the credentials of every Connect() call
    // observed for the given (type, ddiskId), ordered by call.
    TMap<TKey, TVector<NKikimr::NDDisk::TQueryCredentials>> ConnectCredentials;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
