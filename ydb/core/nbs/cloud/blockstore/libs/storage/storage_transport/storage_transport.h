#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/protos/blobstorage_ddisk.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

struct THostConnection
{
    enum class EConnectionType
    {
        DDisk,
        PBuffer,
    };

    EConnectionType ConnectionType;
    NKikimr::NBsController::TDDiskId DDiskId;
    NKikimr::NDDisk::TQueryCredentials Credentials;

    [[nodiscard]] NActors::TActorId GetServiceId() const;
    [[nodiscard]] bool IsConnected() const;
};

////////////////////////////////////////////////////////////////////////////////

class IStorageTransport
{
public:
    using TEvConnectResult = NKikimrBlobStorage::NDDisk::TEvConnectResult;
    using TEvReadPersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult;
    using TEvReadResult = NKikimrBlobStorage::NDDisk::TEvReadResult;
    using TEvWritePersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult;
    using TEvWriteToManyPersistentBuffersResult =
        NKikimrBlobStorage::NDDisk::TEvWritePersistentBuffersResult;
    using TEvWriteResult = NKikimrBlobStorage::NDDisk::TEvWriteResult;
    using TEvSyncWithPersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult;
    using TEvErasePersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult;
    using TEvListPersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult;

    IStorageTransport() = default;

    virtual ~IStorageTransport() = default;

    virtual NThreading::TFuture<TEvConnectResult> Connect(
        const THostConnection& connection) = 0;

    virtual NThreading::TFuture<TEvReadPersistentBufferResult> ReadFromPBuffer(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TReadInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<TEvReadResult> ReadFromDDisk(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const NKikimr::NDDisk::TReadInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<TEvWritePersistentBufferResult> WriteToPBuffer(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<TEvWriteToManyPersistentBuffersResult>
    WriteToManyPBuffers(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        TVector<NKikimrBlobStorage::NDDisk::TDDiskId> persistentBufferIds,
        TDuration replyTimeout,
        const TGuardedSgList& data,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<TEvWriteResult> WriteToDDisk(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<TEvSyncWithPersistentBufferResult>
    SyncWithPBuffer(
        const THostConnection& pbufferConnection,
        const THostConnection& ddiskConnection,
        TVector<NKikimr::NDDisk::TBlockSelector> selectors,
        TVector<ui64> lsns,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<TEvErasePersistentBufferResult>
    EraseFromPBuffer(
        const THostConnection& connection,
        TVector<NKikimr::NDDisk::TBlockSelector> selectors,
        TVector<ui64> lsns,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<TEvListPersistentBufferResult>
    ListPBufferEntries(const THostConnection& connection) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
