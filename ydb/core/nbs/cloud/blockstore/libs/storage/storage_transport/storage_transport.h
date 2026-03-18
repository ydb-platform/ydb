#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/protos/blobstorage_ddisk.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

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
    using TEvSyncWithPersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult;
    using TEvErasePersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult;
    using TEvListPersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult;

    IStorageTransport() = default;

    virtual ~IStorageTransport() = default;

    virtual NThreading::TFuture<TEvConnectResult> Connect(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials) = 0;

    virtual NThreading::TFuture<TEvReadPersistentBufferResult> ReadFromPBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TReadInstruction instruction,
        TGuardedSgList data,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<TEvReadResult> ReadFromDDisk(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const NKikimr::NDDisk::TReadInstruction instruction,
        TGuardedSgList data,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<TEvWritePersistentBufferResult>
    WritePersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        TGuardedSgList data,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<TEvSyncWithPersistentBufferResult>
    FlushFromPBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        TVector<NKikimr::NDDisk::TBlockSelector> selectors,
        TVector<ui64> lsns,
        const std::tuple<ui32, ui32, ui32> ddiskId,
        const ui64 ddiskInstanceGuid,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<TEvErasePersistentBufferResult>
    ErasePersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        TVector<NKikimr::NDDisk::TBlockSelector> selectors,
        TVector<ui64> lsns,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<TEvListPersistentBufferResult>
    ListPersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials) = 0;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
