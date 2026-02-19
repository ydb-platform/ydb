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
    IStorageTransport() = default;

    virtual ~IStorageTransport() = default;

    virtual NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvConnectResult>
    Connect(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials) = 0;

    virtual NThreading::TFuture<
        NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult>
    WritePersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        TGuardedSgList data,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<
        NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult>
    ErasePersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const ui64 lsn,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<
        NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult>
    ReadPersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TReadInstruction instruction,
        TGuardedSgList data,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvReadResult> Read(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const NKikimr::NDDisk::TReadInstruction instruction,
        TGuardedSgList data,
        NWilson::TSpan& span) = 0;

    virtual NThreading::TFuture<
        NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult>
    SyncWithPersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const ui64 lsn,
        const std::tuple<ui32, ui32, ui32> ddiskId,
        const ui64 ddiskInstanceGuid,
        NWilson::TSpan& span) = 0;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
