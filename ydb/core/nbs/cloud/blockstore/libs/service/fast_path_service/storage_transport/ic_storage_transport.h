#pragma once

#include "storage_transport.h"
#include "ic_storage_transport_events.h"

#include <ydb/core/blobstorage/ddisk/ddisk.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TICStorageTransport : public IStorageTransport
{
private:
    NActors::TActorId ICStorageTransportActorId;
public:
    TICStorageTransport();

    ~TICStorageTransport() override = default;

    NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvConnectResult> Connect(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const ui64 requestId) override;

    NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult> WritePersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        TGuardedSgList data,
        const ui64 requestId) override;

    NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvFlushPersistentBufferResult> FlushPersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const ui64 lsn,
        const std::tuple<ui32, ui32, ui32> ddiskId,
        const ui64 ddiskInstanceGuid,
        const ui64 requestId) override;

    NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult> ErasePersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const ui64 lsn,
        const ui64 requestId) override;

    NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult> ReadPersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TReadInstruction instruction,
        TGuardedSgList data,
        const ui64 requestId) override;

    NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvReadResult> Read(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const NKikimr::NDDisk::TReadInstruction instruction,
        TGuardedSgList data,
        const ui64 requestId) override;
};

////////////////////////////////////////////////////////////////////////////////

class TICStorageTransportActor
    : public NActors::TActorBootstrapped<TICStorageTransportActor>
{
private:
    using TPrivateEvents = std::variant<
        TEvICStorageTransportPrivate::TEvConnect,
        TEvICStorageTransportPrivate::TEvWritePersistentBuffer,
        TEvICStorageTransportPrivate::TEvFlushPersistentBuffer,
        TEvICStorageTransportPrivate::TEvErasePersistentBuffer,
        TEvICStorageTransportPrivate::TEvReadPersistentBuffer,
        TEvICStorageTransportPrivate::TEvRead
    >;

    std::unordered_map<ui64, TPrivateEvents> PrivateEventsByRequestId;
public:
    TICStorageTransportActor() = default;

    void Bootstrap(const NActors::TActorContext& ctx);
private:
    STFUNC(StateWork);

    void HandleConnect(
        const TEvICStorageTransportPrivate::TEvConnect::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleConnectResult(
        const NKikimr::NDDisk::TEvConnectResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWritePersistentBuffer(
        const TEvICStorageTransportPrivate::TEvWritePersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWritePersistentBufferResult(
        const NKikimr::NDDisk::TEvWritePersistentBufferResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFlushPersistentBuffer(
        const TEvICStorageTransportPrivate::TEvFlushPersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFlushPersistentBufferResult(
        const NKikimr::NDDisk::TEvFlushPersistentBufferResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleErasePersistentBuffer(
        const TEvICStorageTransportPrivate::TEvErasePersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleErasePersistentBufferResult(
        const NKikimr::NDDisk::TEvErasePersistentBufferResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadPersistentBuffer(
        const TEvICStorageTransportPrivate::TEvReadPersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadPersistentBufferResult(
        const NKikimr::NDDisk::TEvReadPersistentBufferResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRead(
        const TEvICStorageTransportPrivate::TEvRead::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadResult(
        const NKikimr::NDDisk::TEvReadResult::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NYdb::NBS::NBlockStore
