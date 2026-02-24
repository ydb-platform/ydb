#pragma once

#include "ic_storage_transport_events.h"
#include "storage_transport.h"

#include <ydb/core/blobstorage/ddisk/ddisk.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

class TICStorageTransport: public IStorageTransport
{
private:
    NActors::TActorSystem* const ActorSystem;
    NActors::TActorId ICStorageTransportActorId;

public:
    explicit TICStorageTransport(NActors::TActorSystem* actorSystem);

    ~TICStorageTransport() override = default;

    NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvConnectResult> Connect(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials) override;

    NThreading::TFuture<
        NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult>
    WritePersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        TGuardedSgList data,
        NWilson::TSpan& span) override;

    NThreading::TFuture<
        NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult>
    ErasePersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const ui64 lsn,
        NWilson::TSpan& span) override;

    NThreading::TFuture<
        NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult>
    ReadPersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TReadInstruction instruction,
        TGuardedSgList data,
        NWilson::TSpan& span) override;

    NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvReadResult> Read(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const NKikimr::NDDisk::TReadInstruction instruction,
        TGuardedSgList data,
        NWilson::TSpan& span) override;

    NThreading::TFuture<
        NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult>
    SyncWithPersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const NKikimr::NDDisk::TBlockSelector selector,
        const ui64 lsn,
        const std::tuple<ui32, ui32, ui32> ddiskId,
        const ui64 ddiskInstanceGuid,
        NWilson::TSpan& span) override;

    NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult> ListPersistentBuffer(
        const NActors::TActorId serviceId,
        const NKikimr::NDDisk::TQueryCredentials credentials,
        const ui64 requestId) override;
};

////////////////////////////////////////////////////////////////////////////////

class TICStorageTransportActor
    : public NActors::TActorBootstrapped<TICStorageTransportActor>
{
private:
    ui64 RequestIdGenerator = 0;

    THashMap<ui64, TEvICStorageTransportPrivate::TEvConnect>
        ConnectEventsByRequestId;
    THashMap<ui64, TEvICStorageTransportPrivate::TEvWritePersistentBuffer>
        WritePersistentBufferEventsByRequestId;
    THashMap<ui64, TEvICStorageTransportPrivate::TEvErasePersistentBuffer>
        ErasePersistentBufferEventsByRequestId;
    THashMap<ui64, TEvICStorageTransportPrivate::TEvReadPersistentBuffer>
        ReadPersistentBufferEventsByRequestId;
    THashMap<ui64, TEvICStorageTransportPrivate::TEvRead> ReadEventsByRequestId;
    THashMap<ui64, TEvICStorageTransportPrivate::TEvSyncWithPersistentBuffer>
        SyncEventsByRequestId;
    TMap<ui64, TEvICStorageTransportPrivate::TEvListPersistentBuffer>
        ListPersistentBufferEventsByRequestId;

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

    void HandleSyncWithPersistentBuffer(
        const TEvICStorageTransportPrivate::TEvSyncWithPersistentBuffer::TPtr&
            ev,
        const NActors::TActorContext& ctx);

    void HandleSyncWithPersistentBufferResult(
        const NKikimr::NDDisk::TEvSyncWithPersistentBufferResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleListPersistentBuffer(
        const TEvICStorageTransportPrivate::TEvListPersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleListPersistentBufferResult(
        const NKikimr::NDDisk::TEvListPersistentBufferResult::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
