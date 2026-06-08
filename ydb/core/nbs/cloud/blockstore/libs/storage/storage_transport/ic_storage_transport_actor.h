#pragma once

#include "ddisk_helpers.h"
#include "ic_storage_transport_events.h"

#include <ydb/core/blobstorage/ddisk/ddisk.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

class TICStorageTransportActor
    : public NActors::TActorBootstrapped<TICStorageTransportActor>
{
private:
    ui64 RequestIdGenerator = 0;

    THashMap<ui64, std::unique_ptr<TEvTransportPrivate::TEvConnect>>
        ConnectRequests;

    THashMap<ui64, std::unique_ptr<TEvTransportPrivate::TEvReadFromPBuffer>>
        ReadFromPBufferRequests;

    THashMap<ui64, std::unique_ptr<TEvTransportPrivate::TEvReadFromDDisk>>
        ReadFromDDiskRequests;

    THashMap<ui64, std::unique_ptr<TEvTransportPrivate::TEvWriteToPBuffer>>
        WriteToPBufferRequests;

    THashMap<ui64, std::unique_ptr<TEvTransportPrivate::TEvWriteToDDisk>>
        WriteToDDiskRequests;

    THashMap<ui64, std::unique_ptr<TEvTransportPrivate::TEvSyncWithPBuffer>>
        FlushFromPBufferRequests;

    THashMap<
        ui64,
        std::unique_ptr<TEvTransportPrivate::TEvBatchEraseFromPBuffer>>
        BatchEraseFromPBufferRequests;

    THashMap<
        ui64,
        std::unique_ptr<TEvTransportPrivate::TEvBarrierEraseFromPBuffer>>
        BarrierEraseFromPBufferRequests;

    THashMap<ui64, std::unique_ptr<TEvTransportPrivate::TEvListPBufferEntries>>
        ListPBufferEntriesRequests;

    struct TWriteToManyPBuffersReqInfo
    {
        std::unique_ptr<TEvTransportPrivate::TEvWriteToManyPBuffers> Request;
        TSet<NKikimrBlobStorage::NDDisk::TDDiskId, TDDiskIdLess> WaitingReplies;
    };

    THashMap<ui64, TWriteToManyPBuffersReqInfo> WriteToManyPBuffersRequests;

public:
    TICStorageTransportActor() = default;

    ~TICStorageTransportActor() override;

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleConnect(
        const TEvTransportPrivate::TEvConnect::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleConnectUndelivery(
        const NKikimr::NDDisk::TEvConnect::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleConnectResult(
        const NKikimr::NDDisk::TEvConnectResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWritePersistentBuffer(
        const TEvTransportPrivate::TEvWriteToPBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleWritePersistentBufferUndelivery(
        const NKikimr::NDDisk::TEvWritePersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleWritePersistentBufferResult(
        const NKikimr::NDDisk::TEvWritePersistentBufferResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteToManyPersistentBuffers(
        const TEvTransportPrivate::TEvWriteToManyPBuffers::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleWriteToManyPersistentBuffersUndelivery(
        const NKikimr::NDDisk::TEvWritePersistentBuffers::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleWriteToManyPersistentBuffersResult(
        const NKikimr::NDDisk::TEvWritePersistentBuffersResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteToDDisk(
        const TEvTransportPrivate::TEvWriteToDDisk::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleWriteToDDiskUndelivery(
        const NKikimr::NDDisk::TEvWrite::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleWriteToDDiskResult(
        const NKikimr::NDDisk::TEvWriteResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleBatchErasePersistentBuffer(
        const TEvTransportPrivate::TEvBatchEraseFromPBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleErasePersistentBuffer(
        const TEvTransportPrivate::TEvBarrierEraseFromPBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleErasePersistentBufferUndelivery(
        const NKikimr::NDDisk::TEvBatchErasePersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleErasePersistentBufferResult(
        const NKikimr::NDDisk::TEvErasePersistentBufferResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadPersistentBuffer(
        const TEvTransportPrivate::TEvReadFromPBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleReadPersistentBufferUndelivery(
        const NKikimr::NDDisk::TEvReadPersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleReadPersistentBufferResult(
        const NKikimr::NDDisk::TEvReadPersistentBufferResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRead(
        const TEvTransportPrivate::TEvReadFromDDisk::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleReadUndelivery(
        const NKikimr::NDDisk::TEvRead::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleReadResult(
        const NKikimr::NDDisk::TEvReadResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSyncWithPersistentBuffer(
        const TEvTransportPrivate::TEvSyncWithPBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleSyncWithPersistentBufferUndelivery(
        const NKikimr::NDDisk::TEvSyncWithPersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleSyncWithPersistentBufferResult(
        const NKikimr::NDDisk::TEvSyncWithPersistentBufferResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleListPersistentBuffer(
        const TEvTransportPrivate::TEvListPBufferEntries::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleListPersistentBufferUndelivery(
        const NKikimr::NDDisk::TEvListPersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleListPersistentBufferResult(
        const NKikimr::NDDisk::TEvListPersistentBufferResult::TPtr& ev,
        const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId CreateTransportActor();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
