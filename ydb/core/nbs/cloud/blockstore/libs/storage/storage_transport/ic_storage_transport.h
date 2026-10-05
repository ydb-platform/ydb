#pragma once

#include "storage_transport.h"

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

class TICStorageTransport: public IStorageTransport
{
public:
    // Owns icStorageTransportActorId. actorSystem must outlive this object.
    TICStorageTransport(
        NActors::TActorSystem* actorSystem,
        NActors::TActorId icStorageTransportActorId);

    ~TICStorageTransport() override;

    TICStorageTransport(const TICStorageTransport&) = delete;
    TICStorageTransport& operator=(const TICStorageTransport&) = delete;
    TICStorageTransport(TICStorageTransport&&) = delete;
    TICStorageTransport& operator=(TICStorageTransport&&) = delete;

    TConnectResultFutures Connect(const THostConnection& connection) override;

    NThreading::TFuture<TEvReadPersistentBufferResult> ReadFromPBuffer(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const TPBufferKey pBufferKey,
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
        TVector<TPBufferKey> pBufferKeys,
        NWilson::TSpan* span) override;

    NThreading::TFuture<TEvErasePersistentBufferResult> BatchEraseFromPBuffer(
        const THostConnection& connection,
        TVector<TPBufferKey> pBufferKeys,
        NWilson::TSpan* span) override;

    NThreading::TFuture<TEvErasePersistentBufferResult> BarrierEraseFromPBuffer(
        const THostConnection& connection,
        ui64 lsn,
        NWilson::TSpan* span) override;

    NThreading::TFuture<TEvListPersistentBufferResult> ListPBufferEntries(
        const THostConnection& connection) override;

    NThreading::TFuture<TEvDeleteTabletChunksResult> DeleteTabletChunks(
        const THostConnection& connection) override;

protected:
    NActors::TActorSystem* const ActorSystem;

private:
    using EConnectionType = THostConnection::EConnectionType;

    // Owned actor: stopped with TEvPoisonPill in the destructor.
    const NActors::TActorId ICStorageTransportActorId;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
