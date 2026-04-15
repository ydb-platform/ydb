#pragma once

#include "storage_transport.h"

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

class TICStorageTransport: public IStorageTransport
{
public:
    explicit TICStorageTransport(NActors::TActorSystem* actorSystem);

    ~TICStorageTransport() override = default;

    NThreading::TFuture<TEvConnectResult> Connect(
        const THostConnection& connection) override;

    NThreading::TFuture<TEvReadPersistentBufferResult> ReadFromPBuffer(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TReadInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan& span) override;

    NThreading::TFuture<TEvReadResult> ReadFromDDisk(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const NKikimr::NDDisk::TReadInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan& span) override;

    NThreading::TFuture<TEvWritePersistentBufferResult> WriteToPBuffer(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan& span) override;

    NThreading::TFuture<TEvWriteToManyPersistentBuffersResult>
    WriteToManyPBuffers(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        TVector<NKikimrBlobStorage::NDDisk::TDDiskId> persistentBufferIds,
        TDuration replyTimeout,
        const TGuardedSgList& data,
        NWilson::TSpan& span) override;

    NThreading::TFuture<TEvWriteResult> WriteToDDisk(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan& span) override;

    NThreading::TFuture<TEvSyncWithPersistentBufferResult> SyncWithPBuffer(
        const THostConnection& pbufferConnection,
        const THostConnection& ddiskConnection,
        TVector<NKikimr::NDDisk::TBlockSelector> selectors,
        TVector<ui64> lsns,
        NWilson::TSpan& span) override;

    NThreading::TFuture<TEvErasePersistentBufferResult> EraseFromPBuffer(
        const THostConnection& connection,
        TVector<NKikimr::NDDisk::TBlockSelector> selectors,
        TVector<ui64> lsns,
        NWilson::TSpan& span) override;

    NThreading::TFuture<TEvListPersistentBufferResult> ListPBufferEntries(
        const THostConnection& connection) override;

private:
    using EConnectionType = THostConnection::EConnectionType;

    NActors::TActorSystem* const ActorSystem;
    const NActors::TActorId ICStorageTransportActorId;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
