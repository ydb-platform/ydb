#pragma once

#include "direct_session_registry.h"
#include "ic_storage_transport.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/disk_description.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

// IStorageTransport that sends datapath events via IDirectSession from the
// calling (NBS executor) thread, bypassing the transport actor mailbox.
// Connect / disconnect still go through TICStorageTransport (actor path); the
// actor publishes IDirectSession handles into DirectSessionRegistry together
// with a long-lived reply ActorId and TSessionReplyRouter.
//
// When no session is registered for the destination node (local peer, mock IC,
// brief window before TEvNodeConnected), datapath calls fall back to the
// actor-based base class.
class TICDirectStorageTransport: public TICStorageTransport
{
public:
    TICDirectStorageTransport(
        NActors::TActorSystem* actorSystem,
        NActors::TActorId icStorageTransportActorId,
        std::shared_ptr<TDirectSessionRegistry> directSessionRegistry,
        bool enableChecksums);

    ~TICDirectStorageTransport() override = default;

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

private:
    using EConnectionType = THostConnection::EConnectionType;

    const std::shared_ptr<TDirectSessionRegistry> DirectSessionRegistry;
    const bool EnableChecksums;

    [[nodiscard]] TSessionEntry GetSessionEntry(
        const THostConnection& connection) const;
};

////////////////////////////////////////////////////////////////////////////////

// Convenience factory: creates the shared registry, transport actor and
// TICDirectStorageTransport bound together.
[[nodiscard]] std::unique_ptr<IStorageTransport> CreateDirectStorageTransport(
    NActors::TActorSystem* actorSystem,
    const TDiskDescription& diskDescription,
    ui32 dbgIndex,
    bool enableChecksums);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
