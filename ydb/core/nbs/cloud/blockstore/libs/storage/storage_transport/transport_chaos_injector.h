#pragma once

#include "storage_transport.h"

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/generic/hash_set.h>

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

// Decorates IStorageTransport and simulates undelivery for disabled nodes.
class TTransportChaosInjector final: public ITransportWithChaosInjectorControl
{
public:
    explicit TTransportChaosInjector(TStorageTransportPtr underlyingTransport);

    ~TTransportChaosInjector() override = default;

    // Disables nodeId for subsequent requests.
    void DisableNode(ui32 nodeId) override;

    // Enables nodeId for subsequent requests.
    void EnableNode(ui32 nodeId) override;

    // Returns true when nodeId is disabled.
    [[nodiscard]] bool IsNodeDisabled(ui32 nodeId) const override;

    // Connects through the underlying transport if the node is enabled.
    TConnectResultFutures Connect(const THostConnection& connection) override;

    // Reads from a persistent buffer or returns an undelivery error.
    NThreading::TFuture<TEvReadPersistentBufferResult> ReadFromPBuffer(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        TPBufferKey pBufferKey,
        NKikimr::NDDisk::TReadInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan* span) override;

    // Reads from a DDisk or returns an undelivery error.
    NThreading::TFuture<TEvReadResult> ReadFromDDisk(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        NKikimr::NDDisk::TReadInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan* span) override;

    // Writes to a persistent buffer or returns an undelivery error.
    NThreading::TFuture<TEvWritePersistentBufferResult> WriteToPBuffer(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        ui64 lsn,
        NKikimr::NDDisk::TWriteInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan* span) override;

    // Writes to many persistent buffers or reports coordinator undelivery.
    void WriteToManyPBuffers(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        ui64 lsn,
        NKikimr::NDDisk::TWriteInstruction instruction,
        TVector<NKikimrBlobStorage::NDDisk::TDDiskId> persistentBufferIds,
        TDuration replyTimeout,
        const TGuardedSgList& data,
        std::shared_ptr<NWilson::TSpan> span,
        TWriteToManyPBuffersCallback callback) override;

    // Writes to a DDisk or returns an undelivery error.
    NThreading::TFuture<TEvWriteResult> WriteToDDisk(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        NKikimr::NDDisk::TWriteInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan* span) override;

    // Synchronizes through the destination DDisk or returns undelivery.
    NThreading::TFuture<TEvSyncResult> SyncWithPBuffer(
        const THostConnection& pbufferConnection,
        const THostConnection& ddiskConnection,
        TVector<NKikimr::NDDisk::TBlockSelector> selectors,
        TVector<TPBufferKey> pBufferKeys,
        NWilson::TSpan* span) override;

    // Erases persistent-buffer entries or returns an undelivery error.
    NThreading::TFuture<TEvErasePersistentBufferResult> BatchEraseFromPBuffer(
        const THostConnection& connection,
        TVector<TPBufferKey> pBufferKeys,
        NWilson::TSpan* span) override;

    // Erases a persistent-buffer range or returns an undelivery error.
    NThreading::TFuture<TEvErasePersistentBufferResult> BarrierEraseFromPBuffer(
        const THostConnection& connection,
        ui64 lsn,
        NWilson::TSpan* span) override;

    // Lists persistent-buffer entries or returns an undelivery error.
    NThreading::TFuture<TEvListPersistentBufferResult> ListPBufferEntries(
        const THostConnection& connection) override;

    // Deletes tablet chunks or returns an undelivery error.
    NThreading::TFuture<TEvDeleteTabletChunksResult> DeleteTabletChunks(
        const THostConnection& connection) override;

private:
    struct TDisabledNodes: public TAtomicRefCount<TDisabledNodes>
    {
        THashSet<ui32> NodeIds;
    };

    const TStorageTransportPtr UnderlyingTransport;

    THotSwap<TDisabledNodes> DisabledNodes{MakeIntrusive<TDisabledNodes>()};
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
