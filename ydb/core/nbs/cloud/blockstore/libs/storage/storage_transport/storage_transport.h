#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/pbuffer_key.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/protos/blobstorage_ddisk.pb.h>

#include <functional>
#include <memory>

namespace NYdb::NBS {

struct TDiskDescription;

}   // namespace NYdb::NBS

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

    [[nodiscard]] TString DebugPrint() const;
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
    using TEvSyncResult = NKikimrBlobStorage::NDDisk::TEvSyncResult;
    using TEvErasePersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult;
    using TEvListPersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult;
    using TEvDeleteTabletChunksResult =
        NKikimrBlobStorage::NDDisk::TEvDeleteTabletChunksResult;

    // Callback type for WriteToManyPBuffers: called once per response received.
    // May be called multiple times if the underlying transport delivers more
    // than one response for the same request.
    using TWriteToManyPBuffersCallback = std::function<void(
        const TEvWriteToManyPersistentBuffersResult& result,
        std::shared_ptr<NWilson::TSpan> span)>;

    virtual ~IStorageTransport() = default;

    struct TConnectResultFutures
    {
        NThreading::TFuture<TEvConnectResult> ConnectFuture;
        NThreading::TFuture<ui32> DisconnectFuture;
    };

    virtual TConnectResultFutures Connect(
        const THostConnection& connection) = 0;

    virtual NThreading::TFuture<TEvReadPersistentBufferResult> ReadFromPBuffer(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const TPBufferKey pBufferKey,
        const NKikimr::NDDisk::TReadInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan* span) = 0;

    virtual NThreading::TFuture<TEvReadResult> ReadFromDDisk(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const NKikimr::NDDisk::TReadInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan* span) = 0;

    virtual NThreading::TFuture<TEvWritePersistentBufferResult> WriteToPBuffer(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan* span) = 0;

    // Sends a write request to many persistent buffers.
    // The callback is invoked once per response received from the transport
    // layer (may be called more than once for the same request).
    virtual void WriteToManyPBuffers(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const ui64 lsn,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        TVector<NKikimrBlobStorage::NDDisk::TDDiskId> persistentBufferIds,
        TDuration replyTimeout,
        const TGuardedSgList& data,
        std::shared_ptr<NWilson::TSpan> span,
        TWriteToManyPBuffersCallback callback) = 0;

    virtual NThreading::TFuture<TEvWriteResult> WriteToDDisk(
        const THostConnection& connection,
        const NKikimr::NDDisk::TBlockSelector& selector,
        const NKikimr::NDDisk::TWriteInstruction instruction,
        const TGuardedSgList& data,
        NWilson::TSpan* span) = 0;

    virtual NThreading::TFuture<TEvSyncResult> SyncWithPBuffer(
        const THostConnection& pbufferConnection,
        const THostConnection& ddiskConnection,
        TVector<NKikimr::NDDisk::TBlockSelector> selectors,
        TVector<TPBufferKey> pBufferKeys,
        NWilson::TSpan* span) = 0;

    virtual NThreading::TFuture<TEvErasePersistentBufferResult>
    BatchEraseFromPBuffer(
        const THostConnection& connection,
        TVector<TPBufferKey> pBufferKeys,
        NWilson::TSpan* span) = 0;

    virtual NThreading::TFuture<TEvErasePersistentBufferResult>
    BarrierEraseFromPBuffer(
        const THostConnection& connection,
        ui64 lsn,
        NWilson::TSpan* span) = 0;

    virtual NThreading::TFuture<TEvListPersistentBufferResult>
    ListPBufferEntries(const THostConnection& connection) = 0;

    virtual NThreading::TFuture<TEvDeleteTabletChunksResult> DeleteTabletChunks(
        const THostConnection& connection) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Controls node availability for failure simulation.
class IChaosInjectorControl
{
public:
    virtual ~IChaosInjectorControl() = default;

    // Makes subsequent requests to nodeId fail with an undelivery error.
    virtual void DisableNode(ui32 nodeId) = 0;

    // Makes subsequent requests to nodeId use the underlying transport.
    virtual void EnableNode(ui32 nodeId) = 0;

    // Returns true when requests to nodeId are configured to fail.
    [[nodiscard]] virtual bool IsNodeDisabled(ui32 nodeId) const = 0;
};

// Combines storage transport operations with node-failure controls.
class ITransportWithChaosInjectorControl
    : public IStorageTransport
    , public IChaosInjectorControl
{
};

////////////////////////////////////////////////////////////////////////////////

// Creates either a direct-session or actor-based storage transport.
[[nodiscard]] TStorageTransportPtr CreateStorageTransport(
    NActors::TActorSystem* actorSystem,
    const TDiskDescription& diskDescription,
    ui32 dbgIndex,
    bool useDirectSessionTransport,
    bool enableChecksums);

// Wraps a storage transport with a node-failure simulation layer.
[[nodiscard]] TTransportWithChaosInjectorControlPtr
CreateTransportChaosInjector(TStorageTransportPtr underlyingTransport);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
