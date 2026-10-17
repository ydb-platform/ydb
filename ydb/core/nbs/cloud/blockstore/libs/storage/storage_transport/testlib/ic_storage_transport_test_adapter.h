#pragma once

#include "ddisk_stub_actor.h"
#include "fake_direct_session.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/direct_session_registry.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_direct_storage_transport.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport.h>

#include <ydb/core/testlib/actors/test_runtime.h>

#include <util/generic/map.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib {

////////////////////////////////////////////////////////////////////////////////

// Test adapter that drives TICDirectStorageTransport (with actor-path fallback)
// over a TTestActorRuntime. Registers DDisk/PersistentBuffer stub actors as
// services and exposes mock-compatible control methods.
//
// By default no IDirectSession is injected, so datapath falls back to the
// transport actor (preserves existing test behaviour). Call
// EnableFakeDirectSession() to exercise the IDirectSession send path.
class TICStorageTransportTestAdapter: public TICDirectStorageTransport
{
public:
    using EConnectionType = THostConnection::EConnectionType;
    using TDDiskId = NKikimr::NBsController::TDDiskId;

    explicit TICStorageTransportTestAdapter(
        NActors::TTestActorRuntime* runtime,
        bool enableChecksums = true);
    ~TICStorageTransportTestAdapter() override = default;

    [[nodiscard]] const TVector<TDDiskId>& GetDDiskIds() const
    {
        return DDiskIds;
    }

    [[nodiscard]] const TVector<TDDiskId>& GetPBufferIds() const
    {
        return PBufferIds;
    }

    [[nodiscard]] ui32 GetNodeId() const
    {
        return NodeId;
    }

    [[nodiscard]] NActors::TActorId GetTransportActorId() const
    {
        return TransportActorId;
    }

    // Inject a TFakeDirectSession for NodeId so datapath uses IDirectSession.
    void EnableFakeDirectSession();

    // Drop the fake session (Send() starts returning false; registry cleared).
    void ShutdownFakeDirectSession();

    // Publish an arbitrary IDirectSession for NodeId (tests / advanced setup).
    void SetDirectSession(std::shared_ptr<NActors::IDirectSession> session);

    // Events successfully sent through the injected fake IDirectSession.
    // Zero when no fake session is enabled.
    [[nodiscard]] ui64 GetFakeDirectSessionSentEventCount() const;

    void SetPendingConnect(EConnectionType type, const TDDiskId& ddiskId);
    void SetPendingReadFromDDisk(EConnectionType type, const TDDiskId& ddiskId);
    void SetPendingWriteToDDisk(EConnectionType type, const TDDiskId& ddiskId);
    void SetPendingWriteToPBuffer(
        EConnectionType type,
        const TDDiskId& ddiskId);
    void SetPendingErase(EConnectionType type, const TDDiskId& ddiskId);
    void SetPendingSync(EConnectionType type, const TDDiskId& ddiskId);
    void SetSplitWriteToManyReplies(
        EConnectionType type,
        const TDDiskId& ddiskId,
        bool split);

    void ReleasePendingReads(EConnectionType type, const TDDiskId& ddiskId);
    void ReleasePendingWrites(EConnectionType type, const TDDiskId& ddiskId);
    void ReleasePendingWritePBuffers(
        EConnectionType type,
        const TDDiskId& ddiskId);
    void ReleasePendingWritePBuffersFirstHalf(
        EConnectionType type,
        const TDDiskId& ddiskId);
    void ReleasePendingErases(EConnectionType type, const TDDiskId& ddiskId);
    void ReleasePendingSyncs(EConnectionType type, const TDDiskId& ddiskId);

    [[nodiscard]] TVector<NKikimr::NDDisk::TQueryCredentials>
    GetConnectCredentials(EConnectionType type, const TDDiskId& ddiskId) const;

    void
    FireDisconnect(EConnectionType type, const TDDiskId& ddiskId, ui32 nodeId);

private:
    struct TBootstrap
    {
        std::shared_ptr<TDirectSessionRegistry> Registry;
        NActors::TActorId ActorId;
    };

    struct TKey
    {
        int ConnectionType = 0;
        ui32 PDiskId = 0;
        ui32 DDiskSlotId = 0;

        auto operator<=>(const TKey& other) const = default;
    };

    TICStorageTransportTestAdapter(
        NActors::TTestActorRuntime* runtime,
        TBootstrap bootstrap,
        bool enableChecksums);

    [[nodiscard]] static TKey MakeKey(
        EConnectionType type,
        const TDDiskId& ddiskId);

    [[nodiscard]] TDDiskStubStatePtr FindState(
        EConnectionType type,
        const TDDiskId& ddiskId) const;

    void RegisterStub(EConnectionType type, const TDDiskId& ddiskId);

    NActors::TTestActorRuntime* const Runtime;
    const ui32 NodeId;
    const NActors::TActorId EdgeActor;
    const NActors::TActorId TransportActorId;
    const std::shared_ptr<TDirectSessionRegistry> Registry;

    TVector<TDDiskId> DDiskIds;
    TVector<TDDiskId> PBufferIds;

    TMap<TKey, TDDiskStubStatePtr> Stubs;
    std::shared_ptr<TFakeDirectSession> FakeDirectSession;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib
