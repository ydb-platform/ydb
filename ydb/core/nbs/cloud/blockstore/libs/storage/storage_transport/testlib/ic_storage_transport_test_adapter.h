#pragma once

#include "ddisk_stub_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport.h>

#include <ydb/core/testlib/actors/test_runtime.h>

#include <util/generic/map.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib {

////////////////////////////////////////////////////////////////////////////////

// Test adapter that drives the real TICStorageTransport /
// TICStorageTransportActor over a TTestActorRuntime. It registers
// DDisk/PersistentBuffer stub actors as services and exposes mock-compatible
// control methods so the DirectBlockGroup disconnect tests exercise the real
// disconnect path instead of duplicated mock logic.
//
// All IStorageTransport methods are inherited from TICStorageTransport, so the
// adapter only adds the test control surface on top of the real transport.
class TICStorageTransportTestAdapter: public TICStorageTransport
{
public:
    using EConnectionType = THostConnection::EConnectionType;
    using TDDiskId = NKikimr::NBsController::TDDiskId;

    explicit TICStorageTransportTestAdapter(
        NActors::TTestActorRuntime* runtime);
    ~TICStorageTransportTestAdapter() override = default;

    // DDisk / PersistentBuffer ids registered for this group, all on the single
    // runtime node, distinguished by pdisk/slot. Pass them to the
    // DirectBlockGroup under test.
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

    // --- mock-compatible control surface ------------------------------------

    // Marks the (type, ddiskId) connection as pending: the stub stops answering
    // TEvConnect, so the connect stays in flight inside the transport actor.
    void SetPendingConnect(EConnectionType type, const TDDiskId& ddiskId);

    // Marks the next read to (type, ddiskId) as pending: the stub keeps the
    // read in flight so a disconnect can reject it.
    void SetPendingReadFromDDisk(EConnectionType type, const TDDiskId& ddiskId);

    // Same as SetPendingReadFromDDisk but for writes.
    void SetPendingWriteToDDisk(EConnectionType type, const TDDiskId& ddiskId);

    [[nodiscard]] TVector<NKikimr::NDDisk::TQueryCredentials>
    GetConnectCredentials(EConnectionType type, const TDDiskId& ddiskId) const;

    // Sends TEvInterconnect::TEvNodeDisconnected to the transport actor,
    // reproducing the real IC break that fires disconnect callbacks and rejects
    // in-flight session requests.
    void
    FireDisconnect(EConnectionType type, const TDDiskId& ddiskId, ui32 nodeId);

private:
    struct TKey
    {
        int ConnectionType = 0;
        ui32 PDiskId = 0;
        ui32 DDiskSlotId = 0;

        auto operator<=>(const TKey& other) const = default;
    };

    [[nodiscard]] static TKey MakeKey(
        EConnectionType type,
        const TDDiskId& ddiskId);

    [[nodiscard]] TDDiskStubStatePtr FindState(
        EConnectionType type,
        const TDDiskId& ddiskId) const;

    void RegisterStub(EConnectionType type, const TDDiskId& ddiskId);

    // Delegating constructor that receives the already registered transport
    // actor id so it can both initialize the TICStorageTransport base and
    // remember the id for the test control surface.
    TICStorageTransportTestAdapter(
        NActors::TTestActorRuntime* runtime,
        NActors::TActorId transportActorId);

    NActors::TTestActorRuntime* const Runtime;
    const ui32 NodeId;
    const NActors::TActorId EdgeActor;
    NActors::TActorId TransportActorId;

    TVector<TDDiskId> DDiskIds;
    TVector<TDDiskId> PBufferIds;

    TMap<TKey, TDDiskStubStatePtr> Stubs;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib
