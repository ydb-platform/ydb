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

class TICStorageTransportTestAdapter: public TICStorageTransport
{
public:
    using EConnectionType = THostConnection::EConnectionType;
    using TDDiskId = NKikimr::NBsController::TDDiskId;

    explicit TICStorageTransportTestAdapter(
        NActors::TTestActorRuntime* runtime);
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

    void SetPendingConnect(EConnectionType type, const TDDiskId& ddiskId);
    void SetPendingReadFromDDisk(EConnectionType type, const TDDiskId& ddiskId);
    void SetPendingWriteToDDisk(EConnectionType type, const TDDiskId& ddiskId);

    [[nodiscard]] TVector<NKikimr::NDDisk::TQueryCredentials>
    GetConnectCredentials(EConnectionType type, const TDDiskId& ddiskId) const;

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
