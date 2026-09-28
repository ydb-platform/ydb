#include "ic_storage_transport_test_adapter.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/disk_description.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport_actor.h>

#include <ydb/core/base/blobstorage.h>

#include <ydb/library/actors/core/interconnect.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TICStorageTransportTestAdapter::TICStorageTransportTestAdapter(
    TTestActorRuntime* runtime,
    bool enableChecksums)
    : TICStorageTransportTestAdapter(
          runtime,
          [&]
          {
              TBootstrap bootstrap;
              bootstrap.Registry = std::make_shared<TDirectSessionRegistry>();
              bootstrap.ActorId = runtime->Register(
                  std::make_unique<TICStorageTransportActor>(
                      TDiskDescription{
                          .DiskId = "disk-id",
                          .TabletId = 100,
                          .Generation = 1},
                      0,
                      enableChecksums,
                      bootstrap.Registry)
                      .release(),
                  0);
              return bootstrap;
          }(),
          enableChecksums)
{}

TICStorageTransportTestAdapter::TICStorageTransportTestAdapter(
    TTestActorRuntime* runtime,
    TBootstrap bootstrap,
    bool enableChecksums)
    : TICDirectStorageTransport(
          runtime->GetActorSystem(0),
          bootstrap.ActorId,
          bootstrap.Registry,
          enableChecksums)
    , Runtime(runtime)
    , NodeId(runtime->GetNodeId(0))
    , EdgeActor(runtime->AllocateEdgeActor(0))
    , TransportActorId(bootstrap.ActorId)
    , Registry(std::move(bootstrap.Registry))
{
    DDiskIds.reserve(DirectBlockGroupHostCount);
    PBufferIds.reserve(DirectBlockGroupHostCount);
    for (ui32 i = 0; i < DirectBlockGroupHostCount; ++i) {
        DDiskIds.emplace_back(NodeId, 1, i);
        PBufferIds.emplace_back(NodeId, 2, i);
    }

    for (const auto& ddiskId: DDiskIds) {
        RegisterStub(EConnectionType::DDisk, ddiskId);
    }
    for (const auto& pbufferId: PBufferIds) {
        RegisterStub(EConnectionType::PBuffer, pbufferId);
    }
}

////////////////////////////////////////////////////////////////////////////////

TICStorageTransportTestAdapter::TKey TICStorageTransportTestAdapter::MakeKey(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    return TKey{
        .ConnectionType = static_cast<int>(type),
        .PDiskId = ddiskId.PDiskId,
        .DDiskSlotId = ddiskId.DDiskSlotId};
}

TDDiskStubStatePtr TICStorageTransportTestAdapter::FindState(
    EConnectionType type,
    const TDDiskId& ddiskId) const
{
    const auto* state = Stubs.FindPtr(MakeKey(type, ddiskId));
    Y_ABORT_UNLESS(state, "no stub registered for the requested connection");
    return *state;
}

void TICStorageTransportTestAdapter::RegisterStub(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    auto state = MakeIntrusive<TDDiskStubState>();
    auto actorId = Runtime->Register(
        std::make_unique<TDDiskStubActor>(state).release(),
        0);

    TActorId serviceId;
    switch (type) {
        case EConnectionType::DDisk:
            serviceId = MakeBlobStorageDDiskId(
                ddiskId.NodeId,
                ddiskId.PDiskId,
                ddiskId.DDiskSlotId);
            break;
        case EConnectionType::PBuffer:
            serviceId = MakeBlobStoragePersistentBufferId(
                ddiskId.NodeId,
                ddiskId.PDiskId,
                ddiskId.DDiskSlotId);
            Runtime->RegisterService(
                MakeBlobStorageDDiskId(
                    ddiskId.NodeId,
                    ddiskId.PDiskId,
                    ddiskId.DDiskSlotId),
                actorId,
                0);
            break;
    }

    Runtime->RegisterService(serviceId, actorId, 0);
    Stubs[MakeKey(type, ddiskId)] = std::move(state);
}

////////////////////////////////////////////////////////////////////////////////

void TICStorageTransportTestAdapter::EnableFakeDirectSession()
{
    FakeDirectSession =
        std::make_shared<TFakeDirectSession>(Runtime->GetActorSystem(0));
    Registry->Set(
        NodeId,
        MakeSessionEntry(Runtime->GetActorSystem(0), FakeDirectSession));
}

void TICStorageTransportTestAdapter::ShutdownFakeDirectSession()
{
    if (FakeDirectSession) {
        FakeDirectSession->Shutdown();
    }
    Registry->Reset(NodeId);
    FakeDirectSession.reset();
}

void TICStorageTransportTestAdapter::SetDirectSession(
    std::shared_ptr<IDirectSession> session)
{
    FakeDirectSession.reset();
    if (session) {
        Registry->Set(
            NodeId,
            MakeSessionEntry(Runtime->GetActorSystem(0), std::move(session)));
    } else {
        Registry->Reset(NodeId);
    }
}

ui64 TICStorageTransportTestAdapter::GetFakeDirectSessionSentEventCount() const
{
    return FakeDirectSession ? FakeDirectSession->GetSentEventCount() : 0;
}

void TICStorageTransportTestAdapter::SetPendingConnect(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    auto state = FindState(type, ddiskId);
    auto guard = Guard(state->Lock);
    state->PendingConnect = true;
}

void TICStorageTransportTestAdapter::SetPendingReadFromDDisk(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    auto state = FindState(type, ddiskId);
    auto guard = Guard(state->Lock);
    state->PendingRead = true;
}

void TICStorageTransportTestAdapter::SetPendingWriteToDDisk(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    auto state = FindState(type, ddiskId);
    auto guard = Guard(state->Lock);
    state->PendingWrite = true;
}

void TICStorageTransportTestAdapter::SetPendingWriteToPBuffer(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    auto state = FindState(type, ddiskId);
    auto guard = Guard(state->Lock);
    state->PendingWritePBuffer = true;
}

void TICStorageTransportTestAdapter::SetPendingErase(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    auto state = FindState(type, ddiskId);
    auto guard = Guard(state->Lock);
    state->PendingErase = true;
}

void TICStorageTransportTestAdapter::SetPendingSync(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    auto state = FindState(type, ddiskId);
    auto guard = Guard(state->Lock);
    state->PendingSync = true;
}

void TICStorageTransportTestAdapter::SetSplitWriteToManyReplies(
    EConnectionType type,
    const TDDiskId& ddiskId,
    bool split)
{
    auto state = FindState(type, ddiskId);
    auto guard = Guard(state->Lock);
    state->SplitWriteToManyReplies = split;
}

void TICStorageTransportTestAdapter::ReleasePendingReads(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    auto state = FindState(type, ddiskId);
    ReleaseHeldRequests(
        state,
        Runtime->GetActorSystem(0),
        TDDiskStubState::EHeldKind::Read);
    ReleaseHeldRequests(
        state,
        Runtime->GetActorSystem(0),
        TDDiskStubState::EHeldKind::ReadPBuffer);
}

void TICStorageTransportTestAdapter::ReleasePendingWrites(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    ReleaseHeldRequests(
        FindState(type, ddiskId),
        Runtime->GetActorSystem(0),
        TDDiskStubState::EHeldKind::Write);
}

void TICStorageTransportTestAdapter::ReleasePendingWritePBuffers(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    auto state = FindState(type, ddiskId);
    ReleaseHeldRequests(
        state,
        Runtime->GetActorSystem(0),
        TDDiskStubState::EHeldKind::WritePBuffer);
    ReleaseHeldRequests(
        state,
        Runtime->GetActorSystem(0),
        TDDiskStubState::EHeldKind::WritePBuffers);
}

void TICStorageTransportTestAdapter::ReleasePendingWritePBuffersFirstHalf(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    ReleaseHeldWritePBuffersFirstHalf(
        FindState(type, ddiskId),
        Runtime->GetActorSystem(0));
}

void TICStorageTransportTestAdapter::ReleasePendingErases(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    ReleaseHeldRequests(
        FindState(type, ddiskId),
        Runtime->GetActorSystem(0),
        TDDiskStubState::EHeldKind::Erase);
}

void TICStorageTransportTestAdapter::ReleasePendingSyncs(
    EConnectionType type,
    const TDDiskId& ddiskId)
{
    ReleaseHeldRequests(
        FindState(type, ddiskId),
        Runtime->GetActorSystem(0),
        TDDiskStubState::EHeldKind::Sync);
}

TVector<NKikimr::NDDisk::TQueryCredentials>
TICStorageTransportTestAdapter::GetConnectCredentials(
    EConnectionType type,
    const TDDiskId& ddiskId) const
{
    auto state = FindState(type, ddiskId);
    auto guard = Guard(state->Lock);
    return state->ConnectCredentials;
}

void TICStorageTransportTestAdapter::FireDisconnect(
    EConnectionType type,
    const TDDiskId& ddiskId,
    ui32 nodeId)
{
    Y_UNUSED(type);
    Y_UNUSED(ddiskId);

    ShutdownFakeDirectSession();

    auto request = std::make_unique<IEventHandle>(
        TransportActorId,
        EdgeActor,
        std::make_unique<TEvInterconnect::TEvNodeDisconnected>(nodeId)
            .release());
    Runtime->Send(request.release(), 0, true);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib
