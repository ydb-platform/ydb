#include "ic_storage_transport_test_adapter.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport_actor.h>

#include <ydb/core/base/blobstorage.h>

#include <ydb/library/actors/core/interconnect.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TICStorageTransportTestAdapter::TICStorageTransportTestAdapter(
    TTestActorRuntime* runtime)
    : Runtime(runtime)
    , NodeId(runtime->GetNodeId(0))
    , EdgeActor(runtime->AllocateEdgeActor(0))
{
    // Register the real transport actor inside the runtime and address it
    // directly from the transport built on the second constructor.
    TransportActorId = Runtime->Register(new TICStorageTransportActor(), 0);

    Inner = std::make_unique<TICStorageTransport>(
        Runtime->GetActorSystem(0),
        TransportActorId);

    // Single node: every DDisk/PersistentBuffer lives on the runtime node and
    // is distinguished only by pdisk/slot.
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
    auto actorId = Runtime->Register(new TDDiskStubActor(state), 0);

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
            break;
    }

    Runtime->RegisterService(serviceId, actorId, 0);
    Stubs[MakeKey(type, ddiskId)] = std::move(state);
}

////////////////////////////////////////////////////////////////////////////////

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

    Runtime->Send(
        new IEventHandle(
            TransportActorId,
            EdgeActor,
            new TEvInterconnect::TEvNodeDisconnected(nodeId)),
        0,
        true);
}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TICStorageTransportTestAdapter::TEvConnectResult>
TICStorageTransportTestAdapter::Connect(
    const THostConnection& connection,
    TDisconnectCB disconnectCB)
{
    return Inner->Connect(connection, std::move(disconnectCB));
}

NThreading::TFuture<
    TICStorageTransportTestAdapter::TEvReadPersistentBufferResult>
TICStorageTransportTestAdapter::ReadFromPBuffer(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const ui64 lsn,
    const NKikimr::NDDisk::TReadInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    return Inner
        ->ReadFromPBuffer(connection, selector, lsn, instruction, data, span);
}

NThreading::TFuture<TICStorageTransportTestAdapter::TEvReadResult>
TICStorageTransportTestAdapter::ReadFromDDisk(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const NKikimr::NDDisk::TReadInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    return Inner->ReadFromDDisk(connection, selector, instruction, data, span);
}

NThreading::TFuture<
    TICStorageTransportTestAdapter::TEvWritePersistentBufferResult>
TICStorageTransportTestAdapter::WriteToPBuffer(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const ui64 lsn,
    const NKikimr::NDDisk::TWriteInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    return Inner
        ->WriteToPBuffer(connection, selector, lsn, instruction, data, span);
}

void TICStorageTransportTestAdapter::WriteToManyPBuffers(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const ui64 lsn,
    const NKikimr::NDDisk::TWriteInstruction instruction,
    TVector<NKikimrBlobStorage::NDDisk::TDDiskId> persistentBufferIds,
    TDuration replyTimeout,
    const TGuardedSgList& data,
    std::shared_ptr<NWilson::TSpan> span,
    TWriteToManyPBuffersCallback callback)
{
    Inner->WriteToManyPBuffers(
        connection,
        selector,
        lsn,
        instruction,
        std::move(persistentBufferIds),
        replyTimeout,
        data,
        std::move(span),
        std::move(callback));
}

NThreading::TFuture<TICStorageTransportTestAdapter::TEvWriteResult>
TICStorageTransportTestAdapter::WriteToDDisk(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const NKikimr::NDDisk::TWriteInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    return Inner->WriteToDDisk(connection, selector, instruction, data, span);
}

NThreading::TFuture<TICStorageTransportTestAdapter::TEvSyncResult>
TICStorageTransportTestAdapter::SyncWithPBuffer(
    const THostConnection& pbufferConnection,
    const THostConnection& ddiskConnection,
    TVector<NKikimr::NDDisk::TBlockSelector> selectors,
    TVector<ui64> lsns,
    NWilson::TSpan* span)
{
    return Inner->SyncWithPBuffer(
        pbufferConnection,
        ddiskConnection,
        std::move(selectors),
        std::move(lsns),
        span);
}

NThreading::TFuture<
    TICStorageTransportTestAdapter::TEvErasePersistentBufferResult>
TICStorageTransportTestAdapter::BatchEraseFromPBuffer(
    const THostConnection& connection,
    TVector<ui64> lsns,
    NWilson::TSpan* span)
{
    return Inner->BatchEraseFromPBuffer(connection, std::move(lsns), span);
}

NThreading::TFuture<
    TICStorageTransportTestAdapter::TEvErasePersistentBufferResult>
TICStorageTransportTestAdapter::BarrierEraseFromPBuffer(
    const THostConnection& connection,
    ui64 lsn,
    NWilson::TSpan* span)
{
    return Inner->BarrierEraseFromPBuffer(connection, lsn, span);
}

NThreading::TFuture<
    TICStorageTransportTestAdapter::TEvListPersistentBufferResult>
TICStorageTransportTestAdapter::ListPBufferEntries(
    const THostConnection& connection)
{
    return Inner->ListPBufferEntries(connection);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib
