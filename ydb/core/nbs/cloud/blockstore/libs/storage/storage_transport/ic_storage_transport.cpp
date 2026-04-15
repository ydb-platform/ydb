#include "ic_storage_transport.h"

#include "ic_storage_transport_actor.h"

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

using namespace NKikimr;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TICStorageTransport::TICStorageTransport(NActors::TActorSystem* actorSystem)
    : ActorSystem(actorSystem)
    , ICStorageTransportActorId(CreateTransportActor())
{}

TFuture<NKikimrBlobStorage::NDDisk::TEvConnectResult>
TICStorageTransport::Connect(const THostConnection& connection)
{
    auto request = std::make_unique<TEvTransportPrivate::TEvConnect>(
        connection.GetServiceId(),
        connection.Credentials);
    auto future = request->Promise.GetFuture();

    ActorSystem->Send(ICStorageTransportActorId, request.release());

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult>
TICStorageTransport::WriteToPBuffer(
    const THostConnection& connection,
    const NDDisk::TBlockSelector& selector,
    const ui64 lsn,
    const NDDisk::TWriteInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan& span)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::PBuffer);

    auto request = std::make_unique<TEvTransportPrivate::TEvWriteToPBuffer>(
        connection.GetServiceId(),
        connection.Credentials,
        selector,
        lsn,
        instruction,
        data,
        span.GetTraceId());

    auto future = request->Promise.GetFuture();

    span.Event("Before_ActorSystem_Send");
    ActorSystem->Send(ICStorageTransportActorId, request.release());
    span.Event("After_ActorSystem_Send");

    return future;
}

TFuture<TEvTransportPrivate::TProtoEvWriteToManyPersistentBuffersResult>
TICStorageTransport::WriteToManyPBuffers(
    const THostConnection& connection,
    const NDDisk::TBlockSelector& selector,
    const ui64 lsn,
    const NDDisk::TWriteInstruction instruction,
    TVector<NKikimrBlobStorage::NDDisk::TDDiskId> persistentBufferIds,
    TDuration replyTimeout,
    const TGuardedSgList& data,
    NWilson::TSpan& span)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::PBuffer);

    auto request =
        std::make_unique<TEvTransportPrivate::TEvWriteToManyPBuffers>(
            connection.GetServiceId(),
            connection.Credentials,
            selector,
            lsn,
            instruction,
            std::move(persistentBufferIds),
            replyTimeout,
            data,
            span.GetTraceId());

    auto future = request->Promise.GetFuture();

    span.Event("Before_ActorSystem_Send");
    ActorSystem->Send(ICStorageTransportActorId, request.release());
    span.Event("After_ActorSystem_Send");

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvWriteResult>
TICStorageTransport::WriteToDDisk(
    const THostConnection& connection,
    const NKikimr::NDDisk::TBlockSelector& selector,
    const NKikimr::NDDisk::TWriteInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan& span)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::DDisk);

    auto request = std::make_unique<TEvTransportPrivate::TEvWriteToDDisk>(
        connection.GetServiceId(),
        connection.Credentials,
        selector,
        instruction,
        data,
        span.GetTraceId());

    auto future = request->Promise.GetFuture();

    span.Event("Before_ActorSystem_Send");
    ActorSystem->Send(ICStorageTransportActorId, request.release());
    span.Event("After_ActorSystem_Send");

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult>
TICStorageTransport::EraseFromPBuffer(
    const THostConnection& connection,
    TVector<NKikimr::NDDisk::TBlockSelector> selectors,
    TVector<ui64> lsns,
    NWilson::TSpan& span)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::PBuffer);

    auto request = std::make_unique<TEvTransportPrivate::TEvEraseFromPBuffer>(
        connection.GetServiceId(),
        connection.Credentials,
        std::move(selectors),
        std::move(lsns),
        span.GetTraceId());

    auto future = request->Promise.GetFuture();

    span.Event("Before_ActorSystem_Send");
    ActorSystem->Send(ICStorageTransportActorId, request.release());
    span.Event("After_ActorSystem_Send");

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult>
TICStorageTransport::ReadFromPBuffer(
    const THostConnection& connection,
    const NDDisk::TBlockSelector& selector,
    const ui64 lsn,
    const NDDisk::TReadInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan& span)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::PBuffer);

    auto request = std::make_unique<TEvTransportPrivate::TEvReadFromPBuffer>(
        connection.GetServiceId(),
        connection.Credentials,
        selector,
        lsn,
        instruction,
        data,
        span.GetTraceId());

    auto future = request->Promise.GetFuture();

    span.Event("Before_ActorSystem_Send");
    ActorSystem->Send(ICStorageTransportActorId, request.release());
    span.Event("After_ActorSystem_Send");

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvReadResult>
TICStorageTransport::ReadFromDDisk(
    const THostConnection& connection,
    const NDDisk::TBlockSelector& selector,
    const NDDisk::TReadInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan& span)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::DDisk);

    auto request = std::make_unique<TEvTransportPrivate::TEvReadFromDDisk>(
        connection.GetServiceId(),
        connection.Credentials,
        selector,
        instruction,
        data,
        span.GetTraceId());

    auto future = request->Promise.GetFuture();

    span.Event("Before_ActorSystem_Send");
    ActorSystem->Send(ICStorageTransportActorId, request.release());
    span.Event("After_ActorSystem_Send");

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult>
TICStorageTransport::SyncWithPBuffer(
    const THostConnection& pbufferConnection,
    const THostConnection& ddiskConnection,
    TVector<NKikimr::NDDisk::TBlockSelector> selectors,
    TVector<ui64> lsns,
    NWilson::TSpan& span)
{
    Y_ABORT_UNLESS(
        pbufferConnection.ConnectionType == EConnectionType::PBuffer);
    Y_ABORT_UNLESS(ddiskConnection.ConnectionType == EConnectionType::DDisk);

    auto request = std::make_unique<TEvTransportPrivate::TEvSyncWithPBuffer>(
        ddiskConnection.GetServiceId(),
        ddiskConnection.Credentials,
        std::move(selectors),
        std::move(lsns),
        pbufferConnection.DDiskId,
        pbufferConnection.Credentials,
        span.GetTraceId());

    auto future = request->Promise.GetFuture();

    span.Event("Before_ActorSystem_Send");
    ActorSystem->Send(ICStorageTransportActorId, request.release());
    span.Event("After_ActorSystem_Send");

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult>
TICStorageTransport::ListPBufferEntries(const THostConnection& connection)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::PBuffer);

    auto request = std::make_unique<TEvTransportPrivate::TEvListPBufferEntries>(
        connection.GetServiceId(),
        connection.Credentials);

    auto future = request->Promise.GetFuture();

    ActorSystem->Send(ICStorageTransportActorId, request.release());

    return future;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
