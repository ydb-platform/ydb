#include "ic_storage_transport.h"

#include <ydb/library/actors/util/rope.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

using namespace NActors;
using namespace NKikimr::NDDisk;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId CreateTransportActor()
{
    auto actor = std::make_unique<TICStorageTransportActor>();

    return NActors::TActivationContext::Register(
        actor.release(),
        NActors::TActorId(),
        NActors::TMailboxType::ReadAsFilled,
        NKikimr::AppData()->SystemPoolId);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TICStorageTransport::TICStorageTransport(NActors::TActorSystem* actorSystem)
    : ActorSystem(actorSystem)
    , ICStorageTransportActorId(CreateTransportActor())
{}

TFuture<NKikimrBlobStorage::NDDisk::TEvConnectResult>
TICStorageTransport::Connect(
    const NActors::TActorId serviceId,
    const TQueryCredentials credentials)
{
    auto promise = NewPromise<TEvConnectResult>();
    auto future = promise.GetFuture();

    ActorSystem->Send(
        ICStorageTransportActorId,
        new TEvICStorageTransportPrivate::TEvConnect(
            serviceId,
            credentials,
            std::move(promise)));

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult>
TICStorageTransport::WritePersistentBuffer(
    const NActors::TActorId serviceId,
    const TQueryCredentials credentials,
    const TBlockSelector selector,
    const ui64 lsn,
    const TWriteInstruction instruction,
    TGuardedSgList data,
    NWilson::TSpan& span)
{
    auto promise = NewPromise<TEvWritePersistentBufferResult>();
    auto future = promise.GetFuture();

    span.Event("Before_ActorSystem_Send");
    ActorSystem->Send(
        ICStorageTransportActorId,
        new TEvICStorageTransportPrivate::TEvWritePersistentBuffer(
            serviceId,
            credentials,
            selector,
            lsn,
            instruction,
            std::move(data),
            span.GetTraceId(),
            std::move(promise)));
    span.Event("After_ActorSystem_Send");

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult>
TICStorageTransport::ErasePersistentBuffer(
    const NActors::TActorId serviceId,
    const TQueryCredentials credentials,
    TVector<NKikimr::NDDisk::TBlockSelector> selectors,
    TVector<ui64> lsns,
    NWilson::TSpan& span)
{
    auto promise = NewPromise<TEvErasePersistentBufferResult>();
    auto future = promise.GetFuture();

    span.Event("Before_ActorSystem_Send");
    ActorSystem->Send(
        ICStorageTransportActorId,
        new TEvICStorageTransportPrivate::TEvErasePersistentBuffer(
            serviceId,
            credentials,
            std::move(selectors),
            std::move(lsns),
            span.GetTraceId(),
            std::move(promise)));
    span.Event("After_ActorSystem_Send");

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult>
TICStorageTransport::ReadFromPBuffer(
    const NActors::TActorId serviceId,
    const TQueryCredentials credentials,
    const TBlockSelector selector,
    const ui64 lsn,
    const TReadInstruction instruction,
    TGuardedSgList data,
    NWilson::TSpan& span)
{
    auto promise =
        NewPromise<NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult>();
    auto future = promise.GetFuture();

    span.Event("Before_ActorSystem_Send");
    ActorSystem->Send(
        ICStorageTransportActorId,
        new TEvICStorageTransportPrivate::TEvReadPersistentBuffer(
            serviceId,
            credentials,
            selector,
            lsn,
            instruction,
            std::move(data),
            span.GetTraceId(),
            std::move(promise)));
    span.Event("After_ActorSystem_Send");

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvReadResult>
TICStorageTransport::ReadFromDDisk(
    const NActors::TActorId serviceId,
    const TQueryCredentials credentials,
    const TBlockSelector selector,
    const TReadInstruction instruction,
    TGuardedSgList data,
    NWilson::TSpan& span)
{
    auto promise = NewPromise<TEvReadResult>();
    auto future = promise.GetFuture();

    span.Event("Before_ActorSystem_Send");
    ActorSystem->Send(
        ICStorageTransportActorId,
        new TEvICStorageTransportPrivate::TEvRead(
            serviceId,
            credentials,
            selector,
            instruction,
            std::move(data),
            span.GetTraceId(),
            std::move(promise)));
    span.Event("After_ActorSystem_Send");

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult>
TICStorageTransport::FlushFromPBuffer(
    const NActors::TActorId serviceId,
    const NKikimr::NDDisk::TQueryCredentials credentials,
    TVector<NKikimr::NDDisk::TBlockSelector> selectors,
    TVector<ui64> lsns,
    const std::tuple<ui32, ui32, ui32> ddiskId,
    const ui64 ddiskInstanceGuid,
    NWilson::TSpan& span)
{
    auto promise = NewPromise<TEvSyncWithPersistentBufferResult>();
    auto future = promise.GetFuture();

    span.Event("Before_ActorSystem_Send");
    ActorSystem->Send(
        ICStorageTransportActorId,
        new TEvICStorageTransportPrivate::TEvSyncWithPersistentBuffer(
            serviceId,
            credentials,
            std::move(selectors),
            std::move(lsns),
            ddiskId,
            ddiskInstanceGuid,
            span.GetTraceId(),
            std::move(promise)));
    span.Event("After_ActorSystem_Send");

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult>
TICStorageTransport::ListPersistentBuffer(
    const NActors::TActorId serviceId,
    const NKikimr::NDDisk::TQueryCredentials credentials)
{
    auto promise = NewPromise<TEvListPersistentBufferResult>();
    auto future = promise.GetFuture();

    ActorSystem->Send(
        ICStorageTransportActorId,
        new TEvICStorageTransportPrivate::TEvListPersistentBuffer(
            serviceId,
            credentials,
            std::move(promise)));

    return future;
}

////////////////////////////////////////////////////////////////////////////////

void TICStorageTransportActor::Bootstrap(const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Become(&TThis::StateWork);
}

void TICStorageTransportActor::HandleConnect(
    const TEvICStorageTransportPrivate::TEvConnect::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;
    auto [it, inserted] =
        ConnectEventsByRequestId.emplace(requestId, std::move(*msg));
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvConnect with requestId# %lu",
        requestId);

    auto request = std::make_unique<TEvConnect>(msg->Credentials);

    ctx.Send(
        it->second.ServiceId,
        request.release(),
        0,          // flags
        requestId   // cookie
    );
}

void TICStorageTransportActor::HandleConnectResult(
    const TEvConnectResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvConnectResult with requestId# %lu",
        requestId);

    if (auto* request = ConnectEventsByRequestId.FindPtr(requestId)) {
        request->Promise.SetValue(std::move(ev->Get()->Record));
        ConnectEventsByRequestId.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "ConnectEvent with requestId# %lu not found",
            requestId);
    }
}

void TICStorageTransportActor::HandleWritePersistentBuffer(
    const TEvICStorageTransportPrivate::TEvWritePersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;
    auto [it, inserted] = WritePersistentBufferEventsByRequestId.emplace(
        requestId,
        std::move(*msg));
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvWritePersistentBuffer with requestId# %lu",
        requestId);

    auto request = std::make_unique<TEvWritePersistentBuffer>(
        it->second.Credentials,
        it->second.Selector,
        it->second.Lsn,
        it->second.Instruction);

    if (auto guard = it->second.Data.Acquire()) {
        const auto& sglist = guard.Get();
        TRope rope = TRope::Uninitialized(SgListGetSize(sglist));
        SgListCopy(sglist, CreateSgList(rope));
        request->AddPayload(std::move(rope));
    }

    ctx.Send(MakeHolder<IEventHandle>(
        it->second.ServiceId,
        ctx.SelfID,
        request.release(),
        0,           // flags
        requestId,   // cookie
        nullptr,
        std::move(it->second.TraceId)));
}

void TICStorageTransportActor::HandleWritePersistentBufferResult(
    const TEvWritePersistentBufferResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvWritePersistentBufferResult with requestId# %lu",
        requestId);

    if (auto* requestHandler =
            WritePersistentBufferEventsByRequestId.FindPtr(requestId))
    {
        requestHandler->Promise.SetValue(std::move(ev->Get()->Record));
        WritePersistentBufferEventsByRequestId.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "WritePersistentBufferEvent with requestId# %lu not found",
            requestId);
    }
}

void TICStorageTransportActor::HandleErasePersistentBuffer(
    const TEvICStorageTransportPrivate::TEvErasePersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;
    auto [it, inserted] = ErasePersistentBufferEventsByRequestId.emplace(
        requestId,
        std::move(*msg));
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvErasePersistentBuffer with requestId# %lu",
        requestId);

    auto request =
        std::make_unique<TEvBatchErasePersistentBuffer>(it->second.Credentials);
    for (size_t i = 0; i < it->second.Selectors.size(); ++i) {
        request->AddErase(
            it->second.Selectors[i],
            it->second.Lsns[i],
            it->second.Credentials.Generation);
    }

    ctx.Send(MakeHolder<IEventHandle>(
        it->second.ServiceId,
        ctx.SelfID,
        request.release(),
        0,           // flags
        requestId,   // cookie
        nullptr,
        std::move(it->second.TraceId)));
}

void TICStorageTransportActor::HandleErasePersistentBufferResult(
    const TEvErasePersistentBufferResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvErasePersistentBufferResult with requestId# %lu",
        requestId);

    if (auto* requestHandler =
            ErasePersistentBufferEventsByRequestId.FindPtr(requestId))
    {
        requestHandler->Promise.SetValue(std::move(ev->Get()->Record));
        ErasePersistentBufferEventsByRequestId.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "ErasePersistentBufferEvent with requestId# %lu not found",
            requestId);
    }
}

void TICStorageTransportActor::HandleReadPersistentBuffer(
    const TEvICStorageTransportPrivate::TEvReadPersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;

    auto [it, inserted] = ReadPersistentBufferEventsByRequestId.emplace(
        requestId,
        std::move(*msg));
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvReadPersistentBuffer with requestId# %lu",
        requestId);

    auto request = std::make_unique<TEvReadPersistentBuffer>(
        it->second.Credentials,
        it->second.Selector,
        it->second.Lsn,
        it->second.Credentials.Generation,
        it->second.Instruction);

    ctx.Send(MakeHolder<IEventHandle>(
        it->second.ServiceId,
        ctx.SelfID,
        request.release(),
        0,           // flags
        requestId,   // cookie
        nullptr,
        std::move(it->second.TraceId)));
}

void TICStorageTransportActor::HandleReadPersistentBufferResult(
    const TEvReadPersistentBufferResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvReadPersistentBufferResult with requestId# %lu",
        requestId);

    // That means that request is already completed
    if (auto* requestHandler =
            ReadPersistentBufferEventsByRequestId.FindPtr(requestId))
    {
        auto& data = requestHandler->Data;
        if (auto guard = data.Acquire()) {
            const auto& sglist = guard.Get();
            SgListCopy(CreateSgList(ev->Get()->GetPayload()), sglist);
        }

        requestHandler->Promise.SetValue(std::move(ev->Get()->Record));
        ReadPersistentBufferEventsByRequestId.erase(requestId);
    } else {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "ReadPersistentBufferEvent with requestId# %lu not found",
            requestId);
    }
}

void TICStorageTransportActor::HandleRead(
    const TEvICStorageTransportPrivate::TEvRead::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;

    auto [it, inserted] =
        ReadEventsByRequestId.emplace(requestId, std::move(*msg));
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvRead with requestId# %lu",
        requestId);

    auto request = std::make_unique<TEvRead>(
        it->second.Credentials,
        it->second.Selector,
        it->second.Instruction);

    ctx.Send(MakeHolder<IEventHandle>(
        it->second.ServiceId,
        ctx.SelfID,
        request.release(),
        0,           // flags
        requestId,   // cookie
        nullptr,
        std::move(it->second.TraceId)));
}

void TICStorageTransportActor::HandleReadResult(
    const TEvReadResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvReadResult with requestId# %lu",
        requestId);

    if (auto* requestHandler = ReadEventsByRequestId.FindPtr(requestId)) {
        if (auto guard = requestHandler->Data.Acquire()) {
            const auto& sglist = guard.Get();
            SgListCopy(CreateSgList(ev->Get()->GetPayload()), sglist);
        }

        requestHandler->Promise.SetValue(std::move(ev->Get()->Record));
        ReadEventsByRequestId.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "ReadEvent with requestId# %lu not found",
            requestId);
    }
}

void TICStorageTransportActor::HandleSyncWithPersistentBuffer(
    const TEvICStorageTransportPrivate::TEvSyncWithPersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;

    auto [it, inserted] =
        SyncEventsByRequestId.emplace(requestId, std::move(*msg));
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvSyncWithPersistentBuffer with requestId# %lu",
        requestId);

    auto request = std::make_unique<TEvSyncWithPersistentBuffer>(
        it->second.Credentials,
        it->second.DDiskId,
        it->second.DDiskInstanceGuid);

    for (size_t i = 0; i < it->second.Selectors.size(); ++i) {
        request->AddSegment(
            it->second.Selectors[i],
            it->second.Lsns[i],
            it->second.Credentials.Generation);
    }

    ctx.Send(MakeHolder<IEventHandle>(
        it->second.ServiceId,
        ctx.SelfID,
        request.release(),
        0,   // flags
        requestId,
        nullptr,
        std::move(it->second.TraceId)));
}

void TICStorageTransportActor::HandleSyncWithPersistentBufferResult(
    const NKikimr::NDDisk::TEvSyncWithPersistentBufferResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvSyncWithPersistentBufferResult with requestId# %lu",
        requestId);

    if (auto* requestHandler = SyncEventsByRequestId.FindPtr(requestId)) {
        requestHandler->Promise.SetValue(std::move(ev->Get()->Record));
        SyncEventsByRequestId.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "SyncEvent with requestId# %lu not found",
            requestId);
    }
}

void TICStorageTransportActor::HandleListPersistentBuffer(
    const TEvICStorageTransportPrivate::TEvListPersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;

    ListPersistentBufferEventsByRequestId.emplace(requestId, std::move(*msg));

    auto request = std::make_unique<NKikimr::NDDisk::TEvListPersistentBuffer>(
        msg->Credentials);

    ctx.Send(
        msg->ServiceId,
        request.release(),
        0,   // flags
        requestId);
}

void TICStorageTransportActor::HandleListPersistentBufferResult(
    const NKikimr::NDDisk::TEvListPersistentBufferResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received HandleListPersistentBufferResult with requestId# %lu",
        requestId);

    if (auto* requestHandler =
            ListPersistentBufferEventsByRequestId.FindPtr(requestId))
    {
        requestHandler->Promise.SetValue(std::move(ev->Get()->Record));
        ListPersistentBufferEventsByRequestId.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "ListPersistentBuffer with requestId# %lu not found",
            requestId);
    }
}

///////////////////////////////////////////////////////////////////////////////

STFUNC(TICStorageTransportActor::StateWork)
{
    LOG_DEBUG(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "Processing event: %s from sender: %lu",
        ev->GetTypeName().data(),
        ev->Sender.LocalId());

    switch (ev->GetTypeRewrite()) {
        cFunc(TEvents::TEvPoison::EventType, PassAway);

        HFunc(TEvICStorageTransportPrivate::TEvConnect, HandleConnect);
        HFunc(TEvConnectResult, HandleConnectResult);

        HFunc(
            TEvICStorageTransportPrivate::TEvWritePersistentBuffer,
            HandleWritePersistentBuffer);
        HFunc(
            TEvWritePersistentBufferResult,
            HandleWritePersistentBufferResult);

        HFunc(
            TEvICStorageTransportPrivate::TEvErasePersistentBuffer,
            HandleErasePersistentBuffer);
        HFunc(
            TEvErasePersistentBufferResult,
            HandleErasePersistentBufferResult);

        HFunc(
            TEvICStorageTransportPrivate::TEvReadPersistentBuffer,
            HandleReadPersistentBuffer);
        HFunc(TEvReadPersistentBufferResult, HandleReadPersistentBufferResult);

        HFunc(TEvICStorageTransportPrivate::TEvRead, HandleRead);
        HFunc(TEvReadResult, HandleReadResult);

        HFunc(
            TEvICStorageTransportPrivate::TEvSyncWithPersistentBuffer,
            HandleSyncWithPersistentBuffer);
        HFunc(
            NKikimr::NDDisk::TEvSyncWithPersistentBufferResult,
            HandleSyncWithPersistentBufferResult);

        HFunc(
            TEvICStorageTransportPrivate::TEvListPersistentBuffer,
            HandleListPersistentBuffer);
        HFunc(
            NKikimr::NDDisk::TEvListPersistentBufferResult,
            HandleListPersistentBufferResult);

        default:
            LOG_DEBUG_S(
                TActivationContext::AsActorContext(),
                NKikimrServices::NBS_PARTITION,
                "Unhandled event type: " << ev->GetTypeRewrite()
                                         << " event: " << ev->ToString());
            break;
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
