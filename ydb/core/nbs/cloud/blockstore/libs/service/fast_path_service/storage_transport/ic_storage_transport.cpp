#include "ic_storage_transport.h"

namespace NYdb::NBS::NBlockStore {

using namespace NActors;
using namespace NKikimr::NDDisk;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TICStorageTransport::TICStorageTransport()
{
    auto actor = std::make_unique<TICStorageTransportActor>();

    ICStorageTransportActorId = NActors::TActivationContext::Register(
        actor.release(),
        NActors::TActivationContext::AsActorContext().SelfID,
        NActors::TMailboxType::ReadAsFilled,
        NKikimr::AppData()->SystemPoolId);
}

TFuture<NKikimrBlobStorage::NDDisk::TEvConnectResult> TICStorageTransport::Connect(
    const NActors::TActorId serviceId,
    const TQueryCredentials credentials,
    const ui64 requestId)
{
    auto promise = NewPromise<NKikimrBlobStorage::NDDisk::TEvConnectResult>();
    auto future = promise.GetFuture();

    NActors::TActivationContext::AsActorContext().Send(
        ICStorageTransportActorId,
        new TEvICStorageTransportPrivate::TEvConnect(
            serviceId,
            credentials,
            requestId,
            std::move(promise)));

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult> TICStorageTransport::WritePersistentBuffer(
    const NActors::TActorId serviceId,
    const TQueryCredentials credentials,
    const TBlockSelector selector,
    const ui64 lsn,
    const TWriteInstruction instruction,
    TGuardedSgList data,
    NWilson::TTraceId traceId,
    const ui64 requestId)
{
    auto promise = NewPromise<NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult>();
    auto future = promise.GetFuture();

    NActors::TActivationContext::AsActorContext().Send(
        ICStorageTransportActorId,
        new TEvICStorageTransportPrivate::TEvWritePersistentBuffer(
            serviceId,
            credentials,
            selector,
            lsn,
            instruction,
            std::move(data),
            std::move(traceId),
            requestId,
            std::move(promise)));

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult> TICStorageTransport::ErasePersistentBuffer(
    const NActors::TActorId serviceId,
    const TQueryCredentials credentials,
    const TBlockSelector selector,
    const ui64 lsn,
    NWilson::TTraceId traceId,
    const ui64 requestId)
{
    auto promise = NewPromise<NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult>();
    auto future = promise.GetFuture();

    NActors::TActivationContext::AsActorContext().Send(
        ICStorageTransportActorId,
        new TEvICStorageTransportPrivate::TEvErasePersistentBuffer(
            serviceId,
            credentials,
            selector,
            lsn,
            std::move(traceId),
            requestId,
            std::move(promise)));

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult> TICStorageTransport::ReadPersistentBuffer(
    const NActors::TActorId serviceId,
    const TQueryCredentials credentials,
    const TBlockSelector selector,
    const ui64 lsn,
    const TReadInstruction instruction,
    TGuardedSgList data,
    NWilson::TTraceId traceId,
    const ui64 requestId)
{
    auto promise = NewPromise<NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult>();
    auto future = promise.GetFuture();

    NActors::TActivationContext::AsActorContext().Send(
        ICStorageTransportActorId,
        new TEvICStorageTransportPrivate::TEvReadPersistentBuffer(
            serviceId,
            credentials,
            selector,
            lsn,
            instruction,
            std::move(data),
            std::move(traceId),
            requestId,
            std::move(promise)));

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvReadResult> TICStorageTransport::Read(
    const NActors::TActorId serviceId,
    const TQueryCredentials credentials,
    const TBlockSelector selector,
    const TReadInstruction instruction,
    TGuardedSgList data,
    NWilson::TTraceId traceId,
    const ui64 requestId)
{
    auto promise = NewPromise<NKikimrBlobStorage::NDDisk::TEvReadResult>();
    auto future = promise.GetFuture();

    NActors::TActivationContext::AsActorContext().Send(
        ICStorageTransportActorId,
        new TEvICStorageTransportPrivate::TEvRead(
            serviceId,
            credentials,
            selector,
            instruction,
            std::move(data),
            std::move(traceId),
            requestId,
            std::move(promise)));

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult> TICStorageTransport::SyncWithPersistentBuffer(
    const NActors::TActorId serviceId,
    const NKikimr::NDDisk::TQueryCredentials credentials,
    const NKikimr::NDDisk::TBlockSelector selector,
    const ui64 lsn,
    const std::tuple<ui32, ui32, ui32> ddiskId,
    const ui64 ddiskInstanceGuid,
    NWilson::TTraceId traceId,
    const ui64 requestId)
{
    auto promise = NewPromise<NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult>();
    auto future = promise.GetFuture();

    NActors::TActivationContext::AsActorContext().Send(
        ICStorageTransportActorId,
        new TEvICStorageTransportPrivate::TEvSyncWithPersistentBuffer(
            serviceId,
            credentials,
            selector,
            lsn,
            ddiskId,
            ddiskInstanceGuid,
            std::move(traceId),
            requestId,
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

    auto [it, inserted] = ConnectEventsByRequestId.emplace(msg->RequestId, std::move(*msg));
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvConnect with requestId# %lu",
        it->second.RequestId);

    auto request = std::make_unique<TEvConnect>(msg->Credentials);

    ctx.Send(
        it->second.ServiceId,
        request.release(),
        0, // flags
        it->second.RequestId);
}

void TICStorageTransportActor::HandleConnectResult(
    const TEvConnectResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvConnectResult with requestId# %lu",
        requestId
    );

    // That means that request is already completed
    if (!ConnectEventsByRequestId.contains(requestId)) {
        LOG_DEBUG(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "ConnectEvent with requestId# %lu not found",
            requestId);
        return;
    }

    auto& promise = ConnectEventsByRequestId.at(requestId).Promise;
    promise.SetValue(std::move(ev->Get()->Record));
    ConnectEventsByRequestId.erase(requestId);
}

void TICStorageTransportActor::HandleWritePersistentBuffer(
    const TEvICStorageTransportPrivate::TEvWritePersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto [it, inserted] = WritePersistentBufferEventsByRequestId.emplace(msg->RequestId, std::move(*msg));
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvWritePersistentBuffer with requestId# %lu",
        it->second.RequestId);

    auto request = std::make_unique<TEvWritePersistentBuffer>(
        it->second.Credentials,
        it->second.Selector,
        it->second.Lsn,
        it->second.Instruction);

    if (auto guard = it->second.Data.Acquire()) {
        const auto& sglist = guard.Get();
        request->AddPayload(TRope(TString(sglist[0].Data(), sglist[0].Size())));
    } else {
        Y_ABORT_UNLESS(false);
    }

    ctx.Send(MakeHolder<IEventHandle>(
        it->second.ServiceId,
        ctx.SelfID,
        request.release(),
        0, // flags
        it->second.RequestId,
        nullptr,
        std::move(it->second.TraceId)));
}

void TICStorageTransportActor::HandleWritePersistentBufferResult(
    const TEvWritePersistentBufferResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvWritePersistentBufferResult with requestId# %lu",
        requestId);

    // That means that request is already completed
    if (!WritePersistentBufferEventsByRequestId.contains(requestId)) {
        LOG_DEBUG(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "WritePersistentBufferEvent with requestId# %lu not found",
            requestId);

        return;
    }

    auto& promise = WritePersistentBufferEventsByRequestId.at(requestId).Promise;

    promise.SetValue(std::move(ev->Get()->Record));
    WritePersistentBufferEventsByRequestId.erase(requestId);
}

void TICStorageTransportActor::HandleErasePersistentBuffer(
    const TEvICStorageTransportPrivate::TEvErasePersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto [it, inserted] = ErasePersistentBufferEventsByRequestId.emplace(msg->RequestId, std::move(*msg));
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvErasePersistentBuffer with requestId# %lu",
        it->second.RequestId);

    auto request = std::make_unique<TEvErasePersistentBuffer>(
        it->second.Credentials,
        it->second.Selector,
        it->second.Lsn);

    ctx.Send(MakeHolder<IEventHandle>(
        it->second.ServiceId,
        ctx.SelfID,
        request.release(),
        0, // flags
        it->second.RequestId,
        nullptr,
        std::move(it->second.TraceId)));
}

void TICStorageTransportActor::HandleErasePersistentBufferResult(
    const TEvErasePersistentBufferResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvErasePersistentBufferResult with requestId# %lu",
        requestId);

    // That means that request is already completed
    if (!ErasePersistentBufferEventsByRequestId.contains(requestId)) {
        LOG_DEBUG(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "ErasePersistentBufferEvent with requestId# %lu not found",
            requestId);
        return;
    }

    auto& promise = ErasePersistentBufferEventsByRequestId.at(requestId).Promise;

    promise.SetValue(std::move(ev->Get()->Record));
    ErasePersistentBufferEventsByRequestId.erase(requestId);
}

void TICStorageTransportActor::HandleReadPersistentBuffer(
    const TEvICStorageTransportPrivate::TEvReadPersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto [it, inserted] = ReadPersistentBufferEventsByRequestId.emplace(msg->RequestId, std::move(*msg));
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvReadPersistentBuffer with requestId# %lu",
        it->second.RequestId);

    auto request = std::make_unique<TEvReadPersistentBuffer>(
        it->second.Credentials,
        it->second.Selector,
        it->second.Lsn,
        it->second.Instruction);

    ctx.Send(MakeHolder<IEventHandle>(
        it->second.ServiceId,
        ctx.SelfID,
        request.release(),
        0, // flags
        it->second.RequestId,
        nullptr,
        std::move(it->second.TraceId)));
}

void TICStorageTransportActor::HandleReadPersistentBufferResult(
    const TEvReadPersistentBufferResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvReadPersistentBufferResult with requestId# %lu",
        requestId);

    // That means that request is already completed
    if (!ReadPersistentBufferEventsByRequestId.contains(requestId)) {
        LOG_DEBUG(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "ReadPersistentBufferEvent with requestId# %lu not found",
            requestId);
        return;
    }

    auto& promise = ReadPersistentBufferEventsByRequestId.at(requestId).Promise;
    auto& data = ReadPersistentBufferEventsByRequestId.at(requestId).Data;
    if (auto guard = data.Acquire()) {
        const auto& sglist = guard.Get();
        const auto& block = sglist[0];
        // Bad perf method
        const TString payload = ev->Get()->GetPayload(0).ConvertToString();
        memcpy(const_cast<char*>(block.Data()), payload.data(), block.Size());
    } else {
        Y_ABORT_UNLESS(false);
    }

    promise.SetValue(std::move(ev->Get()->Record));
    ReadPersistentBufferEventsByRequestId.erase(requestId);
}

void TICStorageTransportActor::HandleRead(
    const TEvICStorageTransportPrivate::TEvRead::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto [it, inserted] = ReadEventsByRequestId.emplace(msg->RequestId, std::move(*msg));
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvRead with requestId# %lu",
        it->second.RequestId);

    auto request = std::make_unique<TEvRead>(
        it->second.Credentials,
        it->second.Selector,
        it->second.Instruction);

    ctx.Send(MakeHolder<IEventHandle>(
        it->second.ServiceId,
        ctx.SelfID,
        request.release(),
        0, // flags
        it->second.RequestId,
        nullptr,
        std::move(it->second.TraceId)));
}

void TICStorageTransportActor::HandleReadResult(
    const TEvReadResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvReadResult with requestId# %lu",
        requestId);

    // That means that request is already completed
    if (!ReadEventsByRequestId.contains(requestId)) {
        LOG_DEBUG(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "ReadEvent with requestId# %lu not found",
            requestId);
        return;
    }

    auto& promise = ReadEventsByRequestId.at(requestId).Promise;
    auto& data = ReadEventsByRequestId.at(requestId).Data;
    if (auto guard = data.Acquire()) {
        const auto& sglist = guard.Get();
        const auto& block = sglist[0];
        // Bad perf method
        const TString payload = ev->Get()->GetPayload(0).ConvertToString();
        memcpy(const_cast<char*>(block.Data()), payload.data(), block.Size());
    } else {
        Y_ABORT_UNLESS(false);
    }

    promise.SetValue(std::move(ev->Get()->Record));
    ReadEventsByRequestId.erase(requestId);
}

void TICStorageTransportActor::HandleSyncWithPersistentBuffer(
    const TEvICStorageTransportPrivate::TEvSyncWithPersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto [it, inserted] = SyncEventsByRequestId.emplace(msg->RequestId, std::move(*msg));
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvSyncWithPersistentBuffer with requestId# %lu",
        it->second.RequestId);

    auto request = std::make_unique<TEvSyncWithPersistentBuffer>(
        it->second.Credentials,
        it->second.DDiskId,
        it->second.DDiskInstanceGuid);

    request->AddSegment(it->second.Selector, it->second.Lsn);

    ctx.Send(MakeHolder<IEventHandle>(
        it->second.ServiceId,
        ctx.SelfID,
        request.release(),
        0, // flags
        it->second.RequestId,
        nullptr,
        std::move(it->second.TraceId)));
}

void TICStorageTransportActor::HandleSyncWithPersistentBufferResult(
    const NKikimr::NDDisk::TEvSyncWithPersistentBufferResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvSyncWithPersistentBufferResult with requestId# %lu",
        requestId);

    // That means that request is already completed
    if (!SyncEventsByRequestId.contains(requestId)) {
        LOG_DEBUG(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "SyncEvent with requestId# %lu not found",
            requestId);
        return;
    }

    auto& promise = SyncEventsByRequestId.at(requestId).Promise;

    promise.SetValue(std::move(ev->Get()->Record));
    SyncEventsByRequestId.erase(requestId);
}

///////////////////////////////////////////////////////////////////////////////

STFUNC(TICStorageTransportActor::StateWork)
{
    LOG_DEBUG(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
        "Processing event: %s from sender: %lu",
        ev->GetTypeName().data(),
        ev->Sender.LocalId());

    switch (ev->GetTypeRewrite()) {
        cFunc(TEvents::TEvPoison::EventType, PassAway);

        HFunc(TEvICStorageTransportPrivate::TEvConnect, HandleConnect);
        HFunc(TEvConnectResult, HandleConnectResult);

        HFunc(TEvICStorageTransportPrivate::TEvWritePersistentBuffer, HandleWritePersistentBuffer);
        HFunc(TEvWritePersistentBufferResult, HandleWritePersistentBufferResult);

        HFunc(TEvICStorageTransportPrivate::TEvErasePersistentBuffer, HandleErasePersistentBuffer);
        HFunc(TEvErasePersistentBufferResult, HandleErasePersistentBufferResult);

        HFunc(TEvICStorageTransportPrivate::TEvReadPersistentBuffer, HandleReadPersistentBuffer);
        HFunc(TEvReadPersistentBufferResult, HandleReadPersistentBufferResult);

        HFunc(TEvICStorageTransportPrivate::TEvRead, HandleRead);
        HFunc(TEvReadResult, HandleReadResult);

        HFunc(TEvICStorageTransportPrivate::TEvSyncWithPersistentBuffer, HandleSyncWithPersistentBuffer);
        HFunc(NKikimr::NDDisk::TEvSyncWithPersistentBufferResult, HandleSyncWithPersistentBufferResult);

        default:
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Unhandled event type: " << ev->GetTypeRewrite()
                    << " event: " << ev->ToString());
            break;
    }
}

}   // namespace NYdb::NBS::NBlockStore
