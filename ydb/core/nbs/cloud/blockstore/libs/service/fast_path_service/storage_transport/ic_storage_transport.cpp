#include "ic_storage_transport.h"

namespace NYdb::NBS::NBlockStore {

using namespace NThreading;
using namespace NActors;

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
    const NKikimr::NDDisk::TQueryCredentials credentials,
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
    const NKikimr::NDDisk::TQueryCredentials credentials,
    const NKikimr::NDDisk::TBlockSelector selector,
    const ui64 lsn,
    const NKikimr::NDDisk::TWriteInstruction instruction,
    TGuardedSgList data,
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
            requestId,
            std::move(promise)));

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvFlushPersistentBufferResult> TICStorageTransport::FlushPersistentBuffer(
    const NActors::TActorId serviceId,
    const NKikimr::NDDisk::TQueryCredentials credentials,
    const NKikimr::NDDisk::TBlockSelector selector,
    const ui64 lsn,
    const std::tuple<ui32, ui32, ui32> ddiskId,
    const ui64 ddiskInstanceGuid,
    const ui64 requestId)
{
    auto promise = NewPromise<NKikimrBlobStorage::NDDisk::TEvFlushPersistentBufferResult>();
    auto future = promise.GetFuture();

    NActors::TActivationContext::AsActorContext().Send(
        ICStorageTransportActorId,
        new TEvICStorageTransportPrivate::TEvFlushPersistentBuffer(
            serviceId,
            credentials,
            selector,
            lsn,
            ddiskId,
            ddiskInstanceGuid,
            requestId,
            std::move(promise)));

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult> TICStorageTransport::ErasePersistentBuffer(
    const NActors::TActorId serviceId,
    const NKikimr::NDDisk::TQueryCredentials credentials,
    const NKikimr::NDDisk::TBlockSelector selector,
    const ui64 lsn,
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
            requestId,
            std::move(promise)));

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult> TICStorageTransport::ReadPersistentBuffer(
    const NActors::TActorId serviceId,
    const NKikimr::NDDisk::TQueryCredentials credentials,
    const NKikimr::NDDisk::TBlockSelector selector,
    const ui64 lsn,
    const NKikimr::NDDisk::TReadInstruction instruction,
    TGuardedSgList data,
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
            requestId,
            std::move(promise)));

    return future;
}

TFuture<NKikimrBlobStorage::NDDisk::TEvReadResult> TICStorageTransport::Read(
    const NActors::TActorId serviceId,
    const NKikimr::NDDisk::TQueryCredentials credentials,
    const NKikimr::NDDisk::TBlockSelector selector,
    const NKikimr::NDDisk::TReadInstruction instruction,
    TGuardedSgList data,
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

    auto request = std::make_unique<NKikimr::NDDisk::TEvConnect>(msg->Credentials);
    ctx.Send(
        msg->ServiceId,
        request.release(),
        0, // flags
        msg->RequestId
    );

    PrivateEventsByRequestId.emplace(msg->RequestId, std::move(*msg));
}

void TICStorageTransportActor::HandleConnectResult(
    const NKikimr::NDDisk::TEvConnectResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto requestId = ev->Cookie;

    auto& promise = std::get<0>(PrivateEventsByRequestId.at(requestId)).Promise;

    promise.SetValue(std::move(ev->Get()->Record));
    PrivateEventsByRequestId.erase(requestId);
}

void TICStorageTransportActor::HandleWritePersistentBuffer(
    const TEvICStorageTransportPrivate::TEvWritePersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto request = std::make_unique<NKikimr::NDDisk::TEvWritePersistentBuffer>(
        msg->Credentials,
        msg->Selector,
        msg->Lsn,
        msg->Instruction);

    if (auto guard = msg->Data.Acquire()) {
        const auto& sglist = guard.Get();
        request->AddPayload(TRope(TString(sglist[0].Data(), sglist[0].Size())));
    } else {
        Y_ABORT_UNLESS(false);
    }

    ctx.Send(
        msg->ServiceId,
        request.release(),
        0, // flags
        msg->RequestId
    );

    PrivateEventsByRequestId.emplace(msg->RequestId, std::move(*msg));
}

void TICStorageTransportActor::HandleWritePersistentBufferResult(
    const NKikimr::NDDisk::TEvWritePersistentBufferResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto requestId = ev->Cookie;

    auto& promise = std::get<1>(PrivateEventsByRequestId.at(requestId)).Promise;

    promise.SetValue(std::move(ev->Get()->Record));
    PrivateEventsByRequestId.erase(requestId);
}

void TICStorageTransportActor::HandleFlushPersistentBuffer(
    const TEvICStorageTransportPrivate::TEvFlushPersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto request = std::make_unique<NKikimr::NDDisk::TEvFlushPersistentBuffer>(
        msg->Credentials,
        msg->Selector,
        msg->Lsn,
        msg->DDiskId,
        msg->DDiskInstanceGuid);

    ctx.Send(
        msg->ServiceId,
        request.release(),
        0, // flags
        msg->RequestId
    );

    PrivateEventsByRequestId.emplace(msg->RequestId, std::move(*msg));
}

void TICStorageTransportActor::HandleFlushPersistentBufferResult(
    const NKikimr::NDDisk::TEvFlushPersistentBufferResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto requestId = ev->Cookie;

    auto& promise = std::get<2>(PrivateEventsByRequestId.at(requestId)).Promise;

    promise.SetValue(std::move(ev->Get()->Record));
    PrivateEventsByRequestId.erase(requestId);
}

void TICStorageTransportActor::HandleErasePersistentBuffer(
    const TEvICStorageTransportPrivate::TEvErasePersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto request = std::make_unique<NKikimr::NDDisk::TEvErasePersistentBuffer>(
        msg->Credentials,
        msg->Selector,
        msg->Lsn);

    ctx.Send(
        msg->ServiceId,
        request.release(),
        0, // flags
        msg->RequestId
    );

    PrivateEventsByRequestId.emplace(msg->RequestId, std::move(*msg));
}

void TICStorageTransportActor::HandleErasePersistentBufferResult(
    const NKikimr::NDDisk::TEvErasePersistentBufferResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto requestId = ev->Cookie;

    auto& promise = std::get<3>(PrivateEventsByRequestId.at(requestId)).Promise;

    promise.SetValue(std::move(ev->Get()->Record));
    PrivateEventsByRequestId.erase(requestId);
}

void TICStorageTransportActor::HandleReadPersistentBuffer(
    const TEvICStorageTransportPrivate::TEvReadPersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto request = std::make_unique<NKikimr::NDDisk::TEvReadPersistentBuffer>(
        msg->Credentials,
        msg->Selector,
        msg->Lsn,
        msg->Instruction);

    ctx.Send(
        msg->ServiceId,
        request.release(),
        0, // flags
        msg->RequestId
    );

    PrivateEventsByRequestId.emplace(msg->RequestId, std::move(*msg));
}

void TICStorageTransportActor::HandleReadPersistentBufferResult(
    const NKikimr::NDDisk::TEvReadPersistentBufferResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto requestId = ev->Cookie;

    auto& promise = std::get<4>(PrivateEventsByRequestId.at(requestId)).Promise;
    auto& data = std::get<4>(PrivateEventsByRequestId.at(requestId)).Data;
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
    PrivateEventsByRequestId.erase(requestId);
}

void TICStorageTransportActor::HandleRead(
    const TEvICStorageTransportPrivate::TEvRead::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto request = std::make_unique<NKikimr::NDDisk::TEvRead>(
        msg->Credentials,
        msg->Selector,
        msg->Instruction);

    ctx.Send(
        msg->ServiceId,
        request.release(),
        0, // flags
        msg->RequestId
    );

    PrivateEventsByRequestId.emplace(msg->RequestId, std::move(*msg));
}

void TICStorageTransportActor::HandleReadResult(
    const NKikimr::NDDisk::TEvReadResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto requestId = ev->Cookie;

    auto& promise = std::get<5>(PrivateEventsByRequestId.at(requestId)).Promise;
    auto& data = std::get<5>(PrivateEventsByRequestId.at(requestId)).Data;
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
    PrivateEventsByRequestId.erase(requestId);
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
        HFunc(NKikimr::NDDisk::TEvConnectResult, HandleConnectResult);

        HFunc(TEvICStorageTransportPrivate::TEvWritePersistentBuffer, HandleWritePersistentBuffer);
        HFunc(NKikimr::NDDisk::TEvWritePersistentBufferResult, HandleWritePersistentBufferResult);

        HFunc(TEvICStorageTransportPrivate::TEvFlushPersistentBuffer, HandleFlushPersistentBuffer);
        HFunc(NKikimr::NDDisk::TEvFlushPersistentBufferResult, HandleFlushPersistentBufferResult);

        HFunc(TEvICStorageTransportPrivate::TEvErasePersistentBuffer, HandleErasePersistentBuffer);
        HFunc(NKikimr::NDDisk::TEvErasePersistentBufferResult, HandleErasePersistentBufferResult);

        HFunc(TEvICStorageTransportPrivate::TEvReadPersistentBuffer, HandleReadPersistentBuffer);
        HFunc(NKikimr::NDDisk::TEvReadPersistentBufferResult, HandleReadPersistentBufferResult);

        HFunc(TEvICStorageTransportPrivate::TEvRead, HandleRead);
        HFunc(NKikimr::NDDisk::TEvReadResult, HandleReadResult);

        default:
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Unhandled event type: " << ev->GetTypeRewrite()
                    << " event: " << ev->ToString());
            break;
    }
}

}   // namespace NYdb::NBS::NBlockStore
