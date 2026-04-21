#include "ic_storage_transport_actor.h"

#include <ydb/library/actors/util/rope.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

using namespace NActors;
using namespace NKikimr;

namespace {

template <typename TEvent, typename TMap>
void RejectAllPending(TMap& map)
{
    for (auto& request: map) {
        request.second->Promise.SetValue(
            TEvent(
                NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
                "TICStorageTransportActor is destroyed")
                .Record);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TICStorageTransportActor::~TICStorageTransportActor()
{
    RejectAllPending<NDDisk::TEvConnectResult>(ConnectRequests);
    RejectAllPending<NDDisk::TEvReadPersistentBufferResult>(
        ReadFromPBufferRequests);
    RejectAllPending<NDDisk::TEvReadResult>(ReadFromDDiskRequests);
    RejectAllPending<NDDisk::TEvWritePersistentBufferResult>(
        WriteToPBufferRequests);
    RejectAllPending<NDDisk::TEvSyncWithPersistentBufferResult>(
        FlushFromPBufferRequests);
    RejectAllPending<NDDisk::TEvErasePersistentBufferResult>(
        EraseFromPBufferRequests);
    RejectAllPending<NDDisk::TEvListPersistentBufferResult>(
        ListPBufferEntriesRequests);
}

void TICStorageTransportActor::Bootstrap(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Become(&TThis::StateWork);
}

void TICStorageTransportActor::HandleConnect(
    const TEvTransportPrivate::TEvConnect::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;
    auto [it, inserted] =
        ConnectRequests.emplace(requestId, ev->Release().Release());
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvConnect with requestId# %lu",
        requestId);

    auto request = std::make_unique<NDDisk::TEvConnect>(msg->Credentials);

    ctx.Send(
        msg->ServiceId,
        request.release(),
        0,          // flags
        requestId   // cookie
    );
}

void TICStorageTransportActor::HandleConnectResult(
    const NDDisk::TEvConnectResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvConnectResult with requestId# %lu",
        requestId);

    if (auto* r = ConnectRequests.FindPtr(requestId)) {
        auto& request = **r;
        request.Promise.SetValue(std::move(ev->Get()->Record));
        ConnectRequests.erase(requestId);
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
    const TEvTransportPrivate::TEvWriteToPBuffer::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;
    auto [it, inserted] =
        WriteToPBufferRequests.emplace(requestId, ev->Release().Release());
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvWriteToPBuffer with requestId# %lu",
        requestId);

    if (auto guard = msg->Data.Acquire()) {
        auto request = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
            msg->Credentials,
            msg->Selector,
            msg->Lsn,
            msg->Instruction);

        const auto& sglist = guard.Get();
        TRope rope = TRope::Uninitialized(SgListGetSize(sglist));
        SgListCopy(sglist, CreateSgList(rope));
        request->AddPayload(std::move(rope));

        ctx.Send(MakeHolder<IEventHandle>(
            msg->ServiceId,
            ctx.SelfID,
            request.release(),
            0,           // flags
            requestId,   // cookie
            nullptr,
            std::move(msg->TraceId)));

        return;
    }

    LOG_ERROR(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvWriteToPBuffer with requestId# %lu was failed - can't "
        "acquire data. Aborting.",
        requestId);

    auto errorResponse =
        std::make_unique<NDDisk::TEvWritePersistentBufferResult>();
    errorResponse->Record.SetStatus(
        NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR);
    errorResponse->Record.SetErrorReason(
        "NBS TEvWritePersistentBuffer: can't acquire data");

    ctx.Send(MakeHolder<IEventHandle>(
        ctx.SelfID,
        ctx.SelfID,
        errorResponse.release(),
        0,           // flags
        requestId,   // cookie
        nullptr,
        std::move(msg->TraceId)));
}

void TICStorageTransportActor::HandleWritePersistentBufferResult(
    const NDDisk::TEvWritePersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvWritePersistentBufferResult with requestId# %lu",
        requestId);

    if (auto* r = WriteToPBufferRequests.FindPtr(requestId)) {
        auto& request = **r;
        request.Promise.SetValue(std::move(ev->Get()->Record));
        WriteToPBufferRequests.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "WritePersistentBufferEvent with requestId# %lu not found",
            requestId);
    }
}

void TICStorageTransportActor::HandleWriteToManyPersistentBuffers(
    const TEvTransportPrivate::TEvWriteToManyPBuffers::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;
    auto [it, inserted] =
        WriteToManyPBuffersRequests.emplace(requestId, ev->Release().Release());
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent WriteToManyPersistentBuffers/TEvWriteToPBuffers with requestId# "
        "%lu",
        requestId);

    if (auto guard = msg->Data.Acquire()) {
        auto request = std::make_unique<TEvWriteToManyPersistentBuffers>(
            msg->Credentials,
            msg->Selector,
            msg->Lsn,
            msg->Instruction,
            msg->PersistentBufferIds,
            msg->ReplyTimeout.MicroSeconds());

        const auto& sglist = guard.Get();
        TRope rope = TRope::Uninitialized(SgListGetSize(sglist));
        SgListCopy(sglist, CreateSgList(rope));
        request->AddPayload(std::move(rope));

        ctx.Send(MakeHolder<IEventHandle>(
            msg->ServiceId,
            ctx.SelfID,
            request.release(),
            0,           // flags
            requestId,   // cookie
            nullptr,
            std::move(msg->TraceId)));
        return;
    }

    LOG_ERROR(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent WriteToManyPersistentBuffers/TEvWriteToPBuffers with "
        "requestId# %lu was failed - can't acquire data. Immediate error's "
        "returning.",
        requestId);

    auto errorResponse =
        std::make_unique<TEvWriteToManyPersistentBuffersResult>();
    for (const auto& pbufferId: msg->PersistentBufferIds) {
        auto* res = errorResponse->Record.AddResult();
        auto* pbId = res->MutablePersistentBufferId();
        *pbId = pbufferId;
        auto* singlePbufferResult = res->MutableResult();
        singlePbufferResult->SetStatus(
            NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR);
        singlePbufferResult->SetErrorReason(
            "NBS WriteToManyPersistentBuffers/TEvWriteToPBuffers: can't "
            "acquire data");
    }
    ctx.Send(MakeHolder<IEventHandle>(
        ctx.SelfID,
        ctx.SelfID,
        errorResponse.release(),
        0,           // flags
        requestId,   // cookie
        nullptr,
        std::move(msg->TraceId)));
}

void TICStorageTransportActor::HandleWriteToManyPersistentBuffersResult(
    const TEvWriteToManyPersistentBuffersResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvWriteToManyPersistentBuffersResult with requestId# %lu",
        requestId);

    if (auto* r = WriteToManyPBuffersRequests.FindPtr(requestId)) {
        auto& request = **r;
        request.Promise.SetValue(std::move(ev->Get()->Record));
        WriteToManyPBuffersRequests.erase(requestId);
    } else {
        // That means that request is already completed
        // TODO handle this case in writeRequests through weak_ptr with erase
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "TEvWriteToManyPersistentBuffersResult with requestId# %lu not "
            "found",
            requestId);
    }
}

void TICStorageTransportActor::HandleWriteToDDisk(
    const TEvTransportPrivate::TEvWriteToDDisk::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;
    auto [it, inserted] =
        WriteToDDiskRequests.emplace(requestId, ev->Release().Release());
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent HandleWriteToDDisk with requestId# "
        "%lu",
        requestId);

    if (auto guard = msg->Data.Acquire()) {
        auto request = std::make_unique<NDDisk::TEvWrite>(
            msg->Credentials,
            msg->Selector,
            msg->Instruction);

        const auto& sglist = guard.Get();
        TRope rope = TRope::Uninitialized(SgListGetSize(sglist));
        SgListCopy(sglist, CreateSgList(rope));
        request->AddPayload(std::move(rope));

        ctx.Send(MakeHolder<IEventHandle>(
            msg->ServiceId,
            ctx.SelfID,
            request.release(),
            0,           // flags
            requestId,   // cookie
            nullptr,
            std::move(msg->TraceId)));
        return;
    }

    LOG_ERROR(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent HandleWriteToDDisk with requestId# %lu was failed - can't "
        "acquire data. Immediate error's returning.",
        requestId);

    auto errorResponse = std::make_unique<NKikimr::NDDisk::TEvWriteResult>(
        NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
        "NBS HandleWriteToDDisk: can't acquire data");

    ctx.Send(MakeHolder<IEventHandle>(
        ctx.SelfID,
        ctx.SelfID,
        errorResponse.release(),
        0,           // flags
        requestId,   // cookie
        nullptr,
        std::move(msg->TraceId)));
}

void TICStorageTransportActor::HandleWriteToDDiskResult(
    const NKikimr::NDDisk::TEvWriteResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received HandleWriteToDDiskResult with requestId# %lu",
        requestId);

    if (auto* r = WriteToDDiskRequests.FindPtr(requestId)) {
        auto& request = **r;
        request.Promise.SetValue(std::move(ev->Get()->Record));
        WriteToDDiskRequests.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "HandleWriteToDDiskResult with requestId# %lu not "
            "found",
            requestId);
    }
}

void TICStorageTransportActor::HandleErasePersistentBuffer(
    const TEvTransportPrivate::TEvEraseFromPBuffer::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;
    auto [it, inserted] =
        EraseFromPBufferRequests.emplace(requestId, ev->Release().Release());
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvEraseFromPBuffer with requestId# %lu",
        requestId);

    auto request = std::make_unique<NDDisk::TEvBatchErasePersistentBuffer>(
        msg->Credentials);
    for (size_t i = 0; i < msg->Lsns.size(); ++i) {
        request->AddErase(msg->Lsns[i], msg->Credentials.Generation);
    }

    ctx.Send(MakeHolder<IEventHandle>(
        msg->ServiceId,
        ctx.SelfID,
        request.release(),
        0,           // flags
        requestId,   // cookie
        nullptr,
        std::move(msg->TraceId)));
}

void TICStorageTransportActor::HandleErasePersistentBufferResult(
    const NDDisk::TEvErasePersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvErasePersistentBufferResult with requestId# %lu",
        requestId);

    if (auto* r = EraseFromPBufferRequests.FindPtr(requestId)) {
        auto& request = **r;
        request.Promise.SetValue(std::move(ev->Get()->Record));
        EraseFromPBufferRequests.erase(requestId);
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
    const TEvTransportPrivate::TEvReadFromPBuffer::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;

    auto [it, inserted] =
        ReadFromPBufferRequests.emplace(requestId, ev->Release().Release());
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvReadFromPBuffer with requestId# %lu",
        requestId);

    auto request = std::make_unique<NDDisk::TEvReadPersistentBuffer>(
        msg->Credentials,
        msg->Selector,
        msg->Lsn,
        msg->Credentials.Generation,
        msg->Instruction);

    ctx.Send(MakeHolder<IEventHandle>(
        msg->ServiceId,
        ctx.SelfID,
        request.release(),
        0,           // flags
        requestId,   // cookie
        nullptr,
        std::move(msg->TraceId)));
}

void TICStorageTransportActor::HandleReadPersistentBufferResult(
    const NDDisk::TEvReadPersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvReadPersistentBufferResult with requestId# %lu",
        requestId);

    // That means that request is already completed
    if (auto* r = ReadFromPBufferRequests.FindPtr(requestId)) {
        auto& request = **r;
        if (auto guard = request.Data.Acquire()) {
            const auto& sglist = guard.Get();
            SgListCopy(CreateSgList(ev->Get()->GetPayload()), sglist);
            request.Promise.SetValue(std::move(ev->Get()->Record));
        } else {
            LOG_ERROR(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "Recieved TEvReadPersistentBufferResult with requestId# %lu "
                "was failed - can't acquire data. Aborting.",
                requestId);

            NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult
                errorResult;
            errorResult.SetStatus(
                NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR);
            errorResult.SetErrorReason(
                "NBS HandleReadPersistentBufferResult: can't acquire data");
            request.Promise.SetValue(std::move(errorResult));
        }

        ReadFromPBufferRequests.erase(requestId);
    } else {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "ReadPersistentBufferEvent with requestId# %lu not found",
            requestId);
    }
}

void TICStorageTransportActor::HandleRead(
    const TEvTransportPrivate::TEvReadFromDDisk::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const ui64 requestId = ++RequestIdGenerator;

    auto [it, inserted] =
        ReadFromDDiskRequests.emplace(requestId, ev->Release().Release());
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvReadFromDDisk with requestId# %lu",
        requestId);

    auto request = std::make_unique<NDDisk::TEvRead>(
        msg->Credentials,
        msg->Selector,
        msg->Instruction);

    ctx.Send(MakeHolder<IEventHandle>(
        msg->ServiceId,
        ctx.SelfID,
        request.release(),
        0,           // flags
        requestId,   // cookie
        nullptr,
        std::move(msg->TraceId)));
}

void TICStorageTransportActor::HandleReadResult(
    const NDDisk::TEvReadResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvReadResult with requestId# %lu",
        requestId);

    if (auto* r = ReadFromDDiskRequests.FindPtr(requestId)) {
        auto& request = **r;
        if (auto guard = request.Data.Acquire()) {
            const auto& sglist = guard.Get();
            SgListCopy(CreateSgList(ev->Get()->GetPayload()), sglist);
            request.Promise.SetValue(std::move(ev->Get()->Record));
        } else {
            LOG_ERROR(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "Recieved TEvReadResult with requestId# %lu was failed - can't "
                "acquire data.",
                requestId);

            NKikimrBlobStorage::NDDisk::TEvReadResult errorResult;
            errorResult.SetStatus(
                NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR);
            errorResult.SetErrorReason(
                "NBS HandleReadResult: can't acquire data");
            request.Promise.SetValue(std::move(errorResult));
        }

        ReadFromDDiskRequests.erase(requestId);
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
    const TEvTransportPrivate::TEvSyncWithPBuffer::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;

    auto [it, inserted] =
        FlushFromPBufferRequests.emplace(requestId, ev->Release().Release());
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Sent TEvSyncWithPBuffer with requestId# %lu",
        requestId);

    auto request = std::make_unique<NDDisk::TEvSyncWithPersistentBuffer>(
        msg->Credentials,
        std::make_tuple(
            msg->PBufferId.NodeId,
            msg->PBufferId.PDiskId,
            msg->PBufferId.DDiskSlotId),
        msg->PBufferCredentials.DDiskInstanceGuid);

    for (size_t i = 0; i < msg->Selectors.size(); ++i) {
        request->AddSegment(
            msg->Selectors[i],
            msg->Lsns[i],
            msg->Credentials.Generation);
    }

    ctx.Send(MakeHolder<IEventHandle>(
        msg->ServiceId,
        ctx.SelfID,
        request.release(),
        0,   // flags
        requestId,
        nullptr,
        std::move(msg->TraceId)));
}

void TICStorageTransportActor::HandleSyncWithPersistentBufferResult(
    const NDDisk::TEvSyncWithPersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received TEvSyncWithPersistentBufferResult with requestId# %lu",
        requestId);

    if (auto* r = FlushFromPBufferRequests.FindPtr(requestId)) {
        auto& request = **r;
        request.Promise.SetValue(std::move(ev->Get()->Record));
        FlushFromPBufferRequests.erase(requestId);
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
    const TEvTransportPrivate::TEvListPBufferEntries::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;

    ListPBufferEntriesRequests.emplace(requestId, ev->Release().Release());

    auto request =
        std::make_unique<NDDisk::TEvListPersistentBuffer>(msg->Credentials);

    ctx.Send(
        msg->ServiceId,
        request.release(),
        0,   // flags
        requestId);
}

void TICStorageTransportActor::HandleListPersistentBufferResult(
    const NDDisk::TEvListPersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    auto requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Received HandleListPersistentBufferResult with requestId# %lu",
        requestId);

    if (auto* r = ListPBufferEntriesRequests.FindPtr(requestId)) {
        auto& request = **r;
        request.Promise.SetValue(std::move(ev->Get()->Record));
        ListPBufferEntriesRequests.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "ListPBufferEntries with requestId# %lu not found",
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

        HFunc(TEvTransportPrivate::TEvConnect, HandleConnect);
        HFunc(NDDisk::TEvConnectResult, HandleConnectResult);

        HFunc(
            TEvTransportPrivate::TEvWriteToPBuffer,
            HandleWritePersistentBuffer);
        HFunc(
            NDDisk::TEvWritePersistentBufferResult,
            HandleWritePersistentBufferResult);

        HFunc(
            TEvTransportPrivate::TEvWriteToManyPBuffers,
            HandleWriteToManyPersistentBuffers);
        HFunc(
            TEvWriteToManyPersistentBuffersResult,
            HandleWriteToManyPersistentBuffersResult);

        HFunc(TEvTransportPrivate::TEvWriteToDDisk, HandleWriteToDDisk);
        HFunc(NDDisk::TEvWriteResult, HandleWriteToDDiskResult);

        HFunc(
            TEvTransportPrivate::TEvEraseFromPBuffer,
            HandleErasePersistentBuffer);
        HFunc(
            NDDisk::TEvErasePersistentBufferResult,
            HandleErasePersistentBufferResult);

        HFunc(
            TEvTransportPrivate::TEvReadFromPBuffer,
            HandleReadPersistentBuffer);
        HFunc(
            NDDisk::TEvReadPersistentBufferResult,
            HandleReadPersistentBufferResult);

        HFunc(TEvTransportPrivate::TEvReadFromDDisk, HandleRead);
        HFunc(NDDisk::TEvReadResult, HandleReadResult);

        HFunc(
            TEvTransportPrivate::TEvSyncWithPBuffer,
            HandleSyncWithPersistentBuffer);
        HFunc(
            NKikimr::NDDisk::TEvSyncWithPersistentBufferResult,
            HandleSyncWithPersistentBufferResult);

        HFunc(
            TEvTransportPrivate::TEvListPBufferEntries,
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

////////////////////////////////////////////////////////////////////////////////

TActorId CreateTransportActor()
{
    auto actor = std::make_unique<TICStorageTransportActor>();

    return TActivationContext::Register(
        actor.release(),
        TActorId(),
        TMailboxType::ReadAsFilled,
        NKikimr::AppData()->SystemPoolId);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
