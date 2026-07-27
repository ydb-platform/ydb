#include "ic_storage_transport_actor.h"

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error_utils.h>

#include <ydb/library/actors/util/rope.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void SetErrorStatus(
    NKikimrBlobStorage::NDDisk::TReplyStatus_E status,
    TStringBuf reason,
    T& record)
{
    record.SetStatus(status);
    record.SetErrorReason(TString(reason));
}

std::unique_ptr<NDDisk::TEvWritePersistentBuffersResult>
MakeWritePersistentBuffersResult(
    NKikimrBlobStorage::NDDisk::TReplyStatus_E status,
    TStringBuf reason,
    std::span<const NKikimrBlobStorage::NDDisk::TDDiskId> pbufferIds)
{
    auto errorResponse =
        std::make_unique<NDDisk::TEvWritePersistentBuffersResult>();
    for (const auto& pbufferId: pbufferIds) {
        auto* res = errorResponse->Record.AddResult();
        *res->MutablePersistentBufferId() = pbufferId;
        SetErrorStatus(status, reason, *res->MutableResult());
    }
    return errorResponse;
}

template <typename TEvent, typename TMap>
void RejectAllPending(TMap& map)
{
    for (auto& request: map) {
        TEvent event;
        SetErrorStatus(
            NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
            DestroyErrorMessage,
            event.Record);
        request.second->Promise.SetValue(std::move(event.Record));
    }
    map.clear();
}

void RejectAllPending(
    THashMap<ui64, std::unique_ptr<TEvTransportPrivate::TEvConnect>>& map)
{
    for (auto& request: map) {
        NKikimrBlobStorage::NDDisk::TEvConnectResult record;
        SetErrorStatus(
            NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
            DestroyErrorMessage,
            record);
        request.second->ConnectPromise.SetValue(std::move(record));
    }
    map.clear();
}

template <typename T>
void SetUndeliveryError(T& record)
{
    SetErrorStatus(
        NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
        UndeliveryErrorMessage,
        record);
}

template <typename T>
void SetSessionBrokenError(T& record)
{
    SetErrorStatus(
        NKikimrBlobStorage::NDDisk::TReplyStatus::OUTDATED,
        SessionBrokenErrorMessage,
        record);
}

template <typename TEvent, typename TMap>
void RejectRequestsForNode(TMap& map, ui32 nodeId)
{
    for (auto it = map.begin(); it != map.end();) {
        auto& request = it->second;
        if (request->ServiceId.NodeId() == nodeId) {
            TEvent event;
            SetSessionBrokenError(event.Record);
            request->Promise.SetValue(std::move(event.Record));

            map.erase(it++);
        } else {
            ++it;
        }
    }
}

void RejectRequestsForNode(
    THashMap<ui64, std::unique_ptr<TEvTransportPrivate::TEvConnect>>& map,
    ui32 nodeId)
{
    for (auto it = map.begin(); it != map.end();) {
        auto& request = it->second;
        if (request->ServiceId.NodeId() == nodeId) {
            NKikimrBlobStorage::NDDisk::TEvConnectResult record;
            SetSessionBrokenError(record);
            request->DisconnectPromise.SetValue(nodeId);

            map.erase(it++);
        } else {
            ++it;
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TActorId CreateTransportActor(const TString& diskId, ui32 dbgIndex)
{
    auto actor = std::make_unique<TICStorageTransportActor>(diskId, dbgIndex);

    return TActivationContext::Register(
        actor.release(),
        TActorId(),
        TMailboxType::ReadAsFilled,
        NKikimr::AppData()->SystemPoolId);
}

////////////////////////////////////////////////////////////////////////////////

TICStorageTransportActor::TICStorageTransportActor(
    const TString& diskId,
    ui32 dbgIndex)
    : LogTitle(
          GetCycleCount(),
          TLogTitle::TInterconnectTransport{
              .DiskId = diskId,
              .DBGIndex = dbgIndex})
{}

TICStorageTransportActor::~TICStorageTransportActor()
{
    RejectAllPending(ConnectRequests);
    RejectAllPending<NDDisk::TEvReadPersistentBufferResult>(
        ReadFromPBufferRequests);
    RejectAllPending<NDDisk::TEvReadResult>(ReadFromDDiskRequests);
    RejectAllPending<NDDisk::TEvWritePersistentBufferResult>(
        WriteToPBufferRequests);
    RejectAllPending<NDDisk::TEvWriteResult>(WriteToDDiskRequests);
    RejectAllPending<NDDisk::TEvSyncResult>(FlushFromPBufferRequests);
    RejectAllPending<NDDisk::TEvErasePersistentBufferResult>(
        BatchEraseFromPBufferRequests);
    RejectAllPending<NDDisk::TEvErasePersistentBufferResult>(
        BarrierEraseFromPBufferRequests);
    RejectAllPending<NDDisk::TEvListPersistentBufferResult>(
        ListPBufferEntriesRequests);

    for (auto& [id, requestInfo]: WriteToManyPBuffersRequests) {
        auto response = MakeWritePersistentBuffersResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
            DestroyErrorMessage,
            requestInfo.Request->PersistentBufferIds);
        requestInfo.Request->Reply(response->Record);
    }
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
    const ui64 requestId = ++RequestIdGenerator;
    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Sent TEvConnect with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    auto [it, inserted] =
        ConnectRequests.emplace(requestId, ev->Release().Release());
    Y_ABORT_UNLESS(inserted);

    const auto& request = *it->second;

    SendWithUndeliveryTracking(
        ctx,
        request.ServiceId,
        std::make_unique<NDDisk::TEvConnect>(request.Credentials),
        requestId,
        NWilson::TTraceId());
}

void TICStorageTransportActor::HandleConnectUndelivery(
    const NKikimr::NDDisk::TEvConnect::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_WARN(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received NDDisk::TEvConnect undelivery with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto* r = ConnectRequests.FindPtr(requestId)) {
        auto& request = **r;
        auto result = NKikimrBlobStorage::NDDisk::TEvConnectResult();
        SetUndeliveryError(result);
        request.ConnectPromise.SetValue(std::move(result));
        ConnectRequests.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s ConnectEvent with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
            requestId);
    }
}

void TICStorageTransportActor::HandleConnectResult(
    const NDDisk::TEvConnectResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received TEvConnectResult with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto* r = ConnectRequests.FindPtr(requestId)) {
        auto& request = **r;
        request.ConnectPromise.SetValue(std::move(ev->Get()->Record));

        ICSubscribedNodes[request.ServiceId.NodeId()].push_back(
            request.DisconnectPromise);
        ConnectRequests.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s ConnectEvent with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
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
        "%s Sent TEvWriteToPBuffer with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
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
        request->AddPayloadThenChecksum(std::move(rope));
        // TODO(RFC 006): checksums should be computed by the Partition and
        // carried down to here rather than recomputed post-copy; computing it
        // after SgListCopy only covers corruption from this point on and bakes
        // in anything already wrong upstream of the copy.

        SendWithUndeliveryTracking(
            ctx,
            msg->ServiceId,
            std::move(request),
            requestId,
            std::move(msg->TraceId));

        return;
    }

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Sent TEvWriteToPBuffer with requestId# %lu was failed - can't "
        "acquire data. Returning an immediate error.",
        LogTitle.GetWithTime().c_str(),
        requestId);

    auto errorResponse =
        std::make_unique<NDDisk::TEvWritePersistentBufferResult>();
    SetCantAcquireStatus(errorResponse->Record);

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
        "%s Received TEvWritePersistentBufferResult with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
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
            "%s WritePersistentBufferEvent with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
            requestId);
    }
}

void TICStorageTransportActor::HandleWritePersistentBufferUndelivery(
    const NKikimr::NDDisk::TEvWritePersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_WARN(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received NDDisk::TEvWritePersistentBuffer undelivery with "
        "requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto* r = WriteToPBufferRequests.FindPtr(requestId)) {
        auto& request = **r;
        auto result =
            NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult();
        SetUndeliveryError(result);
        request.Promise.SetValue(std::move(result));
        WriteToPBufferRequests.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s WritePersistentBufferEvent with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
            requestId);
    }
}

void TICStorageTransportActor::HandleWriteToManyPersistentBuffers(
    const TEvTransportPrivate::TEvWriteToManyPBuffers::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;
    TSet<NKikimrBlobStorage::NDDisk::TDDiskId, TDDiskIdLess> waitingReplies;
    for (const auto& diskId: msg->PersistentBufferIds) {
        waitingReplies.emplace(diskId);
    }

    auto [it, inserted] = WriteToManyPBuffersRequests.emplace(
        requestId,
        TWriteToManyPBuffersReqInfo{
            .Request =
                std::unique_ptr<TEvTransportPrivate::TEvWriteToManyPBuffers>(
                    ev->Release().Release()),
            .WaitingReplies = std::move(waitingReplies),
        });
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Sent WriteToManyPersistentBuffers/TEvWriteToPBuffers with "
        "requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto guard = msg->Data.Acquire()) {
        auto request =
            std::make_unique<NKikimr::NDDisk::TEvWritePersistentBuffers>(
                msg->Credentials,
                msg->Selector,
                msg->Lsn,
                msg->Instruction,
                msg->PersistentBufferIds,
                msg->ReplyTimeout.MicroSeconds());

        const auto& sglist = guard.Get();
        TRope rope = TRope::Uninitialized(SgListGetSize(sglist));
        SgListCopy(sglist, CreateSgList(rope));
        request->AddPayloadThenChecksum(std::move(rope));
        // TODO(RFC 006): checksums should be computed by the Partition and
        // carried down to here rather than recomputed post-copy; computing it
        // after SgListCopy only covers corruption from this point on and bakes
        // in anything already wrong upstream of the copy.

        SendWithUndeliveryTracking(
            ctx,
            msg->ServiceId,
            std::move(request),
            requestId,
            std::move(msg->TraceId));
        return;
    }

    LOG_ERROR(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Sent WriteToManyPersistentBuffers/TEvWriteToPBuffers with "
        "requestId# %lu was failed - can't acquire data. Immediate error's "
        "returning.",
        LogTitle.GetWithTime().c_str(),
        requestId);

    auto errorResponse = MakeWritePersistentBuffersResult(
        NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN,
        CantAcquireDataErrorMessage,
        msg->PersistentBufferIds);
    ctx.Send(MakeHolder<IEventHandle>(
        ctx.SelfID,
        ctx.SelfID,
        errorResponse.release(),
        0,           // flags
        requestId,   // cookie
        nullptr,
        std::move(msg->TraceId)));
}

void TICStorageTransportActor::HandleWriteToManyPersistentBuffersUndelivery(
    const NKikimr::NDDisk::TEvWritePersistentBuffers::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_WARN(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received NDDisk::TEvWritePersistentBuffers undelivery with "
        "requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto* r = WriteToManyPBuffersRequests.FindPtr(requestId)) {
        auto& requestInfo = *r;
        auto& request = *requestInfo.Request;

        // Will reply undelivered only for coordinator.
        const NKikimrBlobStorage::NDDisk::TDDiskId coordinator[1] = {
            request.PersistentBufferIds[0]};
        auto response = MakeWritePersistentBuffersResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
            UndeliveryErrorMessage,
            coordinator);
        request.Reply(response->Record);
        WriteToManyPBuffersRequests.erase(requestId);
    } else {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s WriteToManyPersistentBuffersEvent with requestId# %lu not "
            "found",
            LogTitle.GetWithTime().c_str(),
            requestId);
    }
}

void TICStorageTransportActor::HandleWriteToManyPersistentBuffersResult(
    const NKikimr::NDDisk::TEvWritePersistentBuffersResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received TEvWriteToManyPersistentBuffersResult with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto* r = WriteToManyPBuffersRequests.FindPtr(requestId)) {
        auto& requestInfo = *r;
        auto& request = *requestInfo.Request;
        auto& waitingReplies = requestInfo.WaitingReplies;

        request.Reply(ev->Get()->Record);

        for (const auto& singlePBufferResponse: ev->Get()->Record.GetResult()) {
            waitingReplies.erase(singlePBufferResponse.GetPersistentBufferId());
        }

        if (waitingReplies.empty()) {
            WriteToManyPBuffersRequests.erase(requestId);
        }
    } else {
        LOG_WARN(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s TEvWriteToManyPersistentBuffersResult with requestId# %lu not "
            "found",
            LogTitle.GetWithTime().c_str(),
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
        "%s Sent HandleWriteToDDisk with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
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

        SendWithUndeliveryTracking(
            ctx,
            msg->ServiceId,
            std::move(request),
            requestId,
            std::move(msg->TraceId));
        return;
    }

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Sent HandleWriteToDDisk with requestId# %lu was failed - can't "
        "acquire data. Returning an immediate error.",
        LogTitle.GetWithTime().c_str(),
        requestId);

    auto errorResponse = std::make_unique<NKikimr::NDDisk::TEvWriteResult>();
    SetCantAcquireStatus(errorResponse->Record);

    ctx.Send(MakeHolder<IEventHandle>(
        ctx.SelfID,
        ctx.SelfID,
        errorResponse.release(),
        0,           // flags
        requestId,   // cookie
        nullptr,
        std::move(msg->TraceId)));
}

void TICStorageTransportActor::HandleWriteToDDiskUndelivery(
    const NKikimr::NDDisk::TEvWrite::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_WARN(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received NDDisk::TEvWrite undelivery with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto* r = WriteToDDiskRequests.FindPtr(requestId)) {
        auto& request = **r;
        auto result = NKikimrBlobStorage::NDDisk::TEvWriteResult();
        SetUndeliveryError(result);
        request.Promise.SetValue(std::move(result));
        WriteToDDiskRequests.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s HandleWriteToDDiskEvent with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
            requestId);
    }
}

void TICStorageTransportActor::HandleWriteToDDiskResult(
    const NKikimr::NDDisk::TEvWriteResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received HandleWriteToDDiskResult with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
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
            "%s HandleWriteToDDiskResult with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
            requestId);
    }
}

void TICStorageTransportActor::HandleBatchErasePersistentBuffer(
    const TEvTransportPrivate::TEvBatchEraseFromPBuffer::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;
    auto [it, inserted] = BatchEraseFromPBufferRequests.emplace(
        requestId,
        ev->Release().Release());
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Sent TEvBatchEraseFromPBuffer with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    auto request = std::make_unique<NDDisk::TEvBatchErasePersistentBuffer>(
        msg->Credentials);
    for (auto lsn: msg->Lsns) {
        request->AddErase(lsn, msg->Credentials.Generation);
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

void TICStorageTransportActor::HandleBarrierErasePersistentBuffer(
    const TEvTransportPrivate::TEvBarrierEraseFromPBuffer::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const ui64 requestId = ++RequestIdGenerator;
    auto [it, inserted] = BarrierEraseFromPBufferRequests.emplace(
        requestId,
        ev->Release().Release());
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Sent TEvBarrierEraseFromPBuffer with requestId# %lu lsn# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId,
        msg->Lsn);

    auto request = std::make_unique<NDDisk::TEvErasePersistentBuffer>(
        msg->Credentials,
        msg->Lsn);

    SendWithUndeliveryTracking(
        ctx,
        msg->ServiceId,
        std::move(request),
        requestId,
        std::move(msg->TraceId));
}

void TICStorageTransportActor::HandleBatchErasePersistentBufferUndelivery(
    const NDDisk::TEvBatchErasePersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_WARN(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received NDDisk::TEvBatchErasePersistentBuffer undelivery with "
        "requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto* r = BatchEraseFromPBufferRequests.FindPtr(requestId)) {
        auto& request = **r;
        auto result =
            NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult();
        SetUndeliveryError(result);
        request.Promise.SetValue(std::move(result));
        BatchEraseFromPBufferRequests.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s TEvBatchErasePersistentBuffer with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
            requestId);
    }
}

void TICStorageTransportActor::HandleBarrierErasePersistentBufferUndelivery(
    const NDDisk::TEvErasePersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_WARN(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received NDDisk::TEvErasePersistentBuffer undelivery with "
        "requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto* r = BarrierEraseFromPBufferRequests.FindPtr(requestId)) {
        auto& request = **r;
        auto result =
            NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult();
        SetUndeliveryError(result);
        request.Promise.SetValue(std::move(result));
        BarrierEraseFromPBufferRequests.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s TEvErasePersistentBuffer with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
            requestId);
    }
}

void TICStorageTransportActor::HandleErasePersistentBufferResult(
    const NDDisk::TEvErasePersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received TEvErasePersistentBufferResult with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto* r = BatchEraseFromPBufferRequests.FindPtr(requestId)) {
        auto& request = **r;
        request.Promise.SetValue(std::move(ev->Get()->Record));
        BatchEraseFromPBufferRequests.erase(requestId);
        return;
    }
    if (auto* r = BarrierEraseFromPBufferRequests.FindPtr(requestId)) {
        auto& request = **r;
        request.Promise.SetValue(std::move(ev->Get()->Record));
        BarrierEraseFromPBufferRequests.erase(requestId);
        return;
    }
    // That means that request is already completed
    LOG_ERROR(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s ErasePersistentBufferEvent with requestId# %lu not found",
        LogTitle.GetWithTime().c_str(),
        requestId);
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
        "%s Sent TEvReadFromPBuffer with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    auto request = std::make_unique<NDDisk::TEvReadPersistentBuffer>(
        msg->Credentials,
        msg->Selector,
        msg->Lsn,
        msg->Credentials.Generation,
        msg->Instruction);

    SendWithUndeliveryTracking(
        ctx,
        msg->ServiceId,
        std::move(request),
        requestId,
        std::move(msg->TraceId));
}

void TICStorageTransportActor::HandleReadPersistentBufferUndelivery(
    const NKikimr::NDDisk::TEvReadPersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_WARN(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received NDDisk::TEvReadPersistentBuffer undelivery with "
        "requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto* r = ReadFromPBufferRequests.FindPtr(requestId)) {
        auto& request = **r;
        auto result =
            NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult();
        SetUndeliveryError(result);
        request.Promise.SetValue(std::move(result));
        ReadFromPBufferRequests.erase(requestId);
    } else {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s ReadPersistentBufferEvent with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
            requestId);
    }
}

void TICStorageTransportActor::HandleReadPersistentBufferResult(
    const NDDisk::TEvReadPersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received TEvReadPersistentBufferResult with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    // That means that request is already completed
    if (auto* r = ReadFromPBufferRequests.FindPtr(requestId)) {
        auto& request = **r;
        if (auto guard = request.Data.Acquire()) {
            const auto& sglist = guard.Get();
            SgListCopy(CreateSgList(ev->Get()->GetPayload()), sglist);
            request.Promise.SetValue(std::move(ev->Get()->Record));
        } else {
            LOG_INFO(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "%s Received TEvReadPersistentBufferResult with requestId# %lu "
                "was failed - can't acquire data. Aborting.",
                LogTitle.GetWithTime().c_str(),
                requestId);

            NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult
                errorResult;
            SetCantAcquireStatus(errorResult);
            request.Promise.SetValue(std::move(errorResult));
        }

        ReadFromPBufferRequests.erase(requestId);
    } else {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s ReadPersistentBufferEvent with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
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
        "%s Sent TEvReadFromDDisk with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    auto request = std::make_unique<NDDisk::TEvRead>(
        msg->Credentials,
        msg->Selector,
        msg->Instruction);

    SendWithUndeliveryTracking(
        ctx,
        msg->ServiceId,
        std::move(request),
        requestId,
        std::move(msg->TraceId));
}

void TICStorageTransportActor::HandleReadUndelivery(
    const NKikimr::NDDisk::TEvRead::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_WARN(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received NDDisk::TEvRead undelivery with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto* r = ReadFromDDiskRequests.FindPtr(requestId)) {
        auto& request = **r;
        auto result = NKikimrBlobStorage::NDDisk::TEvReadResult();
        SetUndeliveryError(result);
        request.Promise.SetValue(std::move(result));
        ReadFromDDiskRequests.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s ReadEvent with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
            requestId);
    }
}

void TICStorageTransportActor::HandleReadResult(
    const NDDisk::TEvReadResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received TEvReadResult with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto* r = ReadFromDDiskRequests.FindPtr(requestId)) {
        auto& request = **r;
        if (auto guard = request.Data.Acquire()) {
            const auto& sglist = guard.Get();
            SgListCopy(CreateSgList(ev->Get()->GetPayload()), sglist);
            request.Promise.SetValue(std::move(ev->Get()->Record));
        } else {
            LOG_INFO(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "%s Received TEvReadResult with requestId# %lu was failed - "
                "can't acquire data.",
                LogTitle.GetWithTime().c_str(),
                requestId);

            NKikimrBlobStorage::NDDisk::TEvReadResult errorResult;
            SetCantAcquireStatus(errorResult);
            request.Promise.SetValue(std::move(errorResult));
        }

        ReadFromDDiskRequests.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s ReadEvent with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
            requestId);
    }
}

void TICStorageTransportActor::HandleSyncWithPersistentBuffer(
    const TEvTransportPrivate::TEvSyncWithPBuffer::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    // SyncWithPBuffer must only be issued for an established pbuffer
    // connection.
    Y_ABORT_UNLESS(msg->PBufferCredentials.DDiskInstanceGuid.has_value());

    const ui64 requestId = ++RequestIdGenerator;

    auto [it, inserted] =
        FlushFromPBufferRequests.emplace(requestId, ev->Release().Release());
    Y_ABORT_UNLESS(inserted);

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Sent TEvSyncWithPBuffer with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    auto request = std::make_unique<NDDisk::TEvSync>(msg->Credentials);
    const auto pBufferId = std::make_tuple(
        msg->PBufferId.NodeId,
        msg->PBufferId.PDiskId,
        msg->PBufferId.DDiskSlotId);

    Y_ABORT_UNLESS(msg->Selectors.size() == msg->Lsns.size());
    for (size_t i = 0; i < msg->Selectors.size(); ++i) {
        request->AddSegmentFromPB(
            pBufferId,
            *msg->PBufferCredentials.DDiskInstanceGuid,
            msg->Selectors[i],
            msg->Lsns[i],
            msg->Credentials.Generation);
    }

    SendWithUndeliveryTracking(
        ctx,
        msg->ServiceId,
        std::move(request),
        requestId,
        std::move(msg->TraceId));
}

void TICStorageTransportActor::HandleSyncWithPersistentBufferUndelivery(
    const NKikimr::NDDisk::TEvSync::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_WARN(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received NDDisk::TEvSync undelivery with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto* r = FlushFromPBufferRequests.FindPtr(requestId)) {
        auto& request = **r;
        auto result = NKikimrBlobStorage::NDDisk::TEvSyncResult();
        SetUndeliveryError(result);
        request.Promise.SetValue(std::move(result));
        FlushFromPBufferRequests.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s SyncEvent with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
            requestId);
    }
}

void TICStorageTransportActor::HandleSyncWithPersistentBufferResult(
    const NDDisk::TEvSyncResult::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received TEvSyncResult with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
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
            "%s SyncEvent with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
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

    SendWithUndeliveryTracking(
        ctx,
        msg->ServiceId,
        std::move(request),
        requestId,
        NWilson::TTraceId());
}

void TICStorageTransportActor::HandleListPersistentBufferUndelivery(
    const NKikimr::NDDisk::TEvListPersistentBuffer::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const ui64 requestId = ev->Cookie;

    LOG_WARN(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received NDDisk::TEvListPersistentBuffer undelivery with "
        "requestId# %lu",
        LogTitle.GetWithTime().c_str(),
        requestId);

    if (auto* r = ListPBufferEntriesRequests.FindPtr(requestId)) {
        auto& request = **r;
        auto result =
            NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult();
        SetUndeliveryError(result);
        request.Promise.SetValue(std::move(result));
        ListPBufferEntriesRequests.erase(requestId);
    } else {
        // That means that request is already completed
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s ListPBufferEntries with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
            requestId);
    }
}

void TICStorageTransportActor::HandleListPersistentBufferResult(
    const NDDisk::TEvListPersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    auto requestId = ev->Cookie;

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Received HandleListPersistentBufferResult with requestId# %lu",
        LogTitle.GetWithTime().c_str(),
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
            "%s ListPBufferEntries with requestId# %lu not found",
            LogTitle.GetWithTime().c_str(),
            requestId);
    }
}

void TICStorageTransportActor::PassAway()
{
    for (auto& [nodeId, promises]: ICSubscribedNodes) {
        for (auto& promise: promises) {
            if (!promise.HasValue()) {
                promise.SetValue(nodeId);
            }
        }

        if (nodeId != SelfId().NodeId()) {
            Send(
                TActivationContext::InterconnectProxy(nodeId),
                std::make_unique<TEvents::TEvUnsubscribe>().release());
        }
    }
    ICSubscribedNodes.clear();
    NActors::IActor::PassAway();
}

void TICStorageTransportActor::RejectAllSessionRequestsForNode(
    ui32 nodeId,
    const NActors::TActorContext& ctx)
{
    LOG_WARN(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s All session's requests for node #%u were rejected",
        LogTitle.GetWithTime().c_str(),
        nodeId);

    RejectRequestsForNode(ConnectRequests, nodeId);
    RejectRequestsForNode<NDDisk::TEvReadResult>(ReadFromDDiskRequests, nodeId);
    RejectRequestsForNode<NDDisk::TEvWriteResult>(WriteToDDiskRequests, nodeId);
    RejectRequestsForNode<NDDisk::TEvSyncResult>(
        FlushFromPBufferRequests,
        nodeId);
}

void TICStorageTransportActor::HandleICNodeDisconnected(
    const TEvInterconnect::TEvNodeDisconnected::TPtr& ev,
    const TActorContext& ctx)
{
    const ui32 nodeId = ev->Get()->NodeId;

    LOG_WARN(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Node #%u disconnected",
        LogTitle.GetWithTime().c_str(),
        nodeId);

    auto it = ICSubscribedNodes.find(nodeId);
    if (it != ICSubscribedNodes.end()) {
        for (auto& disconnectPromise: it->second) {
            if (!disconnectPromise.HasValue()) {
                disconnectPromise.SetValue(nodeId);
            }
        }
        ICSubscribedNodes.erase(it);
    }

    RejectAllSessionRequestsForNode(nodeId, ctx);
}

///////////////////////////////////////////////////////////////////////////////

STFUNC(TICStorageTransportActor::StateWork)
{
    LOG_DEBUG(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "%s Processing event: %s from sender: %lu",
        LogTitle.GetWithTime().c_str(),
        ev->GetTypeName().data(),
        ev->Sender.LocalId());

    switch (ev->GetTypeRewrite()) {
        cFunc(TEvents::TEvPoison::EventType, PassAway);

        HFunc(TEvTransportPrivate::TEvConnect, HandleConnect);
        HFunc(NDDisk::TEvConnect, HandleConnectUndelivery);
        HFunc(NDDisk::TEvConnectResult, HandleConnectResult);

        HFunc(
            TEvTransportPrivate::TEvWriteToPBuffer,
            HandleWritePersistentBuffer);
        HFunc(
            NDDisk::TEvWritePersistentBuffer,
            HandleWritePersistentBufferUndelivery);
        HFunc(
            NDDisk::TEvWritePersistentBufferResult,
            HandleWritePersistentBufferResult);

        HFunc(
            TEvTransportPrivate::TEvWriteToManyPBuffers,
            HandleWriteToManyPersistentBuffers);
        HFunc(
            NKikimr::NDDisk::TEvWritePersistentBuffers,
            HandleWriteToManyPersistentBuffersUndelivery);
        HFunc(
            NKikimr::NDDisk::TEvWritePersistentBuffersResult,
            HandleWriteToManyPersistentBuffersResult);

        HFunc(TEvTransportPrivate::TEvWriteToDDisk, HandleWriteToDDisk);
        HFunc(NDDisk::TEvWrite, HandleWriteToDDiskUndelivery);
        HFunc(NDDisk::TEvWriteResult, HandleWriteToDDiskResult);

        HFunc(
            TEvTransportPrivate::TEvBatchEraseFromPBuffer,
            HandleBatchErasePersistentBuffer);
        HFunc(
            TEvTransportPrivate::TEvBarrierEraseFromPBuffer,
            HandleBarrierErasePersistentBuffer);
        HFunc(
            NDDisk::TEvBatchErasePersistentBuffer,
            HandleBatchErasePersistentBufferUndelivery);
        HFunc(
            NDDisk::TEvErasePersistentBuffer,
            HandleBarrierErasePersistentBufferUndelivery);
        HFunc(
            NDDisk::TEvErasePersistentBufferResult,
            HandleErasePersistentBufferResult);

        HFunc(
            TEvTransportPrivate::TEvReadFromPBuffer,
            HandleReadPersistentBuffer);
        HFunc(
            NDDisk::TEvReadPersistentBuffer,
            HandleReadPersistentBufferUndelivery);
        HFunc(
            NDDisk::TEvReadPersistentBufferResult,
            HandleReadPersistentBufferResult);

        HFunc(TEvTransportPrivate::TEvReadFromDDisk, HandleRead);
        HFunc(NDDisk::TEvRead, HandleReadUndelivery);
        HFunc(NDDisk::TEvReadResult, HandleReadResult);

        HFunc(
            TEvTransportPrivate::TEvSyncWithPBuffer,
            HandleSyncWithPersistentBuffer);
        HFunc(
            NKikimr::NDDisk::TEvSync,
            HandleSyncWithPersistentBufferUndelivery);
        HFunc(
            NKikimr::NDDisk::TEvSyncResult,
            HandleSyncWithPersistentBufferResult);

        HFunc(
            TEvTransportPrivate::TEvListPBufferEntries,
            HandleListPersistentBuffer);
        HFunc(
            NKikimr::NDDisk::TEvListPersistentBuffer,
            HandleListPersistentBufferUndelivery);
        HFunc(
            NKikimr::NDDisk::TEvListPersistentBufferResult,
            HandleListPersistentBufferResult);

        HFunc(TEvInterconnect::TEvNodeDisconnected, HandleICNodeDisconnected);
        IgnoreFunc(TEvInterconnect::TEvNodeConnected);

        default:
            LOG_ERROR(
                TActivationContext::AsActorContext(),
                NKikimrServices::NBS_PARTITION,
                "%s Unhandled event type: %u event %s ",
                LogTitle.GetWithTime().c_str(),
                ev->GetTypeRewrite(),
                ev->ToString().c_str());
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
