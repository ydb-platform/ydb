#include "ic_direct_storage_transport.h"

#include "ddisk_helpers.h"
#include "ic_storage_transport_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/disk_description.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error_utils.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/sglist.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/util/rope.h>
#include <ydb/library/services/services.pb.h>

#include <atomic>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void SetSessionBrokenError(T& record)
{
    SetErrorStatus(
        NKikimrBlobStorage::NDDisk::TReplyStatus::OUTDATED,
        SessionBrokenErrorMessage,
        record);
}

////////////////////////////////////////////////////////////////////////////////

// Completes a single-reply request promise from IReplyHandler::Receive.
// On session / router death the destructor completes with OUTDATED /
// "Session broken".
template <typename TResultEvent, typename TRecord>
class TPromiseReplyHandler: public IReplyHandler
{
public:
    explicit TPromiseReplyHandler(TPromise<TRecord> promise)
        : Promise(std::move(promise))
    {}

    ~TPromiseReplyHandler() override
    {
        CompleteWithSessionBroken();
    }

    bool Receive(TAutoPtr<IEventHandle> ev) override
    {
        if (Completed.load(std::memory_order_relaxed)) {
            return true;
        }

        if (ev->GetTypeRewrite() == TResultEvent::EventType) {
            auto* result = ev->Get<TResultEvent>();
            Complete(std::move(result->Record));
            return true;
        }

        if (ev->GetTypeRewrite() == TEvents::TEvUndelivered::EventType) {
            TRecord record;
            SetErrorStatus(
                NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
                UndeliveryErrorMessage,
                record);
            Complete(std::move(record));
            return true;
        }

        return false;
    }

protected:
    void Complete(TRecord record)
    {
        if (Completed.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        Promise.SetValue(std::move(record));
    }

    void CompleteWithSessionBroken()
    {
        if (Completed.load(std::memory_order_relaxed)) {
            return;
        }
        TRecord record;
        SetSessionBrokenError(record);
        Complete(std::move(record));
    }

    std::atomic<bool> Completed{false};

private:
    TPromise<TRecord> Promise;
};

////////////////////////////////////////////////////////////////////////////////

// Like TPromiseReplyHandler, but copies the reply payload into the caller's
// GuardedSgList before completing the promise.
template <typename TResultEvent, typename TRecord>
class TReadReplyHandler: public TPromiseReplyHandler<TResultEvent, TRecord>
{
public:
    TReadReplyHandler(TPromise<TRecord> promise, TGuardedSgList data)
        : TPromiseReplyHandler<TResultEvent, TRecord>(std::move(promise))
        , Data(std::move(data))
    {}

    bool Receive(TAutoPtr<IEventHandle> ev) override
    {
        if (this->Completed.load(std::memory_order_relaxed)) {
            return true;
        }

        if (ev->GetTypeRewrite() == TResultEvent::EventType) {
            auto* result = ev->Get<TResultEvent>();
            if (auto guard = Data.Acquire()) {
                const auto& sglist = guard.Get();
                SgListCopy(CreateSgList(result->GetPayload()), sglist);
                this->Complete(std::move(result->Record));
            } else {
                TRecord errorResult;
                SetCantAcquireStatus(errorResult);
                this->Complete(std::move(errorResult));
            }
            return true;
        }

        if (ev->GetTypeRewrite() == TEvents::TEvUndelivered::EventType) {
            TRecord record;
            SetErrorStatus(
                NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
                UndeliveryErrorMessage,
                record);
            this->Complete(std::move(record));
            return true;
        }

        return false;
    }

private:
    TGuardedSgList Data;
};

////////////////////////////////////////////////////////////////////////////////

// Aggregates multiple TEvWritePersistentBuffersResult replies for one request.
class TWriteToManyReplyHandler: public IReplyHandler
{
public:
    using TResult = NKikimrBlobStorage::NDDisk::TEvWritePersistentBuffersResult;
    using TCallback = std::function<void(const TResult&)>;

    TWriteToManyReplyHandler(
        TCallback callback,
        TVector<NKikimrBlobStorage::NDDisk::TDDiskId> persistentBufferIds)
        : Callback(std::move(callback))
    {
        Y_ABORT_UNLESS(Callback);
        Y_ABORT_UNLESS(!persistentBufferIds.empty());
        CoordinatorId = persistentBufferIds[0];
        for (const auto& diskId: persistentBufferIds) {
            WaitingReplies.emplace(diskId);
        }
    }

    ~TWriteToManyReplyHandler() override
    {
        if (!Finished.load(std::memory_order_relaxed) &&
            !WaitingReplies.empty())
        {
            TVector<NKikimrBlobStorage::NDDisk::TDDiskId> remaining(
                WaitingReplies.begin(),
                WaitingReplies.end());
            auto response = MakeWritePersistentBuffersResult(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OUTDATED,
                SessionBrokenErrorMessage,
                remaining);
            Finish(response->Record);
        }
    }

    bool Receive(TAutoPtr<IEventHandle> ev) override
    {
        if (Finished.load(std::memory_order_relaxed)) {
            return true;
        }

        if (ev->GetTypeRewrite() ==
            NDDisk::TEvWritePersistentBuffersResult::EventType)
        {
            auto* result = ev->Get<NDDisk::TEvWritePersistentBuffersResult>();
            for (const auto& single: result->Record.GetResult()) {
                WaitingReplies.erase(single.GetPersistentBufferId());
            }
            Callback(result->Record);
            if (WaitingReplies.empty()) {
                Finished.store(true, std::memory_order_release);
                return true;
            }
            return false;
        }

        if (ev->GetTypeRewrite() == TEvents::TEvUndelivered::EventType) {
            // Mirror the actor path: report undelivery only for the
            // coordinator.
            auto response = MakeWritePersistentBuffersResult(
                NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
                UndeliveryErrorMessage,
                std::span<const NKikimrBlobStorage::NDDisk::TDDiskId>(
                    &CoordinatorId,
                    1));
            Finish(response->Record);
            return true;
        }

        return false;
    }

private:
    void Finish(const TResult& result)
    {
        if (Finished.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        Callback(result);
    }

    TCallback Callback;
    NKikimrBlobStorage::NDDisk::TDDiskId CoordinatorId;
    TSet<NKikimrBlobStorage::NDDisk::TDDiskId, TDDiskIdLess> WaitingReplies;
    std::atomic<bool> Finished{false};
};

////////////////////////////////////////////////////////////////////////////////

// Sends via IDirectSession with FlagTrackDelivery so a missing remote actor
// produces TEvUndelivered back to ReplyActorId (cookie preserved). The v2
// uring engine does not generate undelivered for events still queued when a
// session dies; that case is covered by TSessionReplyRouter / IReplyHandler
// destructors completing with OUTDATED.
template <typename TEvent>
[[nodiscard]] bool TrySendDirect(
    TActorSystem* actorSystem,
    const TSessionEntry& entry,
    TActorId recipient,
    ui64 cookie,
    std::unique_ptr<TEvent> event,
    NWilson::TTraceId traceId)
{
    auto handle = MakeHolder<IEventHandle>(
        recipient,
        entry.ReplyActorId,
        event.release(),
        IEventHandle::FlagTrackDelivery,
        cookie,
        /*forwardOnNondelivery=*/nullptr,
        std::move(traceId));

    // No replyCallback: the long-lived TSessionReplyRouter is already
    // registered for ReplyActorId and demultiplexes by cookie.
    if (entry.Session->Send(handle.Release())) {
        return true;
    }

    LOG_WARN_S(
        *actorSystem,
        NKikimrServices::NBS_PARTITION,
        "Direct send failed, node# " << recipient.NodeId() << " eventType# "
                                     << TEvent::EventType);
    return false;
}

template <typename TRequest>
void AttachPayload(TRequest& request, TRope rope, bool enableChecksums)
{
    if (enableChecksums) {
        request.AddPayloadThenChecksum(std::move(rope));
    } else {
        request.AddPayload(std::move(rope));
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TICDirectStorageTransport::TICDirectStorageTransport(
    TActorSystem* actorSystem,
    TActorId icStorageTransportActorId,
    std::shared_ptr<TDirectSessionRegistry> directSessionRegistry,
    bool enableChecksums)
    : TICStorageTransport(actorSystem, icStorageTransportActorId)
    , DirectSessionRegistry(std::move(directSessionRegistry))
    , EnableChecksums(enableChecksums)
{
    Y_ABORT_UNLESS(DirectSessionRegistry);
}

TSessionEntry TICDirectStorageTransport::GetSessionEntry(
    const THostConnection& connection) const
{
    return DirectSessionRegistry->Get(connection.DDiskId.NodeId);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<IStorageTransport::TEvWritePersistentBufferResult>
TICDirectStorageTransport::WriteToPBuffer(
    const THostConnection& connection,
    const NDDisk::TBlockSelector& selector,
    const ui64 lsn,
    const NDDisk::TWriteInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::PBuffer);

    auto entry = GetSessionEntry(connection);
    if (!entry) {
        return TICStorageTransport::WriteToPBuffer(
            connection,
            selector,
            lsn,
            instruction,
            data,
            span);
    }

    auto guard = data.Acquire();
    if (!guard) {
        auto promise = NewPromise<TEvWritePersistentBufferResult>();
        TEvWritePersistentBufferResult record;
        SetCantAcquireStatus(record);
        promise.SetValue(std::move(record));
        return promise.GetFuture();
    }

    auto request = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
        connection.Credentials,
        selector,
        lsn,
        instruction);

    const auto& sglist = guard.Get();
    TRope rope = TRope::Uninitialized(SgListGetSize(sglist));
    SgListCopy(sglist, CreateSgList(rope));
    AttachPayload(*request, std::move(rope), EnableChecksums);

    auto promise = NewPromise<TEvWritePersistentBufferResult>();
    auto future = promise.GetFuture();
    auto handler = MakeIntrusive<TPromiseReplyHandler<
        NDDisk::TEvWritePersistentBufferResult,
        TEvWritePersistentBufferResult>>(std::move(promise));
    const ui64 cookie = entry.Router->Add(handler);

    if (span) {
        span->Event("IDirectSession_Send");
    }

    if (!TrySendDirect(
            ActorSystem,
            entry,
            connection.GetServiceId(),
            cookie,
            std::move(request),
            span ? span->GetTraceId() : NWilson::TTraceId()))
    {
        entry.Router->Remove(cookie);
    }

    return future;
}

void TICDirectStorageTransport::WriteToManyPBuffers(
    const THostConnection& connection,
    const NDDisk::TBlockSelector& selector,
    const ui64 lsn,
    const NDDisk::TWriteInstruction instruction,
    TVector<NKikimrBlobStorage::NDDisk::TDDiskId> persistentBufferIds,
    TDuration replyTimeout,
    const TGuardedSgList& data,
    std::shared_ptr<NWilson::TSpan> span,
    TWriteToManyPBuffersCallback callback)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::PBuffer);

    auto entry = GetSessionEntry(connection);
    if (!entry) {
        TICStorageTransport::WriteToManyPBuffers(
            connection,
            selector,
            lsn,
            instruction,
            std::move(persistentBufferIds),
            replyTimeout,
            data,
            std::move(span),
            std::move(callback));
        return;
    }

    auto wrappedCallback = [callback = std::move(callback), span]   //
        (const TEvWriteToManyPersistentBuffersResult& result)
    {
        if (span) {
            span->Event("Reply received");
        }
        callback(result, span);
    };

    auto guard = data.Acquire();
    if (!guard) {
        auto errorResponse = MakeWritePersistentBuffersResult(
            NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN,
            CantAcquireDataErrorMessage,
            persistentBufferIds);
        wrappedCallback(errorResponse->Record);
        return;
    }

    auto request = std::make_unique<NDDisk::TEvWritePersistentBuffers>(
        connection.Credentials,
        selector,
        lsn,
        instruction,
        persistentBufferIds,
        replyTimeout.MicroSeconds());

    const auto& sglist = guard.Get();
    TRope rope = TRope::Uninitialized(SgListGetSize(sglist));
    SgListCopy(sglist, CreateSgList(rope));
    AttachPayload(*request, std::move(rope), EnableChecksums);

    auto handler = MakeIntrusive<TWriteToManyReplyHandler>(
        std::move(wrappedCallback),
        persistentBufferIds);
    const ui64 cookie = entry.Router->Add(handler);

    if (span) {
        span->Event("IDirectSession_Send");
    }

    if (!TrySendDirect(
            ActorSystem,
            entry,
            connection.GetServiceId(),
            cookie,
            std::move(request),
            span ? span->GetTraceId() : NWilson::TTraceId()))
    {
        entry.Router->Remove(cookie);
    }
}

TFuture<IStorageTransport::TEvWriteResult>
TICDirectStorageTransport::WriteToDDisk(
    const THostConnection& connection,
    const NDDisk::TBlockSelector& selector,
    const NDDisk::TWriteInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::DDisk);

    auto entry = GetSessionEntry(connection);
    if (!entry) {
        return TICStorageTransport::WriteToDDisk(
            connection,
            selector,
            instruction,
            data,
            span);
    }

    auto guard = data.Acquire();
    if (!guard) {
        auto promise = NewPromise<TEvWriteResult>();
        TEvWriteResult record;
        SetCantAcquireStatus(record);
        promise.SetValue(std::move(record));
        return promise.GetFuture();
    }

    auto request = std::make_unique<NDDisk::TEvWrite>(
        connection.Credentials,
        selector,
        instruction);

    const auto& sglist = guard.Get();
    TRope rope = TRope::Uninitialized(SgListGetSize(sglist));
    SgListCopy(sglist, CreateSgList(rope));
    AttachPayload(*request, std::move(rope), EnableChecksums);

    auto promise = NewPromise<TEvWriteResult>();
    auto future = promise.GetFuture();
    auto handler = MakeIntrusive<
        TPromiseReplyHandler<NDDisk::TEvWriteResult, TEvWriteResult>>(
        std::move(promise));
    const ui64 cookie = entry.Router->Add(handler);

    if (span) {
        span->Event("IDirectSession_Send");
    }

    if (!TrySendDirect(
            ActorSystem,
            entry,
            connection.GetServiceId(),
            cookie,
            std::move(request),
            span ? span->GetTraceId() : NWilson::TTraceId()))
    {
        entry.Router->Remove(cookie);
    }

    return future;
}

TFuture<IStorageTransport::TEvErasePersistentBufferResult>
TICDirectStorageTransport::BatchEraseFromPBuffer(
    const THostConnection& connection,
    TVector<ui64> lsns,
    NWilson::TSpan* span)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::PBuffer);

    auto entry = GetSessionEntry(connection);
    if (!entry) {
        return TICStorageTransport::BatchEraseFromPBuffer(
            connection,
            std::move(lsns),
            span);
    }

    auto request = std::make_unique<NDDisk::TEvBatchErasePersistentBuffer>(
        connection.Credentials);
    for (auto lsn: lsns) {
        request->AddErase(lsn, connection.Credentials.Generation);
    }

    auto promise = NewPromise<TEvErasePersistentBufferResult>();
    auto future = promise.GetFuture();
    auto handler = MakeIntrusive<TPromiseReplyHandler<
        NDDisk::TEvErasePersistentBufferResult,
        TEvErasePersistentBufferResult>>(std::move(promise));
    const ui64 cookie = entry.Router->Add(handler);

    if (span) {
        span->Event("IDirectSession_Send");
    }

    if (!TrySendDirect(
            ActorSystem,
            entry,
            connection.GetServiceId(),
            cookie,
            std::move(request),
            span ? span->GetTraceId() : NWilson::TTraceId()))
    {
        entry.Router->Remove(cookie);
    }

    return future;
}

TFuture<IStorageTransport::TEvErasePersistentBufferResult>
TICDirectStorageTransport::BarrierEraseFromPBuffer(
    const THostConnection& connection,
    ui64 lsn,
    NWilson::TSpan* span)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::PBuffer);

    auto entry = GetSessionEntry(connection);
    if (!entry) {
        return TICStorageTransport::BarrierEraseFromPBuffer(
            connection,
            lsn,
            span);
    }

    auto request = std::make_unique<NDDisk::TEvErasePersistentBuffer>(
        connection.Credentials,
        lsn);

    auto promise = NewPromise<TEvErasePersistentBufferResult>();
    auto future = promise.GetFuture();
    auto handler = MakeIntrusive<TPromiseReplyHandler<
        NDDisk::TEvErasePersistentBufferResult,
        TEvErasePersistentBufferResult>>(std::move(promise));
    const ui64 cookie = entry.Router->Add(handler);

    if (span) {
        span->Event("IDirectSession_Send");
    }

    if (!TrySendDirect(
            ActorSystem,
            entry,
            connection.GetServiceId(),
            cookie,
            std::move(request),
            span ? span->GetTraceId() : NWilson::TTraceId()))
    {
        entry.Router->Remove(cookie);
    }

    return future;
}

TFuture<IStorageTransport::TEvReadPersistentBufferResult>
TICDirectStorageTransport::ReadFromPBuffer(
    const THostConnection& connection,
    const NDDisk::TBlockSelector& selector,
    const ui64 lsn,
    const NDDisk::TReadInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::PBuffer);

    auto entry = GetSessionEntry(connection);
    if (!entry) {
        return TICStorageTransport::ReadFromPBuffer(
            connection,
            selector,
            lsn,
            instruction,
            data,
            span);
    }

    auto request = std::make_unique<NDDisk::TEvReadPersistentBuffer>(
        connection.Credentials,
        selector,
        lsn,
        connection.Credentials.Generation,
        instruction);

    auto promise = NewPromise<TEvReadPersistentBufferResult>();
    auto future = promise.GetFuture();
    auto handler = MakeIntrusive<TReadReplyHandler<
        NDDisk::TEvReadPersistentBufferResult,
        TEvReadPersistentBufferResult>>(std::move(promise), data);
    const ui64 cookie = entry.Router->Add(handler);

    if (span) {
        span->Event("IDirectSession_Send");
    }

    if (!TrySendDirect(
            ActorSystem,
            entry,
            connection.GetServiceId(),
            cookie,
            std::move(request),
            span ? span->GetTraceId() : NWilson::TTraceId()))
    {
        entry.Router->Remove(cookie);
    }

    return future;
}

TFuture<IStorageTransport::TEvReadResult>
TICDirectStorageTransport::ReadFromDDisk(
    const THostConnection& connection,
    const NDDisk::TBlockSelector& selector,
    const NDDisk::TReadInstruction instruction,
    const TGuardedSgList& data,
    NWilson::TSpan* span)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::DDisk);

    auto entry = GetSessionEntry(connection);
    if (!entry) {
        return TICStorageTransport::ReadFromDDisk(
            connection,
            selector,
            instruction,
            data,
            span);
    }

    auto request = std::make_unique<NDDisk::TEvRead>(
        connection.Credentials,
        selector,
        instruction);

    auto promise = NewPromise<TEvReadResult>();
    auto future = promise.GetFuture();
    auto handler =
        MakeIntrusive<TReadReplyHandler<NDDisk::TEvReadResult, TEvReadResult>>(
            std::move(promise),
            data);
    const ui64 cookie = entry.Router->Add(handler);

    if (span) {
        span->Event("IDirectSession_Send");
    }

    if (!TrySendDirect(
            ActorSystem,
            entry,
            connection.GetServiceId(),
            cookie,
            std::move(request),
            span ? span->GetTraceId() : NWilson::TTraceId()))
    {
        entry.Router->Remove(cookie);
    }

    return future;
}

TFuture<IStorageTransport::TEvSyncResult>
TICDirectStorageTransport::SyncWithPBuffer(
    const THostConnection& pbufferConnection,
    const THostConnection& ddiskConnection,
    TVector<NKikimr::NDDisk::TBlockSelector> selectors,
    TVector<ui64> lsns,
    NWilson::TSpan* span)
{
    Y_ABORT_UNLESS(
        pbufferConnection.ConnectionType == EConnectionType::PBuffer);
    Y_ABORT_UNLESS(ddiskConnection.ConnectionType == EConnectionType::DDisk);
    Y_ABORT_UNLESS(pbufferConnection.Credentials.DDiskInstanceGuid.has_value());

    auto entry = GetSessionEntry(ddiskConnection);
    if (!entry) {
        return TICStorageTransport::SyncWithPBuffer(
            pbufferConnection,
            ddiskConnection,
            std::move(selectors),
            std::move(lsns),
            span);
    }

    auto request =
        std::make_unique<NDDisk::TEvSync>(ddiskConnection.Credentials);
    const auto pBufferId = std::make_tuple(
        pbufferConnection.DDiskId.NodeId,
        pbufferConnection.DDiskId.PDiskId,
        pbufferConnection.DDiskId.DDiskSlotId);

    Y_ABORT_UNLESS(selectors.size() == lsns.size());
    for (size_t i = 0; i < selectors.size(); ++i) {
        request->AddSegmentFromPB(
            pBufferId,
            *pbufferConnection.Credentials.DDiskInstanceGuid,
            selectors[i],
            lsns[i],
            ddiskConnection.Credentials.Generation);
    }

    auto promise = NewPromise<TEvSyncResult>();
    auto future = promise.GetFuture();
    auto handler = MakeIntrusive<
        TPromiseReplyHandler<NDDisk::TEvSyncResult, TEvSyncResult>>(
        std::move(promise));
    const ui64 cookie = entry.Router->Add(handler);

    if (span) {
        span->Event("IDirectSession_Send");
    }

    if (!TrySendDirect(
            ActorSystem,
            entry,
            ddiskConnection.GetServiceId(),
            cookie,
            std::move(request),
            span ? span->GetTraceId() : NWilson::TTraceId()))
    {
        entry.Router->Remove(cookie);
    }

    return future;
}

TFuture<IStorageTransport::TEvListPersistentBufferResult>
TICDirectStorageTransport::ListPBufferEntries(const THostConnection& connection)
{
    Y_ABORT_UNLESS(connection.ConnectionType == EConnectionType::PBuffer);

    auto entry = GetSessionEntry(connection);
    if (!entry) {
        return TICStorageTransport::ListPBufferEntries(connection);
    }

    auto request = std::make_unique<NDDisk::TEvListPersistentBuffer>(
        connection.Credentials);

    auto promise = NewPromise<TEvListPersistentBufferResult>();
    auto future = promise.GetFuture();
    auto handler = MakeIntrusive<TPromiseReplyHandler<
        NDDisk::TEvListPersistentBufferResult,
        TEvListPersistentBufferResult>>(std::move(promise));
    const ui64 cookie = entry.Router->Add(handler);

    if (!TrySendDirect(
            ActorSystem,
            entry,
            connection.GetServiceId(),
            cookie,
            std::move(request),
            NWilson::TTraceId()))
    {
        entry.Router->Remove(cookie);
    }

    return future;
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IStorageTransport> CreateDirectStorageTransport(
    TActorSystem* actorSystem,
    const TDiskDescription& diskDescription,
    ui32 dbgIndex,
    bool enableChecksums)
{
    auto registry = std::make_shared<TDirectSessionRegistry>();
    auto actorId = CreateTransportActor(
        diskDescription,
        dbgIndex,
        enableChecksums,
        registry);
    return std::make_unique<TICDirectStorageTransport>(
        actorSystem,
        actorId,
        std::move(registry),
        enableChecksums);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
