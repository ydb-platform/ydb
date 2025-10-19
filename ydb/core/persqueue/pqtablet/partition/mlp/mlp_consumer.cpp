#include "mlp_consumer.h"
#include "mlp_message_enricher.h"
#include "mlp_storage.h"

#include <ydb/core/persqueue/common/key.h>
#include <ydb/core/persqueue/events/internal.h>

namespace NKikimr::NPQ::NMLP {

namespace {

void ReplyError(const TActorIdentity selfActorId, const TActorId& sender, ui64 cookie, TString&& error) {
    selfActorId.Send(sender, new TEvPersQueue::TEvMLPErrorResponse(Ydb::StatusIds::INTERNAL_ERROR, std::move(error)), 0, cookie);
}

template<typename T>
void ReplyErrorAll(const TActorIdentity selfActorId, std::deque<T>& queue) {
    for (auto& ev : queue) {
        ReplyError(selfActorId, ev->Sender, ev->Cookie, "Actor destroyed");
    }
    queue.clear();
}

template<typename T>
void RollbackAll(const TActorIdentity selfActorId, std::deque<T>& queue) {
    for (auto& ev : queue) {
        ReplyError(selfActorId, ev.Sender, ev.Cookie, "Rollback");
    }
    queue.clear();
}

template<typename R, typename T>
void ReplyOk(const TActorIdentity selfActorId, std::deque<T>& queue) {
    for (auto& ev : queue) {
        selfActorId.Send(ev.Sender, new R(), 0, ev.Cookie);
    }
    queue.clear();
}

}



TString MakeSnapshotKey(ui32 partitionId, ui32 consumerId) {
    return TStringBuilder() << TKeyPrefix(TKeyPrefix::EType::TypeConsumerData, TPartitionId(partitionId)).ToString()
        << "_" << Sprintf("%.10" PRIu32, consumerId);
}

TConsumerActor::TConsumerActor(ui64 tabletId, const TActorId& tabletActorId, ui32 partitionId, const TActorId& partitionActorId, const NKikimrPQ::TPQTabletConfig_TConsumer& config)
    : TBaseTabletActor(tabletId, tabletActorId, NKikimrServices::EServiceKikimr::PQ_MLP_CONSUMER)
    , PartitionId(partitionId)
    , PartitionActorId(partitionActorId)
    , Config(config)
    , Storage(std::make_unique<TStorage>(CreateDefaultTimeProvider())) {
}

void TConsumerActor::Bootstrap() {
    Become(&TConsumerActor::StateInit);

    // TODO Update consumer config
    Storage->SetKeepMessageOrder(Config.GetKeepMessageOrder());
    Storage->SetMaxMessageReceiveCount(Config.GetMaxMessageReceiveCount());

    auto key = MakeSnapshotKey(PartitionId, Config.GetId());
    LOG_D("Reading snapshot " << key << " from " << TabletActorId.ToString());
    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    request->Record.AddCmdRead()->SetKey(key);
    Send(TabletActorId, std::move(request));

    Schedule(WakeupInterval, new TEvents::TEvWakeup());
}

void TConsumerActor::PassAway() {
    LOG_D("PassAway");

    RollbackAll(SelfId(), PendingReadQueue);
    RollbackAll(SelfId(), PendingCommitQueue);
    RollbackAll(SelfId(), PendingUnlockQueue);
    RollbackAll(SelfId(), PendingChangeMessageDeadlineQueue);

    ReplyErrorAll(SelfId(), ReadRequestsQueue);
    ReplyErrorAll(SelfId(), CommitRequestsQueue);
    ReplyErrorAll(SelfId(), UnlockRequestsQueue);
    ReplyErrorAll(SelfId(), ChangeMessageDeadlineRequestsQueue);

    TBase::PassAway();
}

TString TConsumerActor::BuildLogPrefix() const {
    return TStringBuilder() << "[" << PartitionId << "][MLP][" << Config.GetName() << "] ";
}

void TConsumerActor::Queue(TEvPersQueue::TEvMLPReadRequest::TPtr& ev) {
    LOG_D("Queue TEvPersQueue::TEvMLPReadRequest " << ev->Get()->Record.ShortDebugString());
    ReadRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Queue(TEvPersQueue::TEvMLPCommitRequest::TPtr& ev) {
    LOG_D("Queue TEvPersQueue::TEvMLPCommitRequest " << ev->Get()->Record.ShortDebugString());
    CommitRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Queue(TEvPersQueue::TEvMLPUnlockRequest::TPtr& ev) {
    LOG_D("Queue TEvPersQueue::TEvMLPUnlockRequest " << ev->Get()->Record.ShortDebugString());
    UnlockRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Queue(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    LOG_D("Queue TEvPersQueue::TEvMLPChangeMessageDeadlineRequest " << ev->Get()->Record.ShortDebugString());
    ChangeMessageDeadlineRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Handle(TEvPersQueue::TEvMLPReadRequest::TPtr& ev) {
    Queue(ev);
    ProcessEventQueue();
}

void TConsumerActor::Handle(TEvPersQueue::TEvMLPCommitRequest::TPtr& ev) {
    Queue(ev);
    ProcessEventQueue();
}

void TConsumerActor::Handle(TEvPersQueue::TEvMLPUnlockRequest::TPtr& ev) {
    Queue(ev);
    ProcessEventQueue();
}

void TConsumerActor::Handle(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    Queue(ev);
    ProcessEventQueue();
}

void TConsumerActor::HandleOnInit(TEvKeyValue::TEvResponse::TPtr& ev) {
    LOG_D("HandleOnInit TEvKeyValue::TEvResponse");
    auto& record = ev->Get()->Record;

    if (record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        return Restart(TStringBuilder() << "Received KV error on initialization: " << record.GetStatus());
    }
    if (record.ReadResultSize() != 1) {
        return Restart(TStringBuilder() << "Unexpected KV response on initialization: " << record.ReadResultSize());
    }

    auto& readResult = record.GetReadResult(0);

    switch(readResult.GetStatus()) {
        case NKikimrProto::OK: {
            AFL_ENSURE(readResult.HasValue() && readResult.GetValue().size());

            NKikimrPQ::TMLPStorageSnapshot snapshot;
            if (!snapshot.ParseFromString(readResult.GetValue())) {
                return Restart(TStringBuilder() << "Parse snapshot error");
            }

            if (Config.GetId() != snapshot.GetConfiguration().GetConsumerId()) {
                return Restart(TStringBuilder() << "Snapshot consumer id mismatch: " << Config.GetId() << " vs " << snapshot.GetConfiguration().GetConsumerId());
            }

            if (Config.GetGeneration() == snapshot.GetConfiguration().GetGeneration()) {
                Storage->InitializeFromSnapshot(snapshot);
            } else {
                LOG_W("Received snapshot from old consumer generation: " << Config.GetGeneration() << " vs " << snapshot.GetConfiguration().GetGeneration());
            }

            break;
        }
        case NKikimrProto::NODATA: {
            LOG_D("Initializing new consumer");
            break;
        }
        default:
            return Restart(TStringBuilder() << "Received KV response error on initialization: " << readResult.GetStatus());
    }

    if (!FetchMessagesIfNeeded()) {
        LOG_D("Initialized");
        Become(&TConsumerActor::StateWork);
        ProcessEventQueue();
    }
}

STFUNC(TConsumerActor::StateInit) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvMLPReadRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPCommitRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPUnlockRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest, Queue);
        hFunc(TEvKeyValue::TEvResponse, HandleOnInit);
        hFunc(TEvPQ::TEvProxyResponse, HandleOnInit);
        hFunc(TEvPQ::TEvError, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
        default:
            LOG_E("Unexpected " << EventStr("StateInit", ev));
    }
}

void TConsumerActor::HandleOnWrite(TEvKeyValue::TEvResponse::TPtr& ev) {
    LOG_D("HandleOnWrite TEvKeyValue::TEvResponse");

    auto& record = ev->Get()->Record;

    if (record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        return Restart(TStringBuilder() << "Received KV error on write: " << record.GetStatus()
            << " " << record.GetErrorReason());
    }
    if (record.WriteResultSize() != 1) {
        return Restart(TStringBuilder() << "Unexpected KV response on write: " << record.WriteResultSize());
    }

    auto& writeResult = record.GetWriteResult(0);
    if (writeResult.GetStatus() != NKikimrProto::OK) {
        return Restart(TStringBuilder() << "Received KV response error on write: " << writeResult.GetStatus());
    }

    LOG_D("Snapshot persisted");
    Become(&TConsumerActor::StateWork);

    if (!PendingReadQueue.empty()) {
        auto msgs = std::exchange(PendingReadQueue, {});
        RegisterWithSameMailbox(new TMessageEnricherActor(PartitionId, PartitionActorId, Config.GetName(), std::move(msgs))); // TODO excahnge
    }
    ReplyOk<TEvPersQueue::TEvMLPCommitResponse>(SelfId(), PendingCommitQueue);
    ReplyOk<TEvPersQueue::TEvMLPUnlockResponse>(SelfId(), PendingUnlockQueue);
    ReplyOk<TEvPersQueue::TEvMLPChangeMessageDeadlineResponse>(SelfId(), PendingChangeMessageDeadlineQueue);

    ProcessEventQueue();
    FetchMessagesIfNeeded();

    // TODO commit offset
}

void TConsumerActor::Commit() {

}

STFUNC(TConsumerActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvMLPReadRequest, Handle);
        hFunc(TEvPersQueue::TEvMLPCommitRequest, Handle);
        hFunc(TEvPersQueue::TEvMLPUnlockRequest, Handle);
        hFunc(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest, Handle);
        hFunc(TEvPQ::TEvProxyResponse, Handle);
        hFunc(TEvPQ::TEvError, Handle);
        hFunc(TEvents::TEvWakeup, HandleOnWork);
        sFunc(TEvents::TEvPoison, PassAway);
        default:
            LOG_E("Unexpected " << EventStr("StateWork", ev));
    }
}

STFUNC(TConsumerActor::StateWrite) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPersQueue::TEvMLPReadRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPCommitRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPUnlockRequest, Queue);
        hFunc(TEvPersQueue::TEvMLPChangeMessageDeadlineRequest, Queue);
        hFunc(TEvKeyValue::TEvResponse, HandleOnWrite);
        hFunc(TEvPQ::TEvProxyResponse, Handle);
        hFunc(TEvPQ::TEvError, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
        default:
            LOG_E("Unexpected " << EventStr("StateWrite", ev));
    }
}

void TConsumerActor::Restart(TString&& error) {
    LOG_E(error);

    Send(PartitionActorId, new TEvPQ::TEvMLPRestartActor());

    ReplyErrorAll(SelfId(), ReadRequestsQueue);
    ReplyErrorAll(SelfId(), CommitRequestsQueue);
    ReplyErrorAll(SelfId(), UnlockRequestsQueue);
    ReplyErrorAll(SelfId(), ChangeMessageDeadlineRequestsQueue);

    PassAway();
}

void TConsumerActor::ProcessEventQueue() {
    LOG_D("ProcessEventQueue");

    for (auto& ev : CommitRequestsQueue) {
        for (auto offset : ev->Get()->Record.GetOffset()) {
            Storage->Commit(offset);
        }

        PendingCommitQueue.emplace_back(ev->Sender, ev->Cookie);
    }
    CommitRequestsQueue.clear();

    for (auto& ev : UnlockRequestsQueue) {
        for (auto offset : ev->Get()->Record.GetOffset()) {
            Storage->Unlock(offset);
        }

        PendingUnlockQueue.emplace_back(ev->Sender, ev->Cookie);
    }
    UnlockRequestsQueue.clear();

    for (auto& ev : ChangeMessageDeadlineRequestsQueue) {
        auto deadlineTimestamp = ev->Get()->GetDeadlineTimestamp();
        for (auto offset : ev->Get()->Record.GetOffset()) {
            Storage->ChangeMessageDeadline(offset, deadlineTimestamp);
        }

        PendingChangeMessageDeadlineQueue.emplace_back(ev->Sender, ev->Cookie);
    }
    ChangeMessageDeadlineRequestsQueue.clear();

    if (!ReadRequestsQueue.empty()) {
        Storage->ProccessDeadlines();
        LOG_T("AfterDeadlinesDump: " << Storage->DebugString());
    }

    ui64 fromOffset = 0;
    while (!ReadRequestsQueue.empty()) {
        auto& ev = ReadRequestsQueue.front();

        size_t count = ev->Get()->GetMaxNumberOfMessages();
        auto visibilityTimeout = ev->Get()->GetVisibilityTimeout();
        if (visibilityTimeout == TDuration::Zero()) {
            visibilityTimeout = TDuration::Seconds(Config.GetDefaultVisibilityTimeoutSeconds());
        }
        const auto deadline =  visibilityTimeout.ToDeadLine();

        std::deque<TMessageId> messages;
        //messages.reserve(count);
        for (; count; --count) {
            auto result = Storage->Next(deadline, fromOffset);
            if (!result) {
                break;
            }

            messages.push_back(result->Message);
            fromOffset = result->FromOffset;
        }

        if (messages.empty() && ev->Get()->GetWaitTime() == TDuration::Zero()) {
            // Optimization: do not need to upload the message body.
            LOG_D("Reply empty result: sender=" << ev->Sender.ToString() << " cookie=" << ev->Cookie);
            Send(ev->Sender, new TEvPersQueue::TEvMLPReadResponse(), 0, ev->Cookie);
            ReadRequestsQueue.pop_front();
            continue;
        } else if (messages.empty()) {
            break; // TODO перекладываеть очереди, 0, reply.Cookie
        }

        PendingReadQueue.emplace_back(ev->Sender, ev->Cookie, std::move(messages));
        ReadRequestsQueue.pop_front();
    }

    LOG_T("AfterQueueDump: " << Storage->DebugString());

    if (PendingCommitQueue.empty() && PendingUnlockQueue.empty() &&
        PendingChangeMessageDeadlineQueue.empty() && PendingReadQueue.empty()) {
        LOG_D("Batch is empty");
        return;
    }

    PersistSnapshot();
}

void TConsumerActor::PersistSnapshot() {
    LOG_D("PersistSnapshot");

    Become(&TConsumerActor::StateWrite);

    Storage->Compact();

    NKikimrPQ::TMLPStorageSnapshot snapshot;

    auto* config = snapshot.MutableConfiguration();
    config->SetConsumerId(Config.GetId());
    config->SetGeneration(Config.GetGeneration());
    Storage->CreateSnapshot(snapshot);

    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    auto* write = request->Record.AddCmdWrite();
    write->SetKey(MakeSnapshotKey(PartitionId, Config.GetId()));
    write->SetValue(snapshot.SerializeAsString());

    Send(TabletActorId, std::move(request));
}

bool TConsumerActor::FetchMessagesIfNeeded() {
    if (FetchInProgress) {
        return false;
    }

    auto& metrics = Storage->GetMetrics();
    if (metrics.InflyMessageCount >= TStorage::MaxMessages) {
        LOG_D("Skip fetch: infly limit exceeded");
        return false;
    }
    if (metrics.InflyMessageCount >= TStorage::MinMessages && metrics.UnprocessedMessageCount >= metrics.LockedMessageCount * 2) {
        LOG_D("Skip fetch: there are enough messages. InflyMessageCount=" << metrics.InflyMessageCount
            << ", UnprocessedMessageCount=" << metrics.UnprocessedMessageCount
            << ", LockedMessageCount=" << metrics.LockedMessageCount);
        return false;
    }

    FetchInProgress = true;

    auto maxMessages = std::min(metrics.LockedMessageCount * 2 - metrics.UnprocessedMessageCount,
        TStorage::MaxMessages - metrics.InflyMessageCount);
    if (metrics.InflyMessageCount < TStorage::MinMessages) {
        maxMessages = std::max(maxMessages, TStorage::MinMessages - metrics.InflyMessageCount);
    }
    LOG_D("Fetching " << maxMessages << " messages from offset " << Storage->GetLastOffset() << " from " << PartitionActorId);
    Send(PartitionActorId, MakeEvRead(SelfId(), Config.GetName(), Storage->GetLastOffset(), maxMessages, ++FetchCookie));

    return true;
}

void TConsumerActor::HandleOnInit(TEvPQ::TEvProxyResponse::TPtr& ev) {
    LOG_D("Initialized");
    Become(&TConsumerActor::StateWork);
    Handle(ev);
}

void TConsumerActor::Handle(TEvPQ::TEvProxyResponse::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvProxyResponse");
    if (FetchCookie != ev->Cookie) {
        LOG_D("Cookie mismatch: " << FetchCookie << " != " << ev->Cookie);
        //return;
    }

    FetchInProgress = false;

    if (!IsSucess(ev)) {
        LOG_W("Fetch messages failed: " << ev->Get()->Response->DebugString());
        return;
    }

    auto& response = ev->Get()->Response;
    if (response->GetPartitionResponse().HasCmdReadResult()) {
        auto lastOffset = Storage->GetLastOffset();
        for (auto& result : response->GetPartitionResponse().GetCmdReadResult().GetResult()) {
            if (lastOffset > result.GetOffset()) {
                continue;
            }

            if (result.GetPartNo() > 0) {
                continue;
            }

            Storage->AddMessage(result.GetOffset(), result.HasSourceId() && !result.GetSourceId().empty(), Hash(result.GetSourceId()));
        }
    }

    ProcessEventQueue();
}

void TConsumerActor::Handle(TEvPQ::TEvError::TPtr& ev) {
    Restart(TStringBuilder() << "Received error: " << ev->Get()->Error);
}

void TConsumerActor::HandleOnWork(TEvents::TEvWakeup::TPtr&) {
    FetchMessagesIfNeeded();
    ProcessEventQueue();
    Schedule(WakeupInterval, new TEvents::TEvWakeup());
}

void TConsumerActor::Handle(TEvents::TEvWakeup::TPtr&) {
    LOG_D("Handle TEvents::TEvWakeup");
    Schedule(WakeupInterval, new TEvents::TEvWakeup());
}

NActors::IActor* CreateConsumerActor(
    ui64 tabletId,
    const NActors::TActorId& tabletActorId,
    ui32 partitionId,
    const NActors::TActorId& partitionActorId,
    const NKikimrPQ::TPQTabletConfig_TConsumer& config) {
    return new TConsumerActor(tabletId, tabletActorId, partitionId, partitionActorId, config);
}

}
