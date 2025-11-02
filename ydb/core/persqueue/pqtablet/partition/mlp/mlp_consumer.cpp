#include "mlp_consumer.h"
#include "mlp_message_enricher.h"
#include "mlp_storage.h"

#include <ydb/core/persqueue/common/key.h>

namespace NKikimr::NPQ::NMLP {

namespace {

void ReplyError(const TActorIdentity selfActorId, const TActorId& sender, ui64 cookie, TString&& error) {
    selfActorId.Send(sender, new TEvPQ::TEvMLPErrorResponse(Ydb::StatusIds::UNAVAILABLE, std::move(error)), 0, cookie);
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

TString MakeSnapshotKey(ui32 partitionId, const TString& consumerName) {
    TKeyPrefix ikey(TKeyPrefix::EType::TypeMLPConsumerData, TPartitionId(partitionId), TKeyPrefix::EMark::MarkMLPSnapshot);
    ikey.Append(consumerName.c_str(), consumerName.size());

    return ikey.ToString();
}

static constexpr char WALSeparator = '/';

TString MakeWALKey(ui32 partitionId, const TString& consumerName, ui32 index) {
    TKeyPrefix ikey(TKeyPrefix::EType::TypeMLPConsumerData, TPartitionId(partitionId), TKeyPrefix::EMark::MarkMLPWAL);
    ikey.Append(consumerName.c_str(), consumerName.size());
    ikey.Append(WALSeparator);
    ikey.Append(Sprintf("%.5" PRIu32, index).data(), 5);

    return ikey.ToString();
}

TString MinWALKey(ui32 partitionId, const TString& consumerName) {
    return MakeWALKey(partitionId, consumerName, 0);
}

TString MaxWALKey(ui32 partitionId, const TString& consumerName) {
    return MakeWALKey(partitionId, consumerName, 99999);
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

    // TODO MLP Update consumer config
    Storage->SetKeepMessageOrder(Config.GetKeepMessageOrder());
    Storage->SetMaxMessageReceiveCount(Config.GetMaxMessageReceiveCount());

    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    request->Record.AddCmdRead()->SetKey(MakeSnapshotKey(PartitionId, Config.GetName()));
    auto* readWAL = request->Record.AddCmdReadRange();
    readWAL->MutableRange()->SetFrom(MinWALKey(PartitionId, Config.GetName()));
    readWAL->MutableRange()->SetIncludeFrom(true);
    readWAL->MutableRange()->SetTo(MaxWALKey(PartitionId, Config.GetName()));
    readWAL->MutableRange()->SetIncludeTo(true);
    readWAL->SetIncludeData(true);

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

void TConsumerActor::Queue(TEvPQ::TEvMLPReadRequest::TPtr& ev) {
    LOG_D("Queue TEvPQ::TEvMLPReadRequest " << ev->Get()->Record.ShortDebugString());
    ReadRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Queue(TEvPQ::TEvMLPCommitRequest::TPtr& ev) {
    LOG_D("Queue TEvPQ::TEvMLPCommitRequest " << ev->Get()->Record.ShortDebugString());
    CommitRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Queue(TEvPQ::TEvMLPUnlockRequest::TPtr& ev) {
    LOG_D("Queue TEvPQ::TEvMLPUnlockRequest " << ev->Get()->Record.ShortDebugString());
    UnlockRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Queue(TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    LOG_D("Queue TEvPQ::TEvMLPChangeMessageDeadlineRequest " << ev->Get()->Record.ShortDebugString());
    ChangeMessageDeadlineRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Handle(TEvPQ::TEvMLPReadRequest::TPtr& ev) {
    Queue(ev);
    ProcessEventQueue();
}

void TConsumerActor::Handle(TEvPQ::TEvMLPCommitRequest::TPtr& ev) {
    Queue(ev);
    ProcessEventQueue();
}

void TConsumerActor::Handle(TEvPQ::TEvMLPUnlockRequest::TPtr& ev) {
    Queue(ev);
    ProcessEventQueue();
}

void TConsumerActor::Handle(TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
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

            if (Config.GetName() != snapshot.GetConfiguration().GetConsumerName()) {
                return Restart(TStringBuilder() << "Snapshot consumer id mismatch: " << Config.GetName() << " vs " << snapshot.GetConfiguration().GetConsumerName());
            }

            if (Config.GetGeneration() == snapshot.GetConfiguration().GetGeneration()) {
                Storage->Initialize(snapshot);
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

    if (record.ReadRangeResultSize() != 1) {
        return Restart(TStringBuilder() << "Unexpected KV response on initialization: " << record.ReadResultSize());
    }

    auto& walResult = record.GetReadRangeResult(0);

    switch(walResult.GetStatus()) {
        case NKikimrProto::OK: {

            for (auto w : walResult.GetPair()) {
                NKikimrPQ::TMLPStorageWAL wal;
                if (!wal.ParseFromString(w.GetValue())) {
                    return Restart(TStringBuilder() << "Parse wal error");
                }

                if (Config.GetGeneration() == wal.GetGeneration()) {
                    Storage->ApplyWAL(wal);
                } else {
                    LOG_W("Received snapshot from old consumer generation: " << Config.GetGeneration() << " vs " << wal.GetGeneration());
                }
            }

            NextWALIndex = walResult.GetPair().size();

            break;
        }
        case NKikimrProto::NODATA: {
            LOG_D("Initializing new consumer");
            break;
        }
        default:
            return Restart(TStringBuilder() << "Received KV response error on initialization: " << walResult.GetStatus());
    }

    CommitIfNeeded();

    if (!FetchMessagesIfNeeded()) {
        LOG_D("Initialized");
        Become(&TConsumerActor::StateWork);
        ProcessEventQueue();
    }
}

STFUNC(TConsumerActor::StateInit) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPQ::TEvMLPReadRequest, Queue);
        hFunc(TEvPQ::TEvMLPCommitRequest, Queue);
        hFunc(TEvPQ::TEvMLPUnlockRequest, Queue);
        hFunc(TEvPQ::TEvMLPChangeMessageDeadlineRequest, Queue);
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

    CommitIfNeeded();

    if (!PendingReadQueue.empty()) {
        auto msgs = std::exchange(PendingReadQueue, {});
        RegisterWithSameMailbox(new TMessageEnricherActor(TabletActorId, PartitionId, Config.GetName(), std::move(msgs)));
    }
    ReplyOk<TEvPQ::TEvMLPCommitResponse>(SelfId(), PendingCommitQueue);
    ReplyOk<TEvPQ::TEvMLPUnlockResponse>(SelfId(), PendingUnlockQueue);
    ReplyOk<TEvPQ::TEvMLPChangeMessageDeadlineResponse>(SelfId(), PendingChangeMessageDeadlineQueue);

    ProcessEventQueue();
    FetchMessagesIfNeeded();
}

void TConsumerActor::CommitIfNeeded() {
    auto offset = Storage->GetFirstUncommittedOffset();
    LOG_D("Try commit offset: " << offset << " vs " << LastCommittedOffset);
    if (LastCommittedOffset != offset) {
        Send(PartitionActorId, MakeEvCommit(Config, offset));
        LastCommittedOffset = offset;
    }
}

STFUNC(TConsumerActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPQ::TEvMLPReadRequest, Handle);
        hFunc(TEvPQ::TEvMLPCommitRequest, Handle);
        hFunc(TEvPQ::TEvMLPUnlockRequest, Handle);
        hFunc(TEvPQ::TEvMLPChangeMessageDeadlineRequest, Handle);
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
        hFunc(TEvPQ::TEvMLPReadRequest, Queue);
        hFunc(TEvPQ::TEvMLPCommitRequest, Queue);
        hFunc(TEvPQ::TEvMLPUnlockRequest, Queue);
        hFunc(TEvPQ::TEvMLPChangeMessageDeadlineRequest, Queue);
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

    Send(TabletActorId, new TEvents::TEvPoison());

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

    auto now = TInstant::Now();

    ui64 fromOffset = 0;
    std::deque<TEvPQ::TEvMLPReadRequest::TPtr> readRequestsQueue;
    for (auto& ev : ReadRequestsQueue) {
        size_t count = ev->Get()->GetMaxNumberOfMessages();
        auto visibilityDeadline = ev->Get()->GetVisibilityDeadline();
        if (visibilityDeadline == TInstant::Zero()) {
            visibilityDeadline = TDuration::Seconds(Config.GetDefaultVisibilityTimeoutSeconds()).ToDeadLine(now);
        }

        std::deque<TMessageId> messages;
        for (; count; --count) {
            auto result = Storage->Next(visibilityDeadline, fromOffset);
            if (!result) {
                break;
            }

            messages.push_back(result->Message);
            fromOffset = result->FromOffset;
        }

        if (messages.empty() && ev->Get()->GetWaitDeadline() <= now) {
            // Optimization: do not need to upload the message body.
            LOG_D("Reply empty result: sender=" << ev->Sender.ToString() << " cookie=" << ev->Cookie);
            Send(ev->Sender, new TEvPQ::TEvMLPReadResponse(), 0, ev->Cookie);
            continue;
        } else if (messages.empty()) {
            readRequestsQueue.push_back(std::move(ev));
            continue;
        }

        PendingReadQueue.emplace_back(ev->Sender, ev->Cookie, std::move(messages));
    }

    ReadRequestsQueue = std::move(readRequestsQueue);

    LOG_T("AfterQueueDump: " << Storage->DebugString());

    if (PendingCommitQueue.empty() && PendingUnlockQueue.empty() &&
        PendingChangeMessageDeadlineQueue.empty() && PendingReadQueue.empty()) {
        LOG_D("Batch is empty");
        return;
    }

    Persist();
}

void TConsumerActor::Persist() {
    LOG_D("Persist");

    Become(&TConsumerActor::StateWrite);

    auto batch = Storage->GetBatch();

    auto request = std::make_unique<TEvKeyValue::TEvRequest>();

    auto requireSnapshot = batch.GetRequiredSnapshot()
        || NextWALIndex == 100
        || batch.AffectedMessageCount() > Storage->GetMessageCount() / 2 /* Affected message count is very big  */
        || Storage->GetMessageCount() < 32 /* Snapshot is small. WAL not required */;

    if (requireSnapshot) {
        // TODO MLP Move StartOffset
        Storage->Compact();

        NKikimrPQ::TMLPStorageSnapshot snapshot;

        auto* config = snapshot.MutableConfiguration();
        config->SetConsumerName(Config.GetName());
        config->SetGeneration(Config.GetGeneration());
        Storage->SerializeTo(snapshot);

        auto* write = request->Record.AddCmdWrite();
        write->SetKey(MakeSnapshotKey(PartitionId, Config.GetName()));
        write->SetValue(snapshot.SerializeAsString());
        if (write->GetValue().size() < 1000) {
            write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
        }

        auto* del = request->Record.AddCmdDeleteRange();
        del->MutableRange()->SetFrom(MinWALKey(PartitionId, Config.GetName()));
        del->MutableRange()->SetIncludeFrom(true);
        del->MutableRange()->SetTo(MaxWALKey(PartitionId, Config.GetName()));
        del->MutableRange()->SetIncludeTo(true);

        NextWALIndex = 0;
        LOG_D("Snapshot Count: " << Storage->GetMessageCount() << " Size: " << write->GetValue().size());
    } else {
        NKikimrPQ::TMLPStorageWAL wal;
        batch.SerializeTo(wal);

        auto* write = request->Record.AddCmdWrite();
        write->SetKey(MakeWALKey(PartitionId, Config.GetName(), ++NextWALIndex));
        write->SetValue(wal.SerializeAsString());
        if (write->GetValue().size() < 1000) {
            write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
        }
        LOG_D("WAL Count: " << batch.AffectedMessageCount() << " Size: " << write->GetValue().size());
    }

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

    auto maxMessages = TStorage::MinMessages;
    if (metrics.LockedMessageCount * 2 > metrics.UnprocessedMessageCount) {
        maxMessages = std::max<size_t>(maxMessages, metrics.LockedMessageCount * 2 - metrics.UnprocessedMessageCount);
    }
    maxMessages = std::min(maxMessages, TStorage::MaxMessages - metrics.InflyMessageCount);

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
    if (FetchCookie != GetCookie(ev)) {
        // TODO MLP
        LOG_D("Cookie mismatch: " << FetchCookie << " != " << GetCookie(ev));
        //return;
    }

    FetchInProgress = false;

    if (!IsSucess(ev)) {
        LOG_W("Fetch messages failed: " << ev->Get()->Response->DebugString());
        return;
    }

    size_t messageCount = 0;
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

            Storage->AddMessage(
                result.GetOffset(),
                result.HasSourceId() && !result.GetSourceId().empty(),
                static_cast<ui32>(Hash(result.GetSourceId())),
                TInstant::MilliSeconds(result.GetWriteTimestampMS())
            );
            ++messageCount;
        }

        LOG_D("Fetched " << messageCount << " messages");

        ProcessEventQueue();
    }
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
