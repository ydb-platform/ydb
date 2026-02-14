#include "mlp_consumer.h"
#include "mlp_storage.h"

#include <ydb/core/persqueue/common/key.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/library/persqueue/counter_time_keeper/counter_time_keeper.h>

namespace NKikimr::NPQ::NMLP {

namespace {

static constexpr size_t MaxWALCount = 256;

enum class EKvCookie {
    InitialRead = 1,
    WALRead = 2,
    TxWrite = 3,
    BackgroundWrite = 4
};

void ReplyError(const TActorIdentity selfActorId, ui32 partitionId, const TActorId& sender, ui64 cookie, TString&& error) {
    selfActorId.Send(sender, new TEvPQ::TEvMLPErrorResponse(partitionId, Ydb::StatusIds::UNAVAILABLE, std::move(error)), 0, cookie);
}

template<typename T>
void ReplyErrorAll(const TActorIdentity selfActorId, ui32 partitionId, std::deque<T>& queue) {
    for (auto& ev : queue) {
        ReplyError(selfActorId, partitionId, ev->Sender, ev->Cookie, "Actor destroyed");
    }
    queue.clear();
}

template<typename T>
void RollbackAll(const TActorIdentity selfActorId, ui32 partitionId, std::deque<T>& queue) {
    for (auto& ev : queue) {
        ReplyError(selfActorId, partitionId, ev.Sender, ev.Cookie, "Rollback");
    }
    queue.clear();
}

template<typename R>
R* MakeOkResponse(ui32 partitionId) {
    Y_UNUSED(partitionId);
    return new R();
}

template<>
TEvPQ::TEvMLPPurgeResponse* MakeOkResponse<TEvPQ::TEvMLPPurgeResponse>(ui32 partitionId) {
    return new TEvPQ::TEvMLPPurgeResponse(partitionId);
}

template<typename R, typename T>
void ReplyOk(const TActorIdentity selfActorId, ui32 partitionId, std::deque<T>& queue) {
    for (auto& ev : queue) {
        selfActorId.Send(ev.Sender, MakeOkResponse<R>(partitionId), 0, ev.Cookie);
    }
    queue.clear();
}

}

TString MakeSnapshotKey(ui32 partitionId, const TString& consumerName) {
    TKeyPrefix ikey(TKeyPrefix::EType::TypeMLPConsumerData, TPartitionId(partitionId), TKeyPrefix::EMark::MarkMLPSnapshot);
    ikey.Append(consumerName.c_str(), consumerName.size());

    return ikey.ToString();
}

static constexpr char WALSeparator = '|';

TString MakeWALKey(ui32 partitionId, const TString& consumerName, ui64 index) {
    TKeyPrefix ikey(TKeyPrefix::EType::TypeMLPConsumerData, TPartitionId(partitionId), TKeyPrefix::EMark::MarkMLPWAL);
    ikey.Append(consumerName.c_str(), consumerName.size());
    ikey.Append(WALSeparator);

    auto bucket = Sprintf("%.16llX", index);
    ikey.Append(bucket.data(), bucket.size());

    return ikey.ToString();
}

TString MinWALKey(ui32 partitionId, const TString& consumerName) {
    return MakeWALKey(partitionId, consumerName, 0);
}

TString MaxWALKey(ui32 partitionId, const TString& consumerName) {
    return MakeWALKey(partitionId, consumerName, Max<ui64>());
}

void AddReadWAL(std::unique_ptr<TEvKeyValue::TEvRequest>& request, ui32 partitionId, const TString& consumerName, ui64 fromIndex = 0) {
    auto* readWAL = request->Record.AddCmdReadRange();
    readWAL->MutableRange()->SetFrom(MakeWALKey(partitionId, consumerName, fromIndex));
    readWAL->MutableRange()->SetIncludeFrom(false);
    readWAL->MutableRange()->SetTo(MaxWALKey(partitionId, consumerName));
    readWAL->MutableRange()->SetIncludeTo(true);
    readWAL->SetIncludeData(true);
}

TConsumerActor::TConsumerActor(const TString& database,ui64 tabletId, const TActorId& tabletActorId, ui32 partitionId,
    const TActorId& partitionActorId, const NKikimrPQ::TPQTabletConfig& topicConfig, const NKikimrPQ::TPQTabletConfig_TConsumer& config,
    std::optional<TDuration> retentionPeriod, ui64 partitionEndOffset, NMonitoring::TDynamicCounterPtr& detailedMetricsRoot)
    : TBaseTabletActor(tabletId, tabletActorId, NKikimrServices::EServiceKikimr::PQ_MLP_CONSUMER)
    , Database(database)
    , PartitionId(partitionId)
    , PartitionActorId(partitionActorId)
    , TopicConfig(topicConfig)
    , Config(config)
    , RetentionPeriod(retentionPeriod)
    , PartitionEndOffset(partitionEndOffset)
    , Storage(std::make_unique<TStorage>(CreateDefaultTimeProvider()))
    , DetailedMetricsRoot(detailedMetricsRoot) {
}

void TConsumerActor::Bootstrap() {
    LOG_D("Start MLP consumer " << Config.GetName());
    Become(&TConsumerActor::StateInit);

    UpdateStorageConfig();
    InitializeDetailedMetrics();

    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    request->Record.SetCookie(static_cast<ui64>(EKvCookie::InitialRead));
    request->Record.AddCmdRead()->SetKey(MakeSnapshotKey(PartitionId, Config.GetName()));
    AddReadWAL(request, PartitionId, Config.GetName());

    Send(TabletActorId, std::move(request));

    Schedule(WakeupInterval, new TEvents::TEvWakeup());
}

void TConsumerActor::PassAway() {
    LOG_D("PassAway");

    RollbackAll(SelfId(), PartitionId,PendingReadQueue);
    RollbackAll(SelfId(), PartitionId, PendingCommitQueue);
    RollbackAll(SelfId(), PartitionId, PendingUnlockQueue);
    RollbackAll(SelfId(), PartitionId, PendingChangeMessageDeadlineQueue);
    RollbackAll(SelfId(), PartitionId, PendingPurgeQueue);

    ReplyErrorAll(SelfId(), PartitionId, ReadRequestsQueue);
    ReplyErrorAll(SelfId(), PartitionId, CommitRequestsQueue);
    ReplyErrorAll(SelfId(), PartitionId, UnlockRequestsQueue);
    ReplyErrorAll(SelfId(), PartitionId, ChangeMessageDeadlineRequestsQueue);
    ReplyErrorAll(SelfId(), PartitionId, PurgeRequestsQueue);

    if (DLQMoverActorId) {
        Send(DLQMoverActorId, new TEvents::TEvPoison());
    }

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

void TConsumerActor::Queue(TEvPQ::TEvMLPPurgeRequest::TPtr& ev) {
    LOG_D("Queue TEvPQ::TEvMLPPurgeRequest " << ev->Get()->Record.ShortDebugString());
    PurgeRequestsQueue.push_back(std::move(ev));
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

void TConsumerActor::Handle(TEvPQ::TEvMLPPurgeRequest::TPtr& ev) {
    Queue(ev);
    ProcessEventQueue();
}

void TConsumerActor::HandleOnInit(TEvKeyValue::TEvResponse::TPtr& ev) {
    LOG_D("HandleOnInit TEvKeyValue::TEvResponse");
    auto& record = ev->Get()->Record;

    if (record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        return Restart(TStringBuilder() << "Received KV error on initialization: " << record.GetStatus());
    }

    switch (record.GetCookie()) {
        case static_cast<int>(EKvCookie::InitialRead): {
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
                        LOG_D("Read snapshot");
                        HasSnapshot = true;
                        LastWALIndex = snapshot.GetWALIndex();
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
        }
            [[fallthrough]];

        case static_cast<int>(EKvCookie::WALRead): {
            if (record.ReadRangeResultSize() != 1) {
                return Restart(TStringBuilder() << "Unexpected KV response on initialization: " << record.ReadResultSize());
            }

            auto& walResult = record.GetReadRangeResult(0);

            switch(walResult.GetStatus()) {
                case NKikimrProto::OK:
                case NKikimrProto::OVERRUN: {
                    for (auto& w : walResult.GetPair()) {
                        NKikimrPQ::TMLPStorageWAL wal;
                        if (!wal.ParseFromString(w.GetValue())) {
                            return Restart(TStringBuilder() << "Parse wal error");
                        }

                        if (Config.GetGeneration() == wal.GetGeneration()) {
                            LOG_D("Read WAL " << w.key());
                            LastWALIndex = wal.GetWALIndex();
                            Storage->ApplyWAL(wal);
                        } else {
                            LOG_W("Received WAL from old consumer generation: " << Config.GetGeneration() << " vs " << wal.GetGeneration() << " key: " << w.key());
                        }
                    }

                    if (walResult.GetStatus() == NKikimrProto::OVERRUN) {
                        LOG_D("WAL overrun");
                        auto request = std::make_unique<TEvKeyValue::TEvRequest>();
                        request->Record.SetCookie(static_cast<ui64>(EKvCookie::WALRead));

                        auto* readWAL = request->Record.AddCmdReadRange();
                        readWAL->MutableRange()->SetFrom(walResult.GetPair().rbegin()->GetKey());
                        readWAL->MutableRange()->SetIncludeFrom(false);
                        readWAL->MutableRange()->SetTo(MaxWALKey(PartitionId, Config.GetName()));
                        readWAL->MutableRange()->SetIncludeTo(true);
                        readWAL->SetIncludeData(true);

                        Send(TabletActorId, std::move(request));
                        return;
                    }

                    break;
                }
                case NKikimrProto::NODATA: {
                    LOG_D("Initializing new consumer");
                    break;
                }
                default:
                    return Restart(TStringBuilder() << "Received KV response error on initialization: " << walResult.GetStatus());
            }

            break;
        }
        default:
            AFL_ENSURE(false)("c", record.GetCookie());
    }

    Storage->InitMetrics();
    CommitIfNeeded();

    if (!FetchMessagesIfNeeded()) {
        LOG_D("Initialized");
        NotifyPQRB(true);
        Become(&TConsumerActor::StateWork);
        ProcessEventQueue();
    }
}

void TConsumerActor::Handle(TEvKeyValue::TEvResponse::TPtr& ev) {
    LOG_D("HandleOnWrite TEvKeyValue::TEvResponse " << ev->Get()->Record.ShortDebugString());

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

    if (record.GetCookie() == static_cast<ui64>(EKvCookie::BackgroundWrite)) {
        LOG_D("Background write finished");
        return;
    }

    AFL_ENSURE(CurrentStateFunc() == &TConsumerActor::StateWrite)("c", record.GetCookie());

    LOG_D("TX write finished");
    Become(&TConsumerActor::StateWork);

    CommitIfNeeded();

    if (!PendingReadQueue.empty()) {
        auto msgs = std::exchange(PendingReadQueue, {});
        RegisterWithSameMailbox(CreateMessageEnricher(TabletId, PartitionId, Config.GetName(), std::move(msgs)));
    }
    ReplyOk<TEvPQ::TEvMLPCommitResponse>(SelfId(), PartitionId, PendingCommitQueue);
    ReplyOk<TEvPQ::TEvMLPUnlockResponse>(SelfId(), PartitionId, PendingUnlockQueue);
    ReplyOk<TEvPQ::TEvMLPChangeMessageDeadlineResponse>(SelfId(), PartitionId, PendingChangeMessageDeadlineQueue);
    ReplyOk<TEvPQ::TEvMLPPurgeResponse>(SelfId(), PartitionId, PendingPurgeQueue);

    MoveToDLQIfPossible();
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

void TConsumerActor::UpdateStorageConfig() {
    LOG_D("Update config: RetentionPeriod: " << (RetentionPeriod.has_value() ? RetentionPeriod->ToString() : "infinity")
        << " " << Config.ShortDebugString());

    Storage->SetKeepMessageOrder(Config.GetKeepMessageOrder());
    Storage->SetMaxMessageProcessingCount(Config.GetMaxProcessingAttempts());
    Storage->SetRetentionPeriod(RetentionPeriod);
    if (Config.GetDeadLetterPolicyEnabled() && Config.GetDeadLetterPolicy() != NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_UNSPECIFIED) {
        Storage->SetDeadLetterPolicy(Config.GetDeadLetterPolicy());
    } else {
        Storage->SetDeadLetterPolicy(std::nullopt);
    }
}

void TConsumerActor::InitializeDetailedMetrics() {
    if (DetailedMetricsAreEnabled(TopicConfig)) {
        DetailedMetrics = std::make_unique<TDetailedMetrics>(Config, DetailedMetricsRoot);
    } else {
        DetailedMetrics = nullptr;
    }
}

void TConsumerActor::Handle(TEvPQ::TEvMLPConsumerUpdateConfig::TPtr& ev) {
    AFL_ENSURE(Config.GetGeneration() == ev->Get()->Config.GetGeneration())
        ("c", Config.GetName())
        ("l", Config.GetGeneration())
        ("r", ev->Get()->Config.GetGeneration());

    TopicConfig = std::move(ev->Get()->TopicConfig);
    Config = std::move(ev->Get()->Config);
    RetentionPeriod = std::move(ev->Get()->RetentionPeriod);
    DetailedMetricsRoot = std::move(ev->Get()->DetailedMetricsRoot);

    UpdateStorageConfig();
    InitializeDetailedMetrics();
}

void TConsumerActor::HandleInit(TEvPQ::TEvEndOffsetChanged::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvEndOffsetChanged. Offset: " << ev->Get()->Offset);
    PartitionEndOffset = ev->Get()->Offset;
}

void TConsumerActor::Handle(TEvPQ::TEvEndOffsetChanged::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvEndOffsetChanged. Offset: " << ev->Get()->Offset);
    PartitionEndOffset = ev->Get()->Offset;
    FetchMessagesIfNeeded();
}

void TConsumerActor::Handle(TEvPQ::TEvGetMLPConsumerStateRequest::TPtr& ev) {
    auto response = std::make_unique<TEvPQ::TEvGetMLPConsumerStateResponse>();
    response->RetentionPeriod = RetentionPeriod;
    response->Config = Config;

    for (auto it = Storage->begin(); it != Storage->end(); ++it) {
        auto msg = *it;

        response->Messages.push_back({
            .Offset = msg.Offset,
            .Status = static_cast<ui8>(msg.Status),
            .ProcessingCount = msg.ProcessingCount,
            .ProcessingDeadline = msg.ProcessingDeadline,
            .WriteTimestamp = msg.WriteTimestamp
        });
    }

    Send(ev->Sender, std::move(response), 0, ev->Cookie);
}

void TConsumerActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
    FirstPipeCacheRequest = true;
}

STFUNC(TConsumerActor::StateInit) {
    NPersQueue::TCounterTimeKeeper<ui64> keeper(CPUUsageMetric);

    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPQ::TEvMLPReadRequest, Queue);
        hFunc(TEvPQ::TEvMLPCommitRequest, Queue);
        hFunc(TEvPQ::TEvMLPUnlockRequest, Queue);
        hFunc(TEvPQ::TEvMLPChangeMessageDeadlineRequest, Queue);
        hFunc(TEvPQ::TEvMLPPurgeRequest, Queue);
        hFunc(TEvPQ::TEvMLPConsumerUpdateConfig, Handle);
        hFunc(TEvPQ::TEvEndOffsetChanged, HandleInit);
        hFunc(TEvPQ::TEvGetMLPConsumerStateRequest, Handle);
        hFunc(TEvPQ::TEvMLPConsumerMonRequest, Handle);
        hFunc(TEvKeyValue::TEvResponse, HandleOnInit);
        hFunc(TEvPersQueue::TEvResponse, HandleOnInit);
        hFunc(TEvPQ::TEvError, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
        default:
            LOG_E("Unexpected " << EventStr("StateInit", ev));
            AFL_VERIFY_DEBUG(false)("Unexpected", EventStr("StateInit", ev));
    }
}

STFUNC(TConsumerActor::StateWork) {
    NPersQueue::TCounterTimeKeeper<ui64> keeper(CPUUsageMetric);

    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPQ::TEvMLPReadRequest, Handle);
        hFunc(TEvPQ::TEvMLPCommitRequest, Handle);
        hFunc(TEvPQ::TEvMLPUnlockRequest, Handle);
        hFunc(TEvPQ::TEvMLPChangeMessageDeadlineRequest, Handle);
        hFunc(TEvPQ::TEvMLPPurgeRequest, Handle);
        hFunc(TEvPQ::TEvMLPConsumerUpdateConfig, Handle);
        hFunc(TEvPQ::TEvEndOffsetChanged, Handle);
        hFunc(TEvPQ::TEvGetMLPConsumerStateRequest, Handle);
        hFunc(TEvPQ::TEvMLPConsumerMonRequest, Handle);
        hFunc(TEvKeyValue::TEvResponse, Handle);
        hFunc(TEvPQ::TEvProxyResponse, Handle);
        hFunc(TEvPersQueue::TEvResponse, Handle);
        hFunc(TEvPQ::TEvError, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        hFunc(TEvPQ::TEvMLPDLQMoverResponse, Handle);
        hFunc(TEvents::TEvWakeup, HandleOnWork);
        sFunc(TEvents::TEvPoison, PassAway);
        default:
            LOG_E("Unexpected " << EventStr("StateWork", ev));
            AFL_VERIFY_DEBUG(false)("Unexpected", EventStr("StateWork", ev));
    }
}

STFUNC(TConsumerActor::StateWrite) {
    NPersQueue::TCounterTimeKeeper<ui64> keeper(CPUUsageMetric);

    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPQ::TEvMLPReadRequest, Queue);
        hFunc(TEvPQ::TEvMLPCommitRequest, Queue);
        hFunc(TEvPQ::TEvMLPUnlockRequest, Queue);
        hFunc(TEvPQ::TEvMLPChangeMessageDeadlineRequest, Queue);
        hFunc(TEvPQ::TEvMLPPurgeRequest, Queue);
        hFunc(TEvPQ::TEvMLPConsumerUpdateConfig, Handle);
        hFunc(TEvPQ::TEvEndOffsetChanged, Handle);
        hFunc(TEvPQ::TEvGetMLPConsumerStateRequest, Handle);
        hFunc(TEvPQ::TEvMLPConsumerMonRequest, Handle);
        hFunc(TEvKeyValue::TEvResponse, Handle);
        hFunc(TEvPQ::TEvProxyResponse, Handle);
        hFunc(TEvPersQueue::TEvResponse, Handle);
        hFunc(TEvPQ::TEvError, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        hFunc(TEvPQ::TEvMLPDLQMoverResponse, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
        default:
            LOG_E("Unexpected " << EventStr("StateWrite", ev));
            AFL_VERIFY_DEBUG(false)("Unexpected", EventStr("StateWrite", ev));
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
        for (const auto &message : ev->Get()->Record.GetMessage()) {
            Storage->ChangeMessageDeadline(message.GetOffset(), TInstant::Seconds(message.GetDeadlineTimestampSeconds()));
        }

        PendingChangeMessageDeadlineQueue.emplace_back(ev->Sender, ev->Cookie);
    }
    ChangeMessageDeadlineRequestsQueue.clear();

    for (auto& ev : PurgeRequestsQueue) {
        Storage->Purge(PartitionEndOffset);
        PendingPurgeQueue.emplace_back(ev->Sender, ev->Cookie);
    }
    PurgeRequestsQueue.clear();

    if (!ReadRequestsQueue.empty()) {
        Storage->ProccessDeadlines();
        LOG_T("AfterDeadlinesDump: " << Storage->DebugString());
    }

    auto now = TInstant::Now();

    TStorage::TPosition position;
    std::deque<TEvPQ::TEvMLPReadRequest::TPtr> readRequestsQueue;
    for (auto& ev : ReadRequestsQueue) {
        size_t count = ev->Get()->GetMaxNumberOfMessages();
        auto visibilityDeadline = ev->Get()->GetVisibilityDeadline();
        if (visibilityDeadline == TInstant::Zero()) {
            visibilityDeadline = TDuration::Seconds(Config.GetDefaultProcessingTimeoutSeconds()).ToDeadLine(now);
        }

        std::deque<ui64> messages;
        for (; count; --count) {
            auto result = Storage->Next(visibilityDeadline, position);
            if (!result) {
                break;
            }

            messages.push_back(result.value());
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

    Persist();
}

void TConsumerActor::Persist() {
    LOG_D("Persist");

    Storage->Compact();

    auto batch = Storage->GetBatch();
    if (batch.Empty()) {
        LOG_D("Batch is empty");
        MoveToDLQIfPossible();
        return;
    }

    Become(&TConsumerActor::StateWrite);

    LOG_T("Dump befor persist: " << Storage->DebugString());

    auto tryInlineChannel = [](auto& write) {
        if (write->GetValue().size() < 1000) {
            write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
        }
    };

    auto withWAL = HasSnapshot && Storage->GetMessageCount() > 32 && !batch.GetPurged();
    if (withWAL) {
        auto key = MakeWALKey(PartitionId, Config.GetName(), ++LastWALIndex);

        NKikimrPQ::TMLPStorageWAL wal;
        wal.SetGeneration(Config.GetGeneration());
        wal.SetWALIndex(LastWALIndex);
        batch.SerializeTo(wal);

        auto data = wal.SerializeAsString();
        LOG_D("Write WAL Size: " << data.size() << " Key: " << key);

        auto request = std::make_unique<TEvKeyValue::TEvRequest>();
        request->Record.SetCookie(static_cast<ui64>(EKvCookie::TxWrite));
        auto* write = request->Record.AddCmdWrite();
        write->SetKey(std::move(key));
        write->SetValue(std::move(data));
        tryInlineChannel(write);

        Send(TabletActorId, std::move(request));
    }

    if (!withWAL || LastWALIndex % MaxWALCount == 0) {
        HasSnapshot = true;

        NKikimrPQ::TMLPStorageSnapshot snapshot;

        auto* config = snapshot.MutableConfiguration();
        config->SetConsumerName(Config.GetName());
        config->SetGeneration(Config.GetGeneration());
        Storage->SerializeTo(snapshot);

        snapshot.SetWALIndex(LastWALIndex);

        auto request = std::make_unique<TEvKeyValue::TEvRequest>();

        auto cookie = withWAL ? static_cast<ui64>(EKvCookie::BackgroundWrite) : static_cast<ui64>(EKvCookie::TxWrite);
        request->Record.SetCookie(cookie);

        auto* write = request->Record.AddCmdWrite();
        write->SetKey(MakeSnapshotKey(PartitionId, Config.GetName()));
        write->SetValue(snapshot.SerializeAsString());
        write->SetPriority(withWAL ? ::NKikimrClient::TKeyValueRequest::BACKGROUND : ::NKikimrClient::TKeyValueRequest::REALTIME);
        tryInlineChannel(write);


        auto from = MinWALKey(PartitionId, Config.GetName());
        auto to = MakeWALKey(PartitionId, Config.GetName(), LastWALIndex);

        LOG_D("Delete old WAL: " << from << " - " << to);

        auto* del = request->Record.AddCmdDeleteRange();
        del->MutableRange()->SetFrom(std::move(from));
        del->MutableRange()->SetIncludeFrom(true);
        del->MutableRange()->SetTo(std::move(to));
        del->MutableRange()->SetIncludeTo(true);

        Send(TabletActorId, std::move(request));

        LOG_D("Write Snapshot Count: " << Storage->GetMessageCount() << " Size: " << write->GetValue().size() << " cookie: " << cookie);
    }
}

size_t TConsumerActor::RequiredToFetchMessageCount() const {
    auto& metrics = Storage->GetMetrics();

    auto maxMessages = Storage->HasRetentionExpiredMessages() ? Storage->MaxMessages : Storage->MinMessages;
    if (metrics.LockedMessageCount * 2 > metrics.UnprocessedMessageCount) {
        maxMessages = std::max<size_t>(maxMessages, metrics.LockedMessageCount * 2 - metrics.UnprocessedMessageCount);
    }

    return std::min(maxMessages, Storage->MaxMessages - metrics.InflightMessageCount);
}

bool TConsumerActor::FetchMessagesIfNeeded() {
    if (Storage->GetMessageCount() > 0) {
        LastTimeWithMessages = TInstant::Now();
        NotifyPQRB();
    }

    if (FetchInProgress) {
        return false;
    }

    if (PartitionEndOffset <= Storage->GetLastOffset()) {
        LOG_D("Skip fetch: partition end offset is reached: " << PartitionEndOffset << " vs " << Storage->GetLastOffset());
        return false;
    }

    auto& metrics = Storage->GetMetrics();
    if (metrics.InflightMessageCount >= Storage->MaxMessages) {
        LOG_D("Skip fetch: infly limit exceeded");
        return false;
    }
    if (!Config.GetKeepMessageOrder()
        && metrics.InflightMessageCount >= Storage->MinMessages
        && metrics.UnprocessedMessageCount >= metrics.LockedMessageCount * 2
        && !Storage->HasRetentionExpiredMessages()) {
        LOG_D("Skip fetch: there are enough messages. InflightMessageCount=" << metrics.InflightMessageCount
            << ", UnprocessedMessageCount=" << metrics.UnprocessedMessageCount
            << ", LockedMessageCount=" << metrics.LockedMessageCount);
        return false;
    }

    FetchInProgress = true;

    auto maxMessages = RequiredToFetchMessageCount();
    LOG_D("Fetching " << maxMessages << " messages from offset " << Storage->GetLastOffset() << " from " << PartitionActorId);
    Send(TabletActorId, MakeEvPQRead(Config.GetName(), PartitionId, Storage->GetLastOffset(), maxMessages));

    return true;
}

void TConsumerActor::Handle(TEvPQ::TEvProxyResponse::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvProxyResponse");

    AFL_ENSURE(IsSucess(ev))("e", ev->Get()->Response->DebugString());
}

void TConsumerActor::HandleOnInit(TEvPersQueue::TEvResponse::TPtr& ev) {
    LOG_D("Initialized");
    Become(&TConsumerActor::StateWork);
    Handle(ev);
}

void TConsumerActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
    LOG_D("Handle TEvPersQueue::TEvResponse");

    FetchInProgress = false;

    if (!IsSucess(ev)) {
        LOG_W("Fetch messages failed: " << ev->Get()->Record.DebugString());
        return;
    }

    auto& response = ev->Get()->Record;
    AFL_ENSURE(response.GetPartitionResponse().HasCmdReadResult());
    auto& results = response.GetPartitionResponse().GetCmdReadResult();

    bool allMessagesAdded = false;
    size_t messageCount = 0;

    auto lastOffset = Storage->GetLastOffset();
    for (auto& result : results.GetResult()) {
        if (lastOffset > result.GetOffset()) {
            continue;
        }

        TString messageGroupId;
        size_t delaySeconds = 0;

        NKikimrPQClient::TDataChunk proto;
        bool res = proto.ParseFromString(result.GetData());
        AFL_ENSURE(res)("o", result.GetOffset());

        for (auto& attr : *proto.MutableMessageMeta()) {
            if (attr.key() == MESSAGE_ATTRIBUTE_KEY) {
                messageGroupId = std::move(*attr.mutable_value());
            } else if (attr.key() == MESSAGE_ATTRIBUTE_DELAY_SECONDS) {
                delaySeconds = std::stoul(attr.value());
            }
        }

        allMessagesAdded = Storage->AddMessage(
            result.GetOffset(),
            !messageGroupId.empty(),
            static_cast<ui32>(Hash(messageGroupId)),
            TInstant::MilliSeconds(result.GetWriteTimestampMS()),
            TDuration::Seconds(delaySeconds)
        );
        if (!allMessagesAdded) {
            break;
        }
        ++messageCount;
    }

    LOG_D("Fetched " << messageCount << " messages");
    if (allMessagesAdded) {
        FetchMessagesIfNeeded();
    }

    if (messageCount > 0) {
        LastTimeWithMessages = TInstant::Now();
        NotifyPQRB();
    }

    if (CurrentStateFunc() == &TConsumerActor::StateWork) {
        ProcessEventQueue();
    }
}

void TConsumerActor::Handle(TEvPQ::TEvError::TPtr& ev) {
    Restart(TStringBuilder() << "Received error: " << ev->Get()->Error);
}

void TConsumerActor::HandleOnWork(TEvents::TEvWakeup::TPtr&) {
    FetchMessagesIfNeeded();
    ProcessEventQueue();
    UpdateMetrics();
    NotifyPQRB();
    Schedule(WakeupInterval, new TEvents::TEvWakeup());
}

void TConsumerActor::MoveToDLQIfPossible() {
    if (DLQMoverActorId) {
        return;
    }
    auto messages = Storage->GetDLQMessages();
    if (!messages.empty()) {
        DLQMoverActorId = RegisterWithSameMailbox(CreateDLQMover({
            .ParentActorId = SelfId(),
            .Database = Database,
            .TabletId = TabletId,
            .PartitionId = PartitionId,
            .ConsumerName = Config.GetName(),
            .ConsumerGeneration = Config.GetGeneration(),
            .DestinationTopic = Config.GetDeadLetterQueue(),
            .Messages = std::move(messages)
        }));
    }
}

void TConsumerActor::Handle(TEvPQ::TEvMLPDLQMoverResponse::TPtr& ev) {
    LOG_D("Handle TEvPQ::TEvMLPDLQMoverResponse");

    if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
        LOG_W("Error moving messages to the DLQ: " << ev->Get()->ErrorDescription);
    }

    auto& moved = ev->Get()->MovedMessages;
    LOG_D("Moved to the DLQ: " << JoinRange(", ", moved.begin(), moved.end()));

    DLQMoverActorId = {};
    for (auto [offset, seqNo] : moved) {
        auto result = Storage->MarkDLQMoved({
            .Offset = offset,
            .SeqNo = seqNo
        });
        AFL_ENSURE(result)("o", offset)("s", seqNo);
    }

    if (ev->Get()->Status == Ydb::StatusIds::NOT_FOUND) {
        Storage->WakeUpDLQ();
    }

    if (CurrentStateFunc() == &TConsumerActor::StateWork) {
        ProcessEventQueue();
    }
}

void TConsumerActor::Handle(TEvents::TEvWakeup::TPtr&) {
    LOG_D("Handle TEvents::TEvWakeup");
    UpdateMetrics();
    NotifyPQRB();
    Schedule(WakeupInterval, new TEvents::TEvWakeup());
}

void TConsumerActor::SendToPQTablet(std::unique_ptr<IEventBase> ev) {
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev.release(), TabletId, FirstPipeCacheRequest, 1);
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
    FirstPipeCacheRequest = false;
}

bool TConsumerActor::UseForReading() const {
    return LastTimeWithMessages > TInstant::Now() - NoMessagesTimeout;
}

void TConsumerActor::NotifyPQRB(bool force) {
    auto useForReading = UseForReading();
    if (force || useForReading != LastUseForReading) {
        auto ev = std::make_unique<TEvPQ::TEvMLPConsumerStatus>(Config.GetName(), PartitionId,
            PartitionEndOffset - LastCommittedOffset, useForReading);
        Send(PartitionActorId, std::move(ev));
        LastUseForReading = useForReading;
    }
}

NActors::IActor* CreateConsumerActor(
    const TString& database,
    ui64 tabletId,
    const NActors::TActorId& tabletActorId,
    ui32 partitionId,
    const NActors::TActorId& partitionActorId,
    const NKikimrPQ::TPQTabletConfig& topicConfig,
    const NKikimrPQ::TPQTabletConfig_TConsumer& config,
    const std::optional<TDuration> retention,
    ui64 partitionEndOffset,
    NMonitoring::TDynamicCounterPtr detailedMetricsRoot) {
    return new TConsumerActor(database, tabletId, tabletActorId, partitionId, partitionActorId, topicConfig, config, retention, partitionEndOffset, detailedMetricsRoot);
}

}
