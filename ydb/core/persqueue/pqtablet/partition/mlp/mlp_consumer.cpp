#include "mlp_consumer.h"
#include "mlp_storage.h"

#include <ydb/core/persqueue/common/key.h>
#include <ydb/core/persqueue/public/config.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/library/persqueue/counter_time_keeper/counter_time_keeper.h>

#include <util/generic/serialized_enum.h>
#include <util/stream/format.h>

#include <ranges>

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ::NMLP {

namespace {

static constexpr size_t MaxWALCount = 1024;

enum class EKvCookie {
    InitialRead = 1,
    WALRead = 2,
    TxWrite = 3,
    BackgroundWrite = 4
};

enum EWakeUpTag {
    Regular = 1,
    Processing = 2,
    UpdateChildPartitions = 3,
};

enum class ESendCookie {
    SendToPQTablet = 1,
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
void ReplyOk(const TActorIdentity selfActorId, ui32 partitionId, T& ev) {
    selfActorId.Send(ev.Sender, MakeOkResponse<R>(partitionId), 0, ev.Cookie);
}

template<typename R, typename T>
void ReplyOk(const TActorIdentity selfActorId, ui32 partitionId, std::deque<T>& queue) {
    for (auto& ev : queue) {
        ReplyOk<R>(selfActorId, partitionId, ev);
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

static TStorage::TStorageSettings StorageSettingsFromConfig(const NKikimrPQ::TPQTabletConfig::TConsumer& config, const NKikimrPQ::TPQTabletConfig::TPartition& partitionConfig) {
    const bool keepMessageOrder = config.GetKeepMessageOrder();
    std::vector<ui32> parentPartitionId;
    for (ui32 p : partitionConfig.GetParentPartitionIds()) {
        parentPartitionId.push_back(p);
    }
    return TStorage::TStorageSettings{
        .KeepMessageOrder = keepMessageOrder,
        .ParentPartitionId = std::move(parentPartitionId),
    };
}

void AddReadWAL(std::unique_ptr<TEvKeyValue::TEvRequest>& request, ui32 partitionId, const TString& consumerName, ui64 fromIndex = 0) {
    auto* readWAL = request->Record.AddCmdReadRange();
    readWAL->MutableRange()->SetFrom(MakeWALKey(partitionId, consumerName, fromIndex));
    readWAL->MutableRange()->SetIncludeFrom(false);
    readWAL->MutableRange()->SetTo(MaxWALKey(partitionId, consumerName));
    readWAL->MutableRange()->SetIncludeTo(true);
    readWAL->SetIncludeData(true);
}

TConsumerActor::TConsumerActor(
    const TString& database,
    ui64 tabletId,
    const TActorId& tabletActorId,
    ui32 partitionId,
    const TActorId& partitionActorId,
    ui64 partitionGeneration,
    const NKikimrPQ::TPQTabletConfig& topicConfig,
    const NKikimrPQ::TPQTabletConfig_TConsumer& config,
    std::optional<TDuration> retentionPeriod, ui64 partitionEndOffset,
    NMonitoring::TDynamicCounterPtr& detailedMetricsRoot)
    : TBaseTabletActor(tabletId, tabletActorId, NKikimrServices::EServiceKikimr::PQ_MLP_CONSUMER)
    , Database(database)
    , PartitionId(partitionId)
    , PartitionActorId(partitionActorId)
    , PartitionGeneration(partitionGeneration)
    , TopicConfig(topicConfig)
    , Config(config)
    , RetentionPeriod(retentionPeriod)
    , PartitionEndOffset(partitionEndOffset)
    , Storage(std::make_unique<TStorage>(CreateDefaultTimeProvider(), StorageSettingsFromConfig(Config, GetPartitionConfig())))
    , DetailedMetricsRoot(detailedMetricsRoot) {
}

void TConsumerActor::Bootstrap() {
    YDB_LOG_DEBUG("Start MLP consumer",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"#_Config.GetName", Config.GetName()});
    Become(&TConsumerActor::StateInit);

    UpdateStorageConfig();
    InitializeDetailedMetrics();

    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    request->Record.SetCookie(static_cast<ui64>(EKvCookie::InitialRead));
    request->Record.AddCmdRead()->SetKey(MakeSnapshotKey(PartitionId, Config.GetName()));
    AddReadWAL(request, PartitionId, Config.GetName());

    Send(TabletActorId, std::move(request));

    Schedule(WakeupInterval, new TEvents::TEvWakeup(EWakeUpTag::Regular));
}

void TConsumerActor::PassAway() {
    YDB_LOG_DEBUG("PassAway",
        {"logPrefix", NPQ_LOG_PREFIX});

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

    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));

    TBase::PassAway();
}

TString TConsumerActor::BuildLogPrefix() const {
    return TStringBuilder() << "[" << PartitionId << "][MLP][" << Config.GetName() << "] ";
}

void TConsumerActor::Queue(TEvPQ::TEvMLPReadRequest::TPtr& ev) {
    YDB_LOG_DEBUG("Queue TEvPQ::TEvMLPReadRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    ReadRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Queue(TEvPQ::TEvMLPCommitRequest::TPtr& ev) {
    YDB_LOG_DEBUG("Queue TEvPQ::TEvMLPCommitRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    CommitRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Queue(TEvPQ::TEvMLPUnlockRequest::TPtr& ev) {
    YDB_LOG_DEBUG("Queue TEvPQ::TEvMLPUnlockRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    UnlockRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Queue(TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    YDB_LOG_DEBUG("Queue TEvPQ::TEvMLPChangeMessageDeadlineRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    ChangeMessageDeadlineRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Queue(TEvPQ::TEvMLPPurgeRequest::TPtr& ev) {
    YDB_LOG_DEBUG("Queue TEvPQ::TEvMLPPurgeRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    PurgeRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Queue(TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId::TPtr& ev) {
    YDB_LOG_DEBUG("Queue TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});
    UpdateExternalLockedMessageGroupsIdRequestsQueue.push_back(std::move(ev));
}

void TConsumerActor::Handle(TEvPQ::TEvMLPReadRequest::TPtr& ev) {
    Queue(ev);
    ScheduleProcessing();
}

void TConsumerActor::Handle(TEvPQ::TEvMLPCommitRequest::TPtr& ev) {
    Queue(ev);
    ScheduleProcessing();
}

void TConsumerActor::Handle(TEvPQ::TEvMLPUnlockRequest::TPtr& ev) {
    Queue(ev);
    ScheduleProcessing();
}

void TConsumerActor::Handle(TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    Queue(ev);
    ScheduleProcessing();
}

void TConsumerActor::Handle(TEvPQ::TEvMLPPurgeRequest::TPtr& ev) {
    Queue(ev);
    ScheduleProcessing();
}

void TConsumerActor::Handle(TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId::TPtr& ev) {
    Queue(ev);
    ScheduleProcessing();
}

void TConsumerActor::HandleOnInit(TEvKeyValue::TEvResponse::TPtr& ev) {
    YDB_LOG_DEBUG("HandleOnInit TEvKeyValue::TEvResponse",
        {"logPrefix", NPQ_LOG_PREFIX});
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
                        YDB_LOG_DEBUG("Read snapshot",
                            {"logPrefix", NPQ_LOG_PREFIX});
                        HasSnapshot = true;
                        LastWALIndex = snapshot.GetWALIndex();
                        Storage->Initialize(snapshot);
                    } else {
                        YDB_LOG_WARN("Received snapshot from old consumer vs",
                            {"logPrefix", NPQ_LOG_PREFIX},
                            {"generation", Config.GetGeneration()},
                            {"#_snapshot.GetConfiguration().GetGeneration", snapshot.GetConfiguration().GetGeneration()});
                    }

                    break;
                }
                case NKikimrProto::NODATA: {
                    YDB_LOG_DEBUG("Initializing new consumer",
                        {"logPrefix", NPQ_LOG_PREFIX});
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
                            YDB_LOG_DEBUG("Read WAL",
                                {"logPrefix", NPQ_LOG_PREFIX},
                                {"#_w.key", w.key()});
                            LastWALIndex = wal.GetWALIndex();
                            Storage->ApplyWAL(wal);
                        } else {
                            YDB_LOG_WARN("Received WAL from old consumer vs",
                                {"logPrefix", NPQ_LOG_PREFIX},
                                {"generation", Config.GetGeneration()},
                                {"#_wal.GetGeneration", wal.GetGeneration()},
                                {"key", w.key()});
                        }
                    }

                    if (walResult.GetStatus() == NKikimrProto::OVERRUN) {
                        YDB_LOG_DEBUG("WAL overrun",
                            {"logPrefix", NPQ_LOG_PREFIX});
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
                    YDB_LOG_DEBUG("Initializing new consumer",
                        {"logPrefix", NPQ_LOG_PREFIX});
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
    UpdateLockedGroupsIdInChildPartitions(true);

    if (!FetchMessagesIfNeeded()) {
        YDB_LOG_DEBUG("Initialized",
            {"logPrefix", NPQ_LOG_PREFIX});
        NotifyPQRB(true);
        Become(&TConsumerActor::StateWork);
        ProcessEventQueue();
    }
}

void TConsumerActor::Handle(TEvKeyValue::TEvResponse::TPtr& ev) {
    YDB_LOG_DEBUG("HandleOnWrite TEvKeyValue::TEvResponse",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});

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
        YDB_LOG_DEBUG("Background write finished",
            {"logPrefix", NPQ_LOG_PREFIX});
        return;
    }

    AFL_ENSURE(CurrentStateFunc() == &TConsumerActor::StateWrite)("c", record.GetCookie());

    YDB_LOG_DEBUG("TX write finished",
        {"logPrefix", NPQ_LOG_PREFIX});
    Become(&TConsumerActor::StateWork);

    CommitIfNeeded();
    UpdateChildPartitionsOnCommit();

    if (!PendingReadQueue.empty()) {
        auto msgs = std::exchange(PendingReadQueue, {});
        RegisterWithSameMailbox(CreateMessageEnricher(TabletId, PartitionId, Config.GetName(), std::move(msgs)));
    }
    ReplyOk<TEvPQ::TEvMLPCommitResponse>(SelfId(), PartitionId, PendingCommitQueue);
    ReplyOk<TEvPQ::TEvMLPUnlockResponse>(SelfId(), PartitionId, PendingUnlockQueue);
    ReplyOk<TEvPQ::TEvMLPChangeMessageDeadlineResponse>(SelfId(), PartitionId, PendingChangeMessageDeadlineQueue);
    ReplyOk<TEvPQ::TEvMLPPurgeResponse>(SelfId(), PartitionId, PendingPurgeQueue);

    MoveToDLQIfPossible();
    FetchMessagesIfNeeded();
    ScheduleProcessing();
}

void TConsumerActor::CommitIfNeeded() {
    auto offset = Storage->GetFirstUncommittedOffset();
    YDB_LOG_DEBUG("Try commit vs",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"offset", offset},
        {"lastCommittedOffset", LastCommittedOffset});
    if (LastCommittedOffset != offset) {
        Send(PartitionActorId, MakeEvCommit(Config, offset));
        LastCommittedOffset = offset;
    }
}

void TConsumerActor::UpdateChildPartitionsOnCommit() {
    if (GetPartitionConfig().GetStatus() == NKikimrPQ::ETopicPartitionStatus::Active) {
        return;
    }
    bool update = false;
    if (LastCommittedOffset == PartitionEndOffset) {
        update = ChildPartitionsOrderManager.SetSendFullStateToAll(TChildPartitionsOrderManager::ESendReasons::Done, Storage->GetEstimatedLockedMessageGroupsIdSizeFromSelfAndParents());
    }
    if (!update) {
        if (PartitionEndOffset <= Storage->GetLastOffset()) {
            update = ChildPartitionsOrderManager.SetSendFullStateToAll(TChildPartitionsOrderManager::ESendReasons::Commit, Storage->GetEstimatedLockedMessageGroupsIdSizeFromSelfAndParents());
        }
    }
    if (update) {
        UpdateLockedGroupsIdInChildPartitions(false);
    }
}

void TConsumerActor::UpdateStorageConfig() {
    YDB_LOG_DEBUG("Update config",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"retentionPeriod", (RetentionPeriod.has_value() ? RetentionPeriod->ToString() : "infinity")},
        {"#_Config", Config});

    AFL_ENSURE(Storage->GetKeepMessageOrder() == Config.GetKeepMessageOrder())("initial", Storage->GetKeepMessageOrder())("new", Config.GetKeepMessageOrder());
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
    UpdateLockedGroupsIdInChildPartitions(false);
}

void TConsumerActor::HandleInit(TEvPQ::TEvEndOffsetChanged::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvEndOffsetChanged",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"offset", ev->Get()->Offset});
    PartitionEndOffset = ev->Get()->Offset;
}

void TConsumerActor::Handle(TEvPQ::TEvEndOffsetChanged::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvEndOffsetChanged",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"offset", ev->Get()->Offset});
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

void TConsumerActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    if (ev->Cookie == static_cast<int>(ESendCookie::SendToPQTablet)) {
        FirstPipeCacheRequest = true;
    } else {
        bool update = ChildPartitionsOrderManager.SetSendFullStateByCookie(ev->Cookie, TChildPartitionsOrderManager::ESendReasons::DeliveryProblem);
        if (update) {
            Schedule(ChildPartitionsOrderManager.UpdateChildPartitionsBackoff.Next(), new TEvents::TEvWakeup(EWakeUpTag::UpdateChildPartitions));
        }
    }
}

STFUNC(TConsumerActor::StateInit) {
    NPersQueue::TCounterTimeKeeper<ui64> keeper(CPUUsageMetric);

    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPQ::TEvMLPReadRequest, Queue);
        hFunc(TEvPQ::TEvMLPCommitRequest, Queue);
        hFunc(TEvPQ::TEvMLPUnlockRequest, Queue);
        hFunc(TEvPQ::TEvMLPChangeMessageDeadlineRequest, Queue);
        hFunc(TEvPQ::TEvMLPPurgeRequest, Queue);
        hFunc(TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId, Queue);
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
            YDB_LOG_ERROR("Unexpected",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"#_num_0", EventStr("StateInit", ev)});
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
        hFunc(TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId, Handle);
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
            YDB_LOG_ERROR("Unexpected",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"#_num_0", EventStr("StateWork", ev)});
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
        hFunc(TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId, Queue);
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
            YDB_LOG_ERROR("Unexpected",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"#_num_0", EventStr("StateWrite", ev)});
            AFL_VERIFY_DEBUG(false)("Unexpected", EventStr("StateWrite", ev));
    }
}

void TConsumerActor::Restart(TString&& error) {
    YDB_LOG_ERROR("",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"error", error});

    Send(TabletActorId, new TEvents::TEvPoison());

    PassAway();
}

void TConsumerActor::ScheduleProcessing() {
    if (ProcessingScheduled) {
        return;
    }

    const bool dlqEmptyOrAlreadyProcessing = DLQMoverActorId || Storage->DLQEmpty();
    if (ReadRequestsQueue.empty() &&
        CommitRequestsQueue.empty() &&
        UnlockRequestsQueue.empty() &&
        ChangeMessageDeadlineRequestsQueue.empty() &&
        PurgeRequestsQueue.empty() &&
        UpdateExternalLockedMessageGroupsIdRequestsQueue.empty() &&
        dlqEmptyOrAlreadyProcessing &&
        Storage->IsBatchEmpty()) {
        return;
    }

    auto now = TInstant::Now();
    TDuration delay = NextProcessingTime > now && dlqEmptyOrAlreadyProcessing
        ? NextProcessingTime - now
        : TDuration::Zero();
    ProcessingScheduled = true;
    Schedule(delay, new TEvents::TEvWakeup(EWakeUpTag::Processing));
}

void TConsumerActor::ProcessEventQueue() {
    YDB_LOG_DEBUG("ProcessEventQueue",
        {"logPrefix", NPQ_LOG_PREFIX});

    NextProcessingTime = TInstant::Now() + TDuration::MilliSeconds(AppData()->PQConfig.GetMLPBatchWindowMilliSeconds());

    for (auto& ev : CommitRequestsQueue) {
        for (auto offset : ev->Get()->Record.GetOffset()) {
            Storage->Commit(offset);
        }

        if (Storage->IsBatchEmpty()) {
            ReplyOk<TEvPQ::TEvMLPCommitResponse>(SelfId(), PartitionId, *ev);
        } else {
            PendingCommitQueue.emplace_back(ev->Sender, ev->Cookie);
        }
    }
    CommitRequestsQueue.clear();

    for (auto& ev : UnlockRequestsQueue) {
        for (auto offset : ev->Get()->Record.GetOffset()) {
            Storage->Unlock(offset);
        }

        if (Storage->IsBatchEmpty()) {
            ReplyOk<TEvPQ::TEvMLPUnlockResponse>(SelfId(), PartitionId, *ev);
        } else {
            PendingUnlockQueue.emplace_back(ev->Sender, ev->Cookie);
        }
    }
    UnlockRequestsQueue.clear();

    for (auto& ev : ChangeMessageDeadlineRequestsQueue) {
        for (const auto &message : ev->Get()->Record.GetMessage()) {
            Storage->ChangeMessageDeadline(message.GetOffset(), TInstant::Seconds(message.GetDeadlineTimestampSeconds()));
        }

        if (Storage->IsBatchEmpty()) {
            ReplyOk<TEvPQ::TEvMLPChangeMessageDeadlineResponse>(SelfId(), PartitionId, *ev);
        } else {
            PendingChangeMessageDeadlineQueue.emplace_back(ev->Sender, ev->Cookie);
        }
    }
    ChangeMessageDeadlineRequestsQueue.clear();

    for (auto& ev : PurgeRequestsQueue) {
        Storage->Purge(PartitionEndOffset);
        PendingPurgeQueue.emplace_back(ev->Sender, ev->Cookie);
    }
    PurgeRequestsQueue.clear();

    for (auto& ev : UpdateExternalLockedMessageGroupsIdRequestsQueue) {
        const NKikimrPQ::TEvMLPUpdateExternalLockedMessageGroupsId& record = ev->Get()->Record;
        auto updateResult = Storage->UpdateExternalLockedMessageGroupsId(record.GetUpdate());
        YDB_LOG_DEBUG("UpdateExternalLockedMessageGroupsId",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"applied", updateResult.Applied},
            {"invalid", updateResult.Invalid},
            {"modeChanged", updateResult.ModeChanged},
            {"setChanged", updateResult.SetChanged},
            {"versionChanged", updateResult.VersionChanged},
            {"#_ShortDebugString(record.GetUpdate())", ShortDebugString(record.GetUpdate())});
        if (updateResult.Applied) {
            ChildPartitionsOrderManager.SetSendFullStateToAll(updateResult.ModeChanged ? TChildPartitionsOrderManager::ESendReasons::ParentChange : TChildPartitionsOrderManager::ESendReasons::Commit, Storage->GetEstimatedLockedMessageGroupsIdSizeFromSelfAndParents());
        }
    }
    UpdateExternalLockedMessageGroupsIdRequestsQueue.clear();

    Storage->ProccessDeadlines();
    YDB_LOG_TRACE("Dump NPQLOGPREFIX, afterDeadlinesDump",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"afterDeadlinesDump", Storage->DebugString()});

    auto now = TInstant::Now();

    TStorage::TPosition position;
    std::deque<TEvPQ::TEvMLPReadRequest::TPtr> readRequestsQueue;
    for (auto& ev : ReadRequestsQueue) {
        size_t count = ev->Get()->GetMaxNumberOfMessages();
        auto visibilityDeadline = ev->Get()->GetProcessingTimeout().ToDeadLine();

        absl::flat_hash_set<ui32> skipMessageGroups; // TODO: remove after SQS migration finished
        skipMessageGroups.reserve(ev->Get()->Record.GetSkipMessageGroup().size());
        for (auto& skipMessageGroup : ev->Get()->Record.GetSkipMessageGroup()) {
            skipMessageGroups.insert(static_cast<ui32>(Hash(skipMessageGroup)) & 0x7FFFFFFF);
        }

        std::deque<TReadMessage> messages;
        for (; count; --count) {
            auto result = Storage->Next(visibilityDeadline, position, skipMessageGroups);
            if (!result) {
                break;
            }

            messages.push_back(std::move(result.value()));
        }

        if (messages.empty() && ev->Get()->GetWaitDeadline() <= now) {
            // Optimization: do not need to upload the message body.
            YDB_LOG_DEBUG("Reply empty result",
                {"logPrefix", NPQ_LOG_PREFIX},
                {"sender", ev->Sender},
                {"cookie", ev->Cookie});
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
    YDB_LOG_DEBUG("Persist",
        {"logPrefix", NPQ_LOG_PREFIX});

    Storage->Compact();

    auto batch = Storage->ExtractBatch();
    if (batch.Empty()) {
        YDB_LOG_DEBUG("Batch is empty",
            {"logPrefix", NPQ_LOG_PREFIX});
        MoveToDLQIfPossible();
        return;
    }

    Become(&TConsumerActor::StateWrite);

    YDB_LOG_TRACE("Dump befor",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"persist", Storage->DebugString()});

    auto tryInlineChannel = [](auto& write) {
        if (write->GetValue().size() < 2048) {
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
        YDB_LOG_DEBUG("Write WAL",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"size", data.size()},
            {"key", key});

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

        YDB_LOG_DEBUG("Delete old",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"WAL", from},
            {"to", to});

        auto* del = request->Record.AddCmdDeleteRange();
        del->MutableRange()->SetFrom(std::move(from));
        del->MutableRange()->SetIncludeFrom(true);
        del->MutableRange()->SetTo(std::move(to));
        del->MutableRange()->SetIncludeTo(true);

        Send(TabletActorId, std::move(request));

        YDB_LOG_DEBUG("Write Snapshot",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"count", Storage->GetMessageCount()},
            {"size", write->GetValue().size()},
            {"cookie", cookie});
    }
}

size_t TConsumerActor::RequiredToFetchMessageCount() const {
    auto& metrics = Storage->GetMetrics();

    auto maxMessages = Storage->HasRetentionExpiredMessages() ? Storage->MaxMessages : Storage->MinMessages;
    if (metrics.UnprocessedMessageCount <= metrics.InflightMessageCount / 4) {
        maxMessages = std::max<size_t>(maxMessages, metrics.InflightMessageCount + metrics.InflightMessageCount / 2);
    }
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
        YDB_LOG_DEBUG("Skip fetch: partition end offset is vs",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"reached", PartitionEndOffset},
            {"#_Storage->GetLastOffset", Storage->GetLastOffset()});
        return false;
    }

    auto& metrics = Storage->GetMetrics();
    if (metrics.InflightMessageCount >= Storage->MaxMessages) {
        YDB_LOG_DEBUG("Skip fetch: infly limit exceeded",
            {"logPrefix", NPQ_LOG_PREFIX});
        return false;
    }
    if (!Config.GetKeepMessageOrder()
        && metrics.InflightMessageCount >= Storage->MinMessages
        && metrics.UnprocessedMessageCount >= metrics.LockedMessageCount * 2
        && metrics.UnprocessedMessageCount >= metrics.InflightMessageCount / 4
        && !Storage->HasRetentionExpiredMessages()) {
        YDB_LOG_DEBUG("Skip fetch: there are enough messages",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"inflightMessageCount", metrics.InflightMessageCount},
            {"unprocessedMessageCount", metrics.UnprocessedMessageCount},
            {"lockedMessageCount", metrics.LockedMessageCount});
        return false;
    }

    FetchInProgress = true;

    auto maxMessages = RequiredToFetchMessageCount();
    YDB_LOG_DEBUG("Fetching messages from offset",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"maxMessages", maxMessages},
        {"#_Storage->GetLastOffset", Storage->GetLastOffset()},
        {"partitionActorId", PartitionActorId});
    Send(TabletActorId, MakeEvPQRead(Config.GetName(), PartitionId, Storage->GetLastOffset(), maxMessages));

    return true;
}

void TConsumerActor::Handle(TEvPQ::TEvProxyResponse::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvProxyResponse",
        {"logPrefix", NPQ_LOG_PREFIX});

    AFL_ENSURE(IsSucess(ev))("e", ev->Get()->Response->DebugString());
}

void TConsumerActor::HandleOnInit(TEvPersQueue::TEvResponse::TPtr& ev) {
    YDB_LOG_DEBUG("Initialized",
        {"logPrefix", NPQ_LOG_PREFIX});
    Become(&TConsumerActor::StateWork);
    Handle(ev);
}

void TConsumerActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPersQueue::TEvResponse",
        {"logPrefix", NPQ_LOG_PREFIX});

    FetchInProgress = false;

    if (!IsSucess(ev)) {
        YDB_LOG_WARN("Fetch messages",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"failed", ev->Get()->Record.DebugString()});
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
        size_t delaySeconds = Config.GetDefaultDelayMessageTimeMs() / 1000;

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

    YDB_LOG_DEBUG("Fetched messages",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"messageCount", messageCount});
    if (allMessagesAdded) {
        FetchMessagesIfNeeded();
    }

    if (messageCount > 0) {
        LastTimeWithMessages = TInstant::Now();
        NotifyPQRB();
    }
    if (CurrentStateFunc() == &TConsumerActor::StateWork) {
        ScheduleProcessing();
    }
}

void TConsumerActor::Handle(TEvPQ::TEvError::TPtr& ev) {
    Restart(TStringBuilder() << "Received error: " << ev->Get()->Error);
}

void TConsumerActor::HandleOnWork(TEvents::TEvWakeup::TPtr& ev) {
    YDB_LOG_DEBUG("HandleOnWork TEvents::TEvWakeup",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"Tag", ev->Get()->Tag});
    switch (ev->Get()->Tag) {
        case EWakeUpTag::Regular: {
            FetchMessagesIfNeeded();
            if (!ProcessingScheduled) {
                ProcessEventQueue();
            }
            NotifyPQRB(true);
            UpdateMetrics();
            Schedule(WakeupInterval, new TEvents::TEvWakeup(EWakeUpTag::Regular));
            break;
        }
        case EWakeUpTag::Processing: {
            ProcessingScheduled = false;
            ProcessEventQueue();
            break;
        }
        case EWakeUpTag::UpdateChildPartitions: {
            UpdateLockedGroupsIdInChildPartitions(false);
            break;
        }
    }
}

void TConsumerActor::MoveToDLQIfPossible() {
    if (DLQMoverActorId) {
        return;
    }

    auto destinationTopic = [&]() -> TString {
        auto databasePrefix = TStringBuilder() << Database << "/";
        if (Config.GetDeadLetterQueue().StartsWith("sqs://") || Config.GetDeadLetterQueue().StartsWith(databasePrefix)) {
            return Config.GetDeadLetterQueue();
        } else {
            return databasePrefix << Config.GetDeadLetterQueue();
        }
    };

    auto messages = Storage->GetDLQMessages();
    if (!messages.empty()) {
        YDB_LOG_DEBUG("Move",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"toDLQ", JoinSeq(", ", messages)});
        DLQMoverActorId = RegisterWithSameMailbox(CreateDLQMover({
            .ParentActorId = SelfId(),
            .Database = Database,
            .TabletId = TabletId,
            .PartitionId = PartitionId,
            .ConsumerName = Config.GetName(),
            .ConsumerGeneration = Config.GetGeneration(),
            .DestinationTopic = destinationTopic(),
            .Messages = std::move(messages)
        }));
    }
}

void TConsumerActor::Handle(TEvPQ::TEvMLPDLQMoverResponse::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvMLPDLQMoverResponse",
        {"logPrefix", NPQ_LOG_PREFIX});

    if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
        YDB_LOG_WARN("Error moving messages to the",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"DLQ", ev->Get()->ErrorDescription});
    }

    auto& moved = ev->Get()->MovedMessages;
    YDB_LOG_DEBUG("Moved to the",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"DLQ", JoinSeq(", ", moved | std::views::transform(AsTDLQMessage))});

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
        ScheduleProcessing();
    }
}

void TConsumerActor::Handle(TEvents::TEvWakeup::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvents::TEvWakeup",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"Tag", ev->Get()->Tag});
    if (ev->Get()->Tag == EWakeUpTag::UpdateChildPartitions) {
        UpdateLockedGroupsIdInChildPartitions(false);
        return;
    }
    NotifyPQRB(true);
    UpdateMetrics();
    Schedule(WakeupInterval, new TEvents::TEvWakeup(EWakeUpTag::Regular));
}

void TConsumerActor::SendToPQTablet(std::unique_ptr<IEventBase> ev) {
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev.release(), TabletId, FirstPipeCacheRequest, static_cast<int>(ESendCookie::SendToPQTablet));
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
    FirstPipeCacheRequest = false;
}

bool TConsumerActor::UseForReading() const {
    if (!Storage->HasUnlockedMessageGroupsId()) {
        return false;
    }
    return LastTimeWithMessages > TInstant::Now() - NoMessagesTimeout || LastCommittedOffset < PartitionEndOffset;
}

void TConsumerActor::NotifyPQRB(bool force) {
    auto useForReading = UseForReading();
    if (force || useForReading != LastUseForReading) {
        auto ev = std::make_unique<TEvPQ::TEvMLPConsumerStatus>(Config.GetName(), PartitionId, useForReading);

        const auto& metrics = Storage->GetMetrics();
        const i64 rawMessageCount = static_cast<i64>(PartitionEndOffset)
            - static_cast<i64>(LastCommittedOffset)
            - static_cast<i64>(metrics.CommittedMessageCount);

        ev->Record.SetLockedMessageCount(metrics.LockedMessageCount);
        ev->Record.SetDelayedMessageCount(metrics.DelayedMessageCount);
        ev->Record.SetMessageCount(std::max<i64>(0, rawMessageCount));

        Send(PartitionActorId, std::move(ev));
        LastUseForReading = useForReading;
    }
}

const NKikimrPQ::TPQTabletConfig::TPartition& TConsumerActor::GetPartitionConfig(ui32 partitionId) const {
    const NKikimrPQ::TPQTabletConfig::TPartition* configPtr = NPQ::GetPartitionConfigFromAllPartitions(TopicConfig, partitionId);
    AFL_ENSURE(configPtr != nullptr)("partitionId", partitionId)("selfPartitionId", PartitionId);
    return *configPtr;
}

const NKikimrPQ::TPQTabletConfig::TPartition& TConsumerActor::GetPartitionConfig() const {
    return GetPartitionConfig(PartitionId);
}

bool TConsumerActor::EnumerateChildrenPartitionsWithKeepOrder() {
    if (!Config.GetKeepMessageOrder()) {
        Y_ASSERT(ChildPartitionsOrderManager.Empty());
        return false;
    }
    const auto& partitionConfig = GetPartitionConfig();
    for (const auto childPartitionId : partitionConfig.GetChildPartitionIds()) {
        if (ChildPartitionsOrderManager.ChildrenPartitionWithKeepOrder.contains(childPartitionId)) {
            continue;
        }
        const auto& childPartitionConfig = GetPartitionConfig(childPartitionId);
        const auto childTabletId = childPartitionConfig.GetTabletId();
        ChildPartitionsOrderManager.ChildrenPartitionWithKeepOrder[childPartitionId] = TChildPartitionsOrderManager::TChildrenPartitionWithKeepOrder{
            .TabletId = childTabletId,
            .Cookie = ChildrenPartitionWithKeepOrderCookie++,
            .SendReasons = {.Reasons = TChildPartitionsOrderManager::ESendReasons::Initial},
        };
    }
    return !ChildPartitionsOrderManager.Empty();
}

void TConsumerActor::UpdateLockedGroupsIdInChildPartitions(bool force) {
    using NKikimrPQ::ETopicPartitionStatus;
    using NKikimrPQ::EReadWithKeepOrder;
    if (!EnumerateChildrenPartitionsWithKeepOrder()) {
        return;
    }
    const bool shouldSend = force || AnyOf(IterateValues(ChildPartitionsOrderManager.ChildrenPartitionWithKeepOrder), &TChildPartitionsOrderManager::TChildrenPartitionWithKeepOrder::NeedSendFullState);
    if (!shouldSend) {
        YDB_LOG_DEBUG("UpdateLockedGroupsIdInChildPartitions no send diff",
            {"logPrefix", NPQ_LOG_PREFIX});
        return;
    }
    ++ChildPartitionsOrderManager.ConsumerStep;
    const auto& partitionConfig = GetPartitionConfig();
    const NKikimrPQ::ETopicPartitionStatus status = partitionConfig.GetStatus();
    Y_ASSERT(status != ETopicPartitionStatus::Active);
    const bool almostAllRead = (PartitionEndOffset <= Storage->GetLastOffset());
    const bool allRead = almostAllRead && (Storage->GetMessageCount() == 0) && Storage->ReadWithKeepOrder() == EReadWithKeepOrder::READ_WITH_KEEP_ORDER_ALLOW_ALL;
    const EReadWithKeepOrder childMode = allRead
        ? EReadWithKeepOrder::READ_WITH_KEEP_ORDER_ALLOW_ALL
        : almostAllRead && ChildPartitionsOrderManager.EnableSendFullBlacklist
        ? EReadWithKeepOrder::READ_WITH_KEEP_ORDER_BLACKLIST
        : EReadWithKeepOrder::READ_WITH_KEEP_ORDER_BLOCK_ALL;
    for (auto& [childPartitionId, state] : ChildPartitionsOrderManager.ChildrenPartitionWithKeepOrder) {
        if (!(force || state.NeedSendFullState())) {
            continue;
        }
        std::unique_ptr ev = std::make_unique<TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId>();
        auto& record = ev->Record;
        record.SetConsumer(Config.GetName());
        record.SetPartitionId(childPartitionId);
        auto* update = record.MutableUpdate();
        update->SetParentPartitionId(PartitionId);
        update->SetGeneration(PartitionGeneration);
        update->SetConsumerGeneration(Config.GetGeneration());
        update->SetStep(ChildPartitionsOrderManager.ConsumerStep);
        update->SetMode(childMode);
        if (childMode == EReadWithKeepOrder::READ_WITH_KEEP_ORDER_BLACKLIST) {
            auto* list = update->MutableFullBlacklist();
            auto append = [list](ui32 lockedHash) {
                list->AddParentLockedMessageGroupsIdHash(lockedHash);
            };
            for (ui32 lockedHash : Storage->GetMessageGroupsIdFromSelf()) {
                append(lockedHash);
            }
            Storage->IterateMessageGroupsIdExclusiveFromParent(append);
        }
        YDB_LOG_DEBUG("UpdateLockedGroupsIdInChildPartitions: updating child partition",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"childPartitionId", childPartitionId},
            {"reason", state.SendFullStateReasonsAsString()},
            {"update", ShortDebugString(record)});
        auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev.release(), state.TabletId, true, state.Cookie);
        Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
        state.MarkAsSent();
    }
}

NActors::IActor* CreateConsumerActor(
    const TString& database,
    ui64 tabletId,
    const NActors::TActorId& tabletActorId,
    ui32 partitionId,
    const NActors::TActorId& partitionActorId,
    ui64 partitionGeneration,
    const NKikimrPQ::TPQTabletConfig& topicConfig,
    const NKikimrPQ::TPQTabletConfig_TConsumer& config,
    const std::optional<TDuration> retention,
    ui64 partitionEndOffset,
    NMonitoring::TDynamicCounterPtr detailedMetricsRoot) {
    return new TConsumerActor(database, tabletId, tabletActorId, partitionId, partitionActorId, partitionGeneration, topicConfig, config, retention, partitionEndOffset, detailedMetricsRoot);
}

}
