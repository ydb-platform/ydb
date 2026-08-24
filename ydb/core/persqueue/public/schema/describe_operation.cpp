#include "describe_operation.h"

#include <ydb/core/base/path.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/events.h>
#include <ydb/core/util/backoff.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <util/string/join.h>

#include <absl/container/flat_hash_set.h>

namespace NKikimr::NPQ::NSchema {

namespace {

template <class T>
void SetProtoTime(T* proto, const ui64 ms) {
    proto->set_seconds(ms / 1000);
    proto->set_nanos((ms % 1000) * 1'000'000);
}

void AddWindowsStat(Ydb::Topic::MultipleWindowsStat* stat, ui64 perMin, ui64 perHour, ui64 perDay) {
    stat->set_per_minute(stat->per_minute() + perMin);
    stat->set_per_hour(stat->per_hour() + perHour);
    stat->set_per_day(stat->per_day() + perDay);
}

class TDescribeOperationActor: public TBaseActor<TDescribeOperationActor>
                             , protected TPipeCacheClient
                             , public TConstantLogPrefix
{
private:
    static constexpr TDuration RequestTimeout = TDuration::Seconds(30);
    static constexpr size_t StatsMaxRetries = 15;
    static constexpr TDuration StatsRetryInitialDelay = TDuration::MilliSeconds(25);
    static constexpr TDuration StatsRetryMaxDelay = TDuration::MilliSeconds(250);
    // Sentinel tags must not collide with tablet ids used for stats-retry wakeups.
    static constexpr ui64 BalancerRetryWakeupTag = Max<ui64>();
    static constexpr ui64 RequestTimeoutWakeupTag = Max<ui64>() - 1;

public:
    TDescribeOperationActor(
        const NActors::TActorId& parent,
        TDescribeOperationSettings&& settings,
        std::unique_ptr<IDescribeStrategy> strategy)
        : TBaseActor<TDescribeOperationActor>(NKikimrServices::EServiceKikimr::PQ_SCHEMA)
        , TPipeCacheClient(this)
        , Parent(parent)
        , Settings(std::move(settings))
        , Strategy(std::move(strategy))
    {
    }

    void Bootstrap() {
        LOG_D("Bootstrap " << Settings.Path);
        RequestStartTime = TActivationContext::Now();
        Schedule(RequestTimeout, new TEvents::TEvWakeup(RequestTimeoutWakeupTag));
        Become(&TDescribeOperationActor::StateDescribe);

        ReadSessionsReceived = !Settings.IncludeStats;
        LocationsReceived = !Settings.IncludeLocation && !Settings.IncludeStats;
        StartDescribe();
    }

    void StartDescribe() {
        LOG_D("StartDescribe path=" << Settings.Path
                                    << " forceSyncVersion=" << Settings.ForceSyncVersion);
        DescriberActorId = RegisterWithSameMailbox(NDescriber::CreateDescriberActor(
            SelfId(),
            CanonizePath(Settings.Database),
            {Settings.Path},
            {
                .UserToken = Settings.UserToken,
                .AccessRights = Settings.AccessRights,
                .ForceSyncVersion = Settings.ForceSyncVersion,
            }));
    }

    TStringBuilder LogBuilder() const {
        return TStringBuilder() << "[" << SelfId() << "]";
    }

    TString BuildLogPrefix() const override {
        return TStringBuilder() << "[" << (Strategy ? Strategy->GetName() : "DescribeOperation") << "]";
    }

    bool OnUnhandledException(const std::exception& exc) override {
        DoLogUnhandledException(Service, NPQ_LOG_PREFIX, exc);
        ReplyWithError(
            Ydb::StatusIds::INTERNAL_ERROR,
            TStringBuilder() << "Unhandled exception: " << exc.what(),
            Ydb::PersQueue::ErrorCode::ERROR);
        return true;
    }

private:
    void PassAway() override {
        LOG_D("PassAway");
        if (DescriberActorId) {
            Send(DescriberActorId, new NActors::TEvents::TEvPoison());
            DescriberActorId = {};
        }
        TPipeCacheClient::Close();
        TBaseActor::PassAway();
    }

    void HandlePoison() {
        ReplyWithError(
            Ydb::StatusIds::CANCELLED,
            "Request was cancelled",
            Ydb::PersQueue::ErrorCode::ERROR);
    }

    void ReplyWithError(
        Ydb::StatusIds::StatusCode status,
        const TString& messageText,
        Ydb::PersQueue::ErrorCode::ErrorCode issueCode = Ydb::PersQueue::ErrorCode::BAD_REQUEST)
    {
        if (IsDead) {
            return;
        }
        auto response = std::make_unique<TEvDescribeOperationResponse>();
        response->Status = status;
        response->ErrorMessage = messageText;
        response->IssueCode = issueCode;
        Send(Parent, response.release());
        IsDead = true;
        PassAway();
    }

    void ReplyWithSuccess() {
        if (IsDead) {
            return;
        }

        auto response = std::make_unique<TEvDescribeOperationResponse>();
        response->Status = Ydb::StatusIds::SUCCESS;
        response->TopicInfo = std::move(TopicInfo);
        response->SelfEntry = std::move(SelfEntry);
        response->Partitions = std::move(Partitions);
        response->ConsumerName = std::move(ConsumerName);
        response->UsedSyncVersion = UsedSyncVersion;
        Send(Parent, response.release());
        IsDead = true;
        PassAway();
    }

    STFUNC(StateDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(TEvPersQueue::TEvGetPartitionsLocationResponse, Handle);
            hFunc(TEvPersQueue::TEvReadSessionsInfoResponse, Handle);
            hFunc(TEvPersQueue::TEvStatusResponse, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == BalancerRetryWakeupTag) {
            HandleBalancerRetryWakeup();
            return;
        }
        if (ev->Get()->Tag == RequestTimeoutWakeupTag) {
            HandleRequestTimeout();
            return;
        }
        HandleStatsRetryWakeup(ev->Get()->Tag);
    }

    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
        DescriberActorId = {};
        TopicInfo = std::move(ev->Get()->Topics.begin()->second);
        UsedSyncVersion = ev->Get()->UsedSyncVersion;
        LOG_D("Handle TEvDescribeTopicsResponse. Status=" << TopicInfo.Status
                                                         << " usedSyncVersion=" << UsedSyncVersion);

        if (TopicInfo.Status != NDescriber::EStatus::SUCCESS) {
            const auto status = [&]() {
                switch (TopicInfo.Status) {
                    case NDescriber::EStatus::NOT_FOUND:
                    case NDescriber::EStatus::NOT_TOPIC:
                    case NDescriber::EStatus::UNAUTHORIZED:
                    case NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
                        return Ydb::StatusIds::SCHEME_ERROR;
                    case NDescriber::EStatus::BAD_REQUEST:
                        return Ydb::StatusIds::BAD_REQUEST;
                    case NDescriber::EStatus::UNKNOWN_ERROR:
                        return Ydb::StatusIds::INTERNAL_ERROR;
                    default:
                        return Ydb::StatusIds::INTERNAL_ERROR;
                }
            }();
            const auto issueCode = [&]() {
                switch (TopicInfo.Status) {
                    case NDescriber::EStatus::NOT_FOUND:
                    case NDescriber::EStatus::UNAUTHORIZED:
                    case NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
                        return Ydb::PersQueue::ErrorCode::ACCESS_DENIED;
                    case NDescriber::EStatus::NOT_TOPIC:
                        return Ydb::PersQueue::ErrorCode::VALIDATION_ERROR;
                    case NDescriber::EStatus::BAD_REQUEST:
                        return Ydb::PersQueue::ErrorCode::BAD_REQUEST;
                    default:
                        return Ydb::PersQueue::ErrorCode::BAD_REQUEST;
                }
            }();

            return ReplyWithError(
                status,
                NDescriber::Description(Settings.Path, TopicInfo.Status),
                issueCode);
        }

        ReadBalancerTabletId = TopicInfo.Info->Description.GetBalancerTabletID();

        auto schemaResult = Strategy->ValidateSchema(TopicInfo);
        if (schemaResult.Error) {
            if (schemaResult.Error->RetryWithSync && !UsedSyncVersion) {
                LOG_D("Schema validation failed without sync version, retrying describe. "
                      << schemaResult.Error->Message);
                Settings.ForceSyncVersion = true;
                StartDescribe();
                return;
            }
            return ReplyWithError(
                schemaResult.Error->Status,
                schemaResult.Error->Message,
                schemaResult.Error->IssueCode);
        }
        ConsumerName = std::move(schemaResult.ConsumerName);

        ConvertDirectoryEntry(TopicInfo.Self->Info, &SelfEntry, true);
        if (TopicInfo.CdcStream) {
            SelfEntry.set_name(std::move(TopicInfo.CdcStreamName));
        }

        RequestReadBalancer();

        if (Settings.IncludeStats) {
            for (const auto& partition : TopicInfo.Info->Description.GetPartitions()) {
                if (!Strategy->NeedProcessPartition(partition)) {
                    continue;
                }
                if (TabletsInflight.contains(partition.GetTabletId())) {
                    continue;
                }
                RequestStats(partition.GetTabletId());
            }
        }

        if (ReplyIfPossible()) {
            return;
        }

        Become(&TDescribeOperationActor::StateWork);
    }

    void Handle(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev) {
        LOG_D("Handle TEvGetPartitionsLocationResponse");
        if (!TabletsInflight.contains(ReadBalancerTabletId)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        if (!record.GetStatus()) {
            LOG_D("PartitionsLocation response status=false");
            ScheduleBalancerRetry();
            return;
        }

        LocationsBackoff.Reset();
        BalancerRetryPending = false;

        for (const auto& location : record.GetLocations()) {
            auto& l = Partitions[location.GetPartitionId()].Location;
            l.set_node_id(location.GetNodeId());
            l.set_generation(location.GetGeneration());
        }

        LocationsReceived = true;

        if (ReadSessionsReceived) {
            TabletsInflight.erase(ReadBalancerTabletId);
            ReplyIfPossible();
        }
    }

    void Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev) {
        const auto tabletId = ev->Cookie;
        LOG_D("Handle TEvStatusResponse. TabletId=" << tabletId);
        if (!TabletsInflight.contains(tabletId)) {
            return;
        }

        auto& record = ev->Get()->Record;
        bool doRestart = record.PartResultSize() == 0;
        for (const auto& partResult : record.GetPartResult()) {
            if (partResult.GetStatus() == NKikimrPQ::TStatusResponse::STATUS_INITIALIZING ||
                partResult.GetStatus() == NKikimrPQ::TStatusResponse::STATUS_UNKNOWN)
            {
                doRestart = true;
                break;
            }
        }
        if (doRestart) {
            LOG_D("StatusResponse requires retry. TabletId=" << tabletId
                                                             << " parts=" << record.PartResultSize());
            ScheduleStatsRetry(tabletId);
            return;
        }

        for (const auto& partResult : record.GetPartResult()) {
            auto& partitionInfo = Partitions[partResult.GetPartition()];
            Ydb::Topic::DescribeConsumerResult::PartitionInfo& partRes = partitionInfo.Stats;
            Ydb::Topic::PartitionStats* partStats = partRes.mutable_partition_stats();

            partStats->set_store_size_bytes(partResult.GetPartitionSize());
            partStats->mutable_partition_offsets()->set_start(partResult.GetStartOffset());
            partStats->mutable_partition_offsets()->set_end(partResult.GetEndOffset());

            SetProtoTime(partStats->mutable_last_write_time(), partResult.GetLastWriteTimestampMs());
            SetProtoTime(partStats->mutable_max_write_time_lag(), partResult.GetWriteLagMs());

            AddWindowsStat(
                partStats->mutable_bytes_written(),
                partResult.GetAvgWriteSpeedPerMin(),
                partResult.GetAvgWriteSpeedPerHour(),
                partResult.GetAvgWriteSpeedPerDay());

            const auto& lagInfo = partResult.GetLagsInfo();
            auto consStats = partRes.mutable_partition_consumer_stats();

            consStats->set_last_read_offset(lagInfo.GetReadPosition().GetOffset());
            consStats->set_committed_offset(lagInfo.GetWritePosition().GetOffset());

            SetProtoTime(consStats->mutable_last_read_time(), lagInfo.GetLastReadTimestampMs());
            SetProtoTime(consStats->mutable_max_read_time_lag(), lagInfo.GetReadLagMs());
            SetProtoTime(consStats->mutable_max_write_time_lag(), lagInfo.GetWriteLagMs());
            SetProtoTime(consStats->mutable_max_committed_time_lag(), lagInfo.GetCommitedLagMs());

            AddWindowsStat(
                consStats->mutable_bytes_read(),
                partResult.GetAvgReadSpeedPerMin(),
                partResult.GetAvgReadSpeedPerHour(),
                partResult.GetAvgReadSpeedPerDay());

            if (const auto consumerCount = partResult.ConsumerResultSize(); consumerCount > 0) {
                partitionInfo.Consumers.reserve(consumerCount);
                for (const auto& cons : partResult.GetConsumerResult()) {
                    auto& consumerStats = partitionInfo.Consumers[cons.GetConsumer()];
                    SetProtoTime(consumerStats.mutable_min_partitions_last_read_time(), cons.GetLastReadTimestampMs());
                    SetProtoTime(consumerStats.mutable_max_read_time_lag(), cons.GetReadLagMs());
                    SetProtoTime(consumerStats.mutable_max_write_time_lag(), cons.GetWriteLagMs());
                    SetProtoTime(consumerStats.mutable_max_committed_time_lag(), cons.GetCommitedLagMs());
                    AddWindowsStat(
                        consumerStats.mutable_bytes_read(),
                        cons.GetAvgReadSpeedPerMin(),
                        cons.GetAvgReadSpeedPerHour(),
                        cons.GetAvgReadSpeedPerDay());
                }
            }
        }

        StatsRetryPending.erase(tabletId);
        TabletsInflight.erase(tabletId);
        ReplyIfPossible();
    }

    void Handle(TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev) {
        LOG_D("Handle TEvReadSessionsInfoResponse");
        if (!TabletsInflight.contains(ReadBalancerTabletId)) {
            return;
        }

        for (auto& partition : *ev->Get()->Record.MutablePartitionInfo()) {
            const auto partitionId = partition.GetPartition();
            Partitions[partitionId].ReadSession = std::move(partition);
        }

        ReadSessionsReceived = true;
        if (LocationsReceived) {
            TabletsInflight.erase(ReadBalancerTabletId);
            ReplyIfPossible();
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        LOG_D("Handle TEvDeliveryProblem. TabletId=" << ev->Get()->TabletId);
        if (!OnUndelivered(ev)) {
            return;
        }

        if (!TabletsInflight.contains(ev->Get()->TabletId)) {
            return;
        }

        if (ev->Get()->TabletId == ReadBalancerTabletId) {
            ScheduleBalancerRetry();
        } else {
            ScheduleStatsRetry(ev->Get()->TabletId);
        }
    }

    bool ReplyIfPossible() {
        if (TabletsInflight.empty()) {
            LOG_D("ReplyWithResult");
            ReplyWithSuccess();
            return true;
        }

        LOG_D("Waiting for tablets inflight: " << JoinSeq(", ", TabletsInflight));
        return false;
    }

    void RequestReadBalancer() {
        if (LocationsReceived && ReadSessionsReceived) {
            return;
        }
        const auto remaining = RemainingRequestTimeout();
        if (!remaining) {
            HandleRequestTimeout();
            return;
        }
        if (!LocationsReceived) {
            TVector<ui64> partitionIds;
            for (const auto& partition : TopicInfo.Info->Description.GetPartitions()) {
                if (Strategy->NeedProcessPartition(partition)) {
                    partitionIds.push_back(partition.GetPartitionId());
                }
            }
            LOG_D("PartitionsLocation " << ReadBalancerTabletId << " partitions " << JoinSeq(", ", partitionIds));
            SendToTablet(
                ReadBalancerTabletId,
                new TEvPersQueue::TEvGetPartitionsLocation(partitionIds, remaining));
        }
        if (!ReadSessionsReceived && Settings.IncludeStats) {
            auto ev = Strategy->CreateReadSessionsInfoRequest();
            if (ev) {
                LOG_D("ReadSessionsInfo " << ReadBalancerTabletId);
                SendToTablet(ReadBalancerTabletId, ev.release());
            } else {
                ReadSessionsReceived = true;
            }
        }
        TabletsInflight.insert(ReadBalancerTabletId);
    }

    void RequestStats(ui64 tabletId) {
        LOG_D("Stats " << tabletId);
        StatsBackoff.try_emplace(tabletId, StatsMaxRetries, StatsRetryInitialDelay, StatsRetryMaxDelay);
        SendToTablet(tabletId, Strategy->CreateStatusRequest().release(), tabletId);
        TabletsInflight.insert(tabletId);
    }

    void ScheduleStatsRetry(ui64 tabletId) {
        if (!RemainingRequestTimeout()) {
            HandleRequestTimeout();
            return;
        }
        auto [it, _] = StatsBackoff.try_emplace(
            tabletId, StatsMaxRetries, StatsRetryInitialDelay, StatsRetryMaxDelay);
        if (!it->second.HasMore()) {
            LOG_W("Stats retries exceeded for tablet " << tabletId);
            StatsRetryPending.erase(tabletId);
            TabletsInflight.erase(tabletId);
            ReplyWithError(
                Ydb::StatusIds::UNAVAILABLE,
                TStringBuilder() << "Tablet " << tabletId << " unresponsive",
                Ydb::PersQueue::ErrorCode::ERROR);
            return;
        }
        if (!StatsRetryPending.insert(tabletId).second) {
            return;
        }
        const auto delay = it->second.Next();
        LOG_D("Stats retry " << tabletId << " " << it->second.GetIteration() << " in " << delay);
        Schedule(delay, new TEvents::TEvWakeup(tabletId));
    }

    bool HandleStatsRetryWakeup(ui64 tabletId) {
        if (!StatsRetryPending.erase(tabletId)) {
            return false;
        }
        if (!TabletsInflight.contains(tabletId)) {
            return true;
        }
        if (!RemainingRequestTimeout()) {
            HandleRequestTimeout();
            return true;
        }
        RequestStats(tabletId);
        return true;
    }

    void ScheduleBalancerRetry() {
        if (!RemainingRequestTimeout()) {
            HandleRequestTimeout();
            return;
        }
        if (LocationsReceived && ReadSessionsReceived) {
            return;
        }
        if (!LocationsBackoff.HasMore()) {
            LOG_W("Balancer retries exceeded");
            TabletsInflight.erase(ReadBalancerTabletId);
            ReplyWithError(
                Ydb::StatusIds::UNAVAILABLE,
                "Partition locations are not available",
                Ydb::PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED);
            return;
        }
        if (BalancerRetryPending) {
            return;
        }
        BalancerRetryPending = true;
        const auto delay = LocationsBackoff.Next();
        LOG_D("Balancer retry " << LocationsBackoff.GetIteration() << " in " << delay
                                << " needLocation=" << !LocationsReceived
                                << " needSessions=" << !ReadSessionsReceived);
        Schedule(delay, new TEvents::TEvWakeup(BalancerRetryWakeupTag));
    }

    void HandleBalancerRetryWakeup() {
        BalancerRetryPending = false;
        if (!TabletsInflight.contains(ReadBalancerTabletId)) {
            return;
        }
        if (LocationsReceived && ReadSessionsReceived) {
            return;
        }
        RequestReadBalancer();
    }

    TDuration RemainingRequestTimeout() const {
        if (!RequestStartTime) {
            return RequestTimeout;
        }
        const auto now = TActivationContext::Now();
        if (now >= *RequestStartTime + RequestTimeout) {
            return TDuration::Zero();
        }
        return *RequestStartTime + RequestTimeout - now;
    }

    void HandleRequestTimeout() {
        LOG_W("Describe request timed out");
        ReplyWithError(
            Ydb::StatusIds::TIMEOUT,
            "Describe request timed out",
            Ydb::PersQueue::ErrorCode::ERROR);
    }

private:
    const NActors::TActorId Parent;
    TDescribeOperationSettings Settings;
    std::unique_ptr<IDescribeStrategy> Strategy;

    NDescriber::TTopicInfo TopicInfo;
    Ydb::Scheme::Entry SelfEntry;
    absl::flat_hash_map<ui32, TPartitionDescribeInfo> Partitions;
    TString ConsumerName;

    ui64 ReadBalancerTabletId = 0;
    absl::flat_hash_set<ui64> TabletsInflight;

    bool LocationsReceived = false;
    bool ReadSessionsReceived = false;
    bool BalancerRetryPending = false;
    bool IsDead = false;
    TBackoff LocationsBackoff = TBackoff(25, TDuration::MilliSeconds(10), TDuration::MilliSeconds(100));
    std::optional<TInstant> RequestStartTime;
    absl::flat_hash_map<ui64, TBackoff> StatsBackoff;
    absl::flat_hash_set<ui64> StatsRetryPending;
    NActors::TActorId DescriberActorId;
    bool UsedSyncVersion = false;
};

} // namespace

NActors::IActor* CreateDescribeOperationActor(
    const NActors::TActorId& parent,
    TDescribeOperationSettings&& settings,
    std::unique_ptr<IDescribeStrategy> strategy)
{
    return new TDescribeOperationActor(parent, std::move(settings), std::move(strategy));
}

} // namespace NKikimr::NPQ::NSchema
