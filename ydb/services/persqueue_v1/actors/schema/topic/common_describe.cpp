#include "common_describe.h"

namespace NKikimr::NGRpcProxy::V1::NTopic {

TDescribeOperationActor::TDescribeOperationActor(
    const NActors::TActorId& parent,
    TDescribeSettings&& settings,
    std::unique_ptr<IDescribeStrategy> strategy)
    : NPQ::TPipeCacheClient(this)
    , Parent(parent)
    , Settings(std::move(settings))
    , Strategy(std::move(strategy))
{
}

void TDescribeOperationActor::Bootstrap() {
    LOG_D("Bootstrap " << Settings.Path);
    RequestStartTime = TActivationContext::Now();
    Schedule(RequestTimeout, new TEvents::TEvWakeup(RequestTimeoutWakeupTag));

    DescriberActorId = RegisterWithSameMailbox(NPQ::NDescriber::CreateDescriberActor(
        SelfId(),
        CanonizePath(Settings.Database),
        { Settings.Path },
        {
            .UserToken = Settings.UserToken,
            .AccessRights = Settings.AccessRights,
        }
    ));
    Become(&TDescribeOperationActor::StateDescribe);

    ReadSessionsReceived = !Settings.IncludeStats;
    LocationsReceived = !Settings.IncludeLocation && !Settings.IncludeStats;
}

TStringBuilder TDescribeOperationActor::LogBuilder() const {
    return TStringBuilder() << "[" << SelfId() << "]";
}

TString TDescribeOperationActor::BuildLogPrefix() const {
    return TStringBuilder() << "[" << (Strategy ? Strategy->GetName() : "DescribeOperation") << "]";
}

void TDescribeOperationActor::PassAway() {
    LOG_D("PassAway");
    if (DescriberActorId) {
        Send(DescriberActorId, new NActors::TEvents::TEvPoison());
        DescriberActorId = {};
    }
    NPQ::TPipeCacheClient::Close();
    TActorBootstrapped::PassAway();
}

void TDescribeOperationActor::HandlePoison() {
    ReplyWithError(
        Ydb::StatusIds::CANCELLED,
        "Request was cancelled",
        Ydb::PersQueue::ErrorCode::ERROR);
}

void TDescribeOperationActor::ReplyWithError(
    Ydb::StatusIds::StatusCode status,
    const TString& messageText,
    Ydb::PersQueue::ErrorCode::ErrorCode issueCode)
{
    if (IsDead) {
        return;
    }
    auto response = std::make_unique<TEvDescribeResponse>();
    response->Status = status;
    response->ErrorMessage = messageText;
    response->IssueCode = issueCode;
    Send(Parent, response.release());
    IsDead = true;
    PassAway();
}

STFUNC(TDescribeOperationActor::StateDescribe) {
    switch (ev->GetTypeRewrite()) {
        hFunc(NPQ::NDescriber::TEvDescribeTopicsResponse, Handle);
        cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
    case TEvents::TEvWakeup::EventType:
        if (ev->Get<TEvents::TEvWakeup>()->Tag == RequestTimeoutWakeupTag) {
            HandleRequestTimeout();
            return;
        }
        break;
    }
}

void TDescribeOperationActor::Handle(NPQ::NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
    DescriberActorId = {};
    TopicInfo = std::move(ev->Get()->Topics.begin()->second);
    LOG_D("Handle TEvDescribeTopicsResponse. Status=" << TopicInfo.Status);

    if (TopicInfo.Status != NPQ::NDescriber::EStatus::SUCCESS) {
        const auto status = [&]() {
            switch (TopicInfo.Status) {
                case NPQ::NDescriber::EStatus::NOT_FOUND:
                case NPQ::NDescriber::EStatus::NOT_TOPIC:
                case NPQ::NDescriber::EStatus::UNAUTHORIZED:
                case NPQ::NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
                    return Ydb::StatusIds::SCHEME_ERROR;
                case NPQ::NDescriber::EStatus::UNKNOWN_ERROR:
                    return Ydb::StatusIds::INTERNAL_ERROR;
                default:
                    return Ydb::StatusIds::INTERNAL_ERROR;
            }
        }();
        const auto issueCode = [&]() {
            switch (TopicInfo.Status) {
                case NPQ::NDescriber::EStatus::NOT_FOUND:
                case NPQ::NDescriber::EStatus::UNAUTHORIZED:
                case NPQ::NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
                    return Ydb::PersQueue::ErrorCode::ACCESS_DENIED;
                case NPQ::NDescriber::EStatus::NOT_TOPIC:
                    return Ydb::PersQueue::ErrorCode::VALIDATION_ERROR;
                default:
                    return Ydb::PersQueue::ErrorCode::BAD_REQUEST;
            }
        }();

        return ReplyWithError(
            status,
            NPQ::NDescriber::Description(Settings.Path, TopicInfo.Status),
            issueCode);
    }

    ReadBalancerTabletId = TopicInfo.Info->Description.GetBalancerTabletID();

    auto schemaResult = Strategy->ValidateSchema(TopicInfo);
    if (schemaResult.Error) {
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

STFUNC(TDescribeOperationActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        hFunc(TEvPersQueue::TEvGetPartitionsLocationResponse, Handle);
        hFunc(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse, Handle);
        hFunc(NKikimr::TEvPersQueue::TEvStatusResponse, Handle);
        cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
    case TEvents::TEvWakeup::EventType:
        if (ev->Get<TEvents::TEvWakeup>()->Tag == BalancerRetryWakeupTag) {
            HandleBalancerRetryWakeup();
            return;
        }
        if (ev->Get<TEvents::TEvWakeup>()->Tag == RequestTimeoutWakeupTag) {
            HandleRequestTimeout();
            return;
        }
        if (HandleStatsRetryWakeup(ev->Get<TEvents::TEvWakeup>()->Tag)) {
            return;
        }
        break;
    }
}

void TDescribeOperationActor::Handle(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev) {
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

void TDescribeOperationActor::Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev) {
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
        Ydb::Topic::DescribeConsumerResult::PartitionInfo& partRes = Partitions[partResult.GetPartition()].Stats;
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
            partResult.GetAvgWriteSpeedPerDay()
        );

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
            partResult.GetAvgReadSpeedPerDay()
        );
    }

    StatsRetryPending.erase(tabletId);
    TabletsInflight.erase(tabletId);
    ReplyIfPossible();
}

void TDescribeOperationActor::Handle(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev) {
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

void TDescribeOperationActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
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

bool TDescribeOperationActor::ReplyIfPossible() {
    if (TabletsInflight.empty()) {
        LOG_D("ReplyWithResult");
        ReplyWithSuccess();
        return true;
    }

    LOG_D("Waiting for tablets inflight: " << JoinSeq(", ", TabletsInflight));
    return false;
}

void TDescribeOperationActor::ReplyWithSuccess() {
    if (IsDead) {
        return;
    }

    auto response = std::make_unique<TEvDescribeResponse>();
    response->Status = Ydb::StatusIds::SUCCESS;
    response->TopicInfo = std::move(TopicInfo);
    response->SelfEntry = std::move(SelfEntry);
    response->Partitions = std::move(Partitions);
    response->ConsumerName = std::move(ConsumerName);
    Send(Parent, response.release());
    IsDead = true;
    PassAway();
}

void TDescribeOperationActor::RequestReadBalancer() {
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

void TDescribeOperationActor::RequestStats(ui64 tabletId) {
    LOG_D("Stats " << tabletId);
    StatsBackoff.try_emplace(tabletId, StatsMaxRetries, StatsRetryInitialDelay, StatsRetryMaxDelay);
    SendToTablet(tabletId, Strategy->CreateStatusRequest().release(), tabletId);
    TabletsInflight.insert(tabletId);
}

void TDescribeOperationActor::ScheduleStatsRetry(ui64 tabletId) {
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

bool TDescribeOperationActor::HandleStatsRetryWakeup(ui64 tabletId) {
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

void TDescribeOperationActor::ScheduleBalancerRetry() {
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

void TDescribeOperationActor::HandleBalancerRetryWakeup() {
    BalancerRetryPending = false;
    if (!TabletsInflight.contains(ReadBalancerTabletId)) {
        return;
    }
    if (LocationsReceived && ReadSessionsReceived) {
        return;
    }
    RequestReadBalancer();
}

TDuration TDescribeOperationActor::RemainingRequestTimeout() const {
    if (!RequestStartTime) {
        return RequestTimeout;
    }
    const auto now = TActivationContext::Now();
    if (now >= *RequestStartTime + RequestTimeout) {
        return TDuration::Zero();
    }
    return *RequestStartTime + RequestTimeout - now;
}

void TDescribeOperationActor::HandleRequestTimeout() {
    LOG_W("Describe request timed out");
    ReplyWithError(
        Ydb::StatusIds::TIMEOUT,
        "Describe request timed out",
        Ydb::PersQueue::ErrorCode::ERROR);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
