#include "reset_offset_actor.h"

#include <ydb/core/persqueue/public/utils.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ::NResetOffset {

TResetOffsetActor::TResetOffsetActor(const TActorId& parentId, const TResetOffsetSettings& settings)
    : TBaseActor(NKikimrServices::EServiceKikimr::PQ_SCHEMA)
    , ParentId(parentId)
    , Settings(settings)
{
}

void TResetOffsetActor::Bootstrap() {
    DoDescribe();
}

void TResetOffsetActor::DoDescribe() {
    YDB_LOG_DEBUG("Start describe",
        {"logPrefix", NPQ_LOG_PREFIX});
    Become(&TResetOffsetActor::DescribeState);

    NDescriber::TDescribeSettings settings = {
        .UserToken = Settings.UserToken,
        .AccessRights = NACLib::EAccessRights::SelectRow
    };
    ChildActorId = RegisterWithSameMailbox(NDescriber::CreateDescriberActor(
        SelfId(), Settings.DatabasePath, { Settings.TopicName }, settings));
}

void TResetOffsetActor::Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
    YDB_LOG_DEBUG("Handle NDescriber::TEvDescribeTopicsResponse",
        {"logPrefix", NPQ_LOG_PREFIX});

    ChildActorId = {};

    auto& topics = ev->Get()->Topics;
    AFL_ENSURE(topics.size() == 1)("s", topics.size());

    auto& topic = topics.begin()->second;
    switch (topic.Status) {
        case NDescriber::EStatus::SUCCESS: {
            AFL_ENSURE(topic.Info);
            TopicInfo = topic;
            const auto& config = TopicInfo.Info->Description.GetPQTabletConfig();
            auto consumerConfig = GetConsumer(config, Settings.Consumer);
            if (!consumerConfig) {
                const auto converted = NPersQueue::ConvertNewConsumerName(Settings.Consumer, AppData()->PQConfig);
                consumerConfig = GetConsumer(config, converted);
            }
            if (!consumerConfig) {
                return ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "Consumer '" << Settings.Consumer << "' does not exist");
            }
            if (consumerConfig->GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
                return ReplyErrorAndDie(Ydb::StatusIds::BAD_REQUEST,
                    TStringBuilder() << "ResetOffset is not supported for MLP consumer '" << Settings.Consumer << "'");
            }
            ResolvedConsumer = consumerConfig->GetName();
            return DoReset();
        }
        default: {
            auto status = NDescriber::Convert(topic.Status);
            if (status == Ydb::StatusIds::NOT_FOUND) {
                status = Ydb::StatusIds::SCHEME_ERROR;
            }
            ReplyErrorAndDie(status, NDescriber::Description(Settings.TopicName, topic.Status));
        }
    }
}

STFUNC(TResetOffsetActor::DescribeState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TResetOffsetActor::DoReset() {
    YDB_LOG_DEBUG("Start reset",
        {"logPrefix", NPQ_LOG_PREFIX});
    Become(&TResetOffsetActor::ResetState);

    for (auto& partition : TopicInfo.Info->Description.GetPartitions()) {
        auto partitionId = partition.GetPartitionId();
        auto& partitionStatus = Partitions[partitionId] = {
            .TabletId = partition.GetTabletId()
        };
        RequestPartitionIfNeeded(partitionId, partitionStatus);
    }

    ReplyIfPossible();
}

void TResetOffsetActor::Handle(TEvPQ::TEvResetOffsetResponse::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvResetOffsetResponse",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record.ShortDebugString()});

    const ui32 partitionId = ev->Get()->GetPartitionId();
    auto it = Partitions.find(partitionId);
    if (it == Partitions.end()) {
        return;
    }

    auto& partitionStatus = it->second;
    const auto status = ev->Get()->GetStatus();

    // Commit is idempotent: SUCCESS from any in-flight attempt (including a
    // previous retry whose pipe already broke) completes the partition.
    if (status == Ydb::StatusIds::SUCCESS) {
        MarkPartitionSuccess(partitionStatus);
        ReplyIfPossible();
        return;
    }

    const ui64 cookie = ev->Get()->GetCookie() ? ev->Get()->GetCookie() : ev->Cookie;
    if (partitionStatus.Cookie != cookie) {
        return;
    }
    if (partitionStatus.Status != EPartitionStatus::InProgress) {
        return;
    }

    if (status == Ydb::StatusIds::SCHEME_ERROR) {
        ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, TString(ev->Get()->GetErrorMessage()));
        return;
    }

    partitionStatus.ErrorStatus = status;
    partitionStatus.Error = ev->Get()->GetErrorMessage();
    RetryIfPossible(partitionId, partitionStatus);
    ReplyIfPossible();
}

void TResetOffsetActor::RetryIfPossible(ui32 partitionId, TPartitionStatus& partitionStatus) {
    if (partitionStatus.Status == EPartitionStatus::InProgress && !partitionStatus.WaitRetry) {
        --PendingPartitions;
        if (partitionStatus.Backoff.HasMore()) {
            ++PendingRetries;
            partitionStatus.WaitRetry = true;
            Schedule(partitionStatus.Backoff.Next(), new TEvents::TEvWakeup(partitionId));
        } else {
            partitionStatus.Status = EPartitionStatus::Error;
        }
    }
}

void TResetOffsetActor::MarkPartitionSuccess(TPartitionStatus& partitionStatus) {
    if (partitionStatus.Status == EPartitionStatus::Success) {
        return;
    }
    if (partitionStatus.Status == EPartitionStatus::Error) {
        partitionStatus.Status = EPartitionStatus::Success;
        partitionStatus.Error.clear();
        return;
    }
    if (partitionStatus.WaitRetry) {
        partitionStatus.WaitRetry = false;
        --PendingRetries;
    } else if (partitionStatus.Status == EPartitionStatus::InProgress) {
        --PendingPartitions;
    }
    partitionStatus.Status = EPartitionStatus::Success;
}

void TResetOffsetActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPipeCache::TEvDeliveryProblem",
        {"logPrefix", NPQ_LOG_PREFIX});

    auto tabletId = ev->Get()->TabletId;
    // SubscribeCookie of the last TEvForward to this tablet. A delayed
    // DeliveryProblem from an older pipe must not start another retry.
    if (ev->Cookie != TabletCookies[tabletId]) {
        return;
    }
    ++TabletCookies[tabletId];

    for (auto& [partitionId, partitionStatus] : Partitions) {
        if (partitionStatus.TabletId == tabletId) {
            RetryIfPossible(partitionId, partitionStatus);
        }
    }

    ReplyIfPossible();
}

void TResetOffsetActor::Handle(TEvents::TEvWakeup::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvents::TEvWakeup",
        {"logPrefix", NPQ_LOG_PREFIX});

    auto partitionId = ev->Get()->Tag;
    auto it = Partitions.find(partitionId);
    if (it == Partitions.end()) {
        return;
    }

    auto& partitionStatus = it->second;
    if (partitionStatus.Status == EPartitionStatus::InProgress && partitionStatus.WaitRetry) {
        partitionStatus.WaitRetry = false;
        --PendingRetries;
        RequestPartitionIfNeeded(partitionId, partitionStatus);
    }

    ReplyIfPossible();
}

STFUNC(TResetOffsetActor::ResetState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPQ::TEvResetOffsetResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TResetOffsetActor::RequestPartitionIfNeeded(ui32 partitionId, TPartitionStatus& status) {
    if (status.Status == EPartitionStatus::Success || status.Status == EPartitionStatus::Error) {
        return;
    }

    ++PendingPartitions;
    status.Status = EPartitionStatus::InProgress;
    status.Cookie = ++NextCookie;
    status.WaitRetry = false;
    SendToTablet(
        status.TabletId,
        new TEvPQ::TEvResetOffsetRequest(
            Settings.TopicName,
            ResolvedConsumer,
            partitionId,
            Settings.Position,
            Settings.TimestampMs,
            status.Cookie),
        status.Cookie);
}

void TResetOffsetActor::ReplyIfPossible() {
    YDB_LOG_DEBUG("ReplyIfPossible: PendingPartitions PendingRetries",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"pendingPartitions", PendingPartitions},
        {"pendingRetries", PendingRetries});
    if (PendingPartitions > 0 || PendingRetries > 0) {
        return;
    }

    ReplyResultAndDie();
}

void TResetOffsetActor::SendToTablet(ui64 tabletId, IEventBase* ev, ui64 cookie) {
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev, tabletId, true, TabletCookies[tabletId]);
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery, cookie);
}

void TResetOffsetActor::ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
    YDB_LOG_INFO("Reply error",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"statusCodeName", Ydb::StatusIds::StatusCode_Name(errorCode)});
    Send(ParentId, new TEvResetOffsetResult(errorCode, std::move(errorMessage)));
    PassAway();
}

void TResetOffsetActor::ReplyResultAndDie() {
    std::vector<TPartitionResult> results;
    results.reserve(Partitions.size());
    for (const auto& [partitionId, partitionStatus] : Partitions) {
        TPartitionResult result;
        result.PartitionId = partitionId;
        if (partitionStatus.Status == EPartitionStatus::Success) {
            result.Status = Ydb::StatusIds::SUCCESS;
        } else {
            result.Status = partitionStatus.ErrorStatus;
            result.Error = partitionStatus.Error.empty() ? "Failed to reset offset" : partitionStatus.Error;
        }
        results.push_back(std::move(result));
    }

    Send(ParentId, new TEvResetOffsetResult(Ydb::StatusIds::SUCCESS, {}, std::move(results)));
    PassAway();
}

void TResetOffsetActor::PassAway() {
    if (ChildActorId) {
        Send(ChildActorId, new TEvents::TEvPoison());
    }
    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
    TBaseActor::PassAway();
}

bool TResetOffsetActor::OnUnhandledException(const std::exception& exc) {
    Send(ParentId, new TEvResetOffsetResult(Ydb::StatusIds::INTERNAL_ERROR,
        TStringBuilder() << "Unhandled exception: " << exc.what()));
    return TBaseActor::OnUnhandledException(exc);
}

IActor* CreateResetOffsetActor(const NActors::TActorId& parentId, TResetOffsetSettings&& settings) {
    return new TResetOffsetActor(parentId, std::move(settings));
}

} // namespace NKikimr::NPQ::NResetOffset
