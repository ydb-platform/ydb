#include "set_offsets_actor.h"

#include <ydb/core/persqueue/public/utils.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ::NSetOffsets {

TSetOffsetsActor::TSetOffsetsActor(const TActorId& parentId, const TSetOffsetsSettings& settings)
    : TBaseActor(NKikimrServices::EServiceKikimr::PQ_SCHEMA)
    , ParentId(parentId)
    , Settings(settings)
{
}

void TSetOffsetsActor::Bootstrap() {
    DoDescribe();
}

void TSetOffsetsActor::DoDescribe() {
    YDB_LOG_DEBUG("Start describe",
        {"logPrefix", NPQ_LOG_PREFIX});
    Become(&TSetOffsetsActor::DescribeState);

    NDescriber::TDescribeSettings settings = {
        .UserToken = Settings.UserToken,
        .AccessRights = NACLib::EAccessRights::SelectRow
    };
    ChildActorId = RegisterWithSameMailbox(NDescriber::CreateDescriberActor(
        SelfId(), Settings.DatabasePath, { Settings.TopicName }, settings));
}

void TSetOffsetsActor::Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
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
                    TStringBuilder() << "SetOffsets is not supported for MLP consumer '" << Settings.Consumer << "'");
            }
            ResolvedConsumer = consumerConfig->GetName();
            return DoSet();
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

STFUNC(TSetOffsetsActor::DescribeState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TSetOffsetsActor::DoSet() {
    YDB_LOG_DEBUG("Start reset",
        {"logPrefix", NPQ_LOG_PREFIX});
    Become(&TSetOffsetsActor::SetState);

    for (auto& partition : TopicInfo.Info->Description.GetPartitions()) {
        auto partitionId = partition.GetPartitionId();
        auto& partitionStatus = Partitions[partitionId] = {
            .TabletId = partition.GetTabletId()
        };
        RequestPartitionIfNeeded(partitionId, partitionStatus);
    }

    ReplyIfPossible();
}

void TSetOffsetsActor::Handle(TEvPQ::TEvSetOffsetsResponse::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvSetOffsetsResponse",
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

    const ui64 cookie = ev->Get()->Record.HasCookie() ? ev->Get()->GetCookie() : ev->Cookie;
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

void TSetOffsetsActor::RetryIfPossible(ui32 partitionId, TPartitionStatus& partitionStatus) {
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

void TSetOffsetsActor::MarkPartitionSuccess(TPartitionStatus& partitionStatus) {
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

void TSetOffsetsActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPipeCache::TEvDeliveryProblem",
        {"logPrefix", NPQ_LOG_PREFIX});

    auto tabletId = ev->Get()->TabletId;
    auto cookieIt = TabletCookies.find(tabletId);
    if (cookieIt == TabletCookies.end()) {
        return;
    }
    // SubscribeCookie of the last TEvForward to this tablet. Pipe cache echoes
    // that value as TEvDeliveryProblem::Cookie. A delayed DeliveryProblem from
    // an older pipe must not start another retry.
    if (ev->Cookie != cookieIt->second) {
        return;
    }
    ++cookieIt->second;

    for (auto& [partitionId, partitionStatus] : Partitions) {
        if (partitionStatus.TabletId == tabletId) {
            RetryIfPossible(partitionId, partitionStatus);
        }
    }

    ReplyIfPossible();
}

void TSetOffsetsActor::Handle(TEvents::TEvWakeup::TPtr& ev) {
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

STFUNC(TSetOffsetsActor::SetState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPQ::TEvSetOffsetsResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TSetOffsetsActor::RequestPartitionIfNeeded(ui32 partitionId, TPartitionStatus& status) {
    if (status.Status == EPartitionStatus::Success || status.Status == EPartitionStatus::Error) {
        return;
    }

    ++PendingPartitions;
    status.Status = EPartitionStatus::InProgress;
    status.Cookie = ++NextCookie;
    status.WaitRetry = false;
    SendToTablet(
        status.TabletId,
        new TEvPQ::TEvSetOffsetsRequest(
            Settings.TopicName,
            ResolvedConsumer,
            partitionId,
            Settings.Position,
            Settings.TimestampMs,
            status.Cookie),
        status.Cookie);
}

void TSetOffsetsActor::ReplyIfPossible() {
    YDB_LOG_DEBUG("ReplyIfPossible: PendingPartitions PendingRetries",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"pendingPartitions", PendingPartitions},
        {"pendingRetries", PendingRetries});
    if (PendingPartitions > 0 || PendingRetries > 0) {
        return;
    }

    ReplyResultAndDie();
}

void TSetOffsetsActor::SendToTablet(ui64 tabletId, IEventBase* ev, ui64 cookie) {
    // SubscribeCookie is what TEvPipeCache puts on TEvDeliveryProblem::Cookie.
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev, tabletId, true, TabletCookies[tabletId]);
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery, cookie);
}

void TSetOffsetsActor::ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
    YDB_LOG_INFO("Reply error",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"statusCodeName", Ydb::StatusIds::StatusCode_Name(errorCode)});
    Send(ParentId, new TEvSetOffsetsResult(errorCode, std::move(errorMessage)));
    PassAway();
}

void TSetOffsetsActor::ReplyResultAndDie() {
    std::vector<TPartitionResult> results;
    results.reserve(Partitions.size());
    for (const auto& [partitionId, partitionStatus] : Partitions) {
        TPartitionResult result;
        result.PartitionId = partitionId;
        if (partitionStatus.Status == EPartitionStatus::Success) {
            result.Status = Ydb::StatusIds::SUCCESS;
        } else {
            result.Status = partitionStatus.ErrorStatus;
            result.Error = partitionStatus.Error.empty() ? "Failed to set offsets" : partitionStatus.Error;
        }
        results.push_back(std::move(result));
    }
    std::sort(results.begin(), results.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.PartitionId < rhs.PartitionId;
    });

    Send(ParentId, new TEvSetOffsetsResult(Ydb::StatusIds::SUCCESS, {}, std::move(results)));
    PassAway();
}

void TSetOffsetsActor::PassAway() {
    if (ChildActorId) {
        Send(ChildActorId, new TEvents::TEvPoison());
    }
    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
    TBaseActor::PassAway();
}

bool TSetOffsetsActor::OnUnhandledException(const std::exception& exc) {
    Send(ParentId, new TEvSetOffsetsResult(Ydb::StatusIds::INTERNAL_ERROR,
        TStringBuilder() << "Unhandled exception: " << exc.what()));
    return TBaseActor::OnUnhandledException(exc);
}

IActor* CreateSetOffsetsActor(const NActors::TActorId& parentId, TSetOffsetsSettings&& settings) {
    return new TSetOffsetsActor(parentId, std::move(settings));
}

} // namespace NKikimr::NPQ::NSetOffsets
