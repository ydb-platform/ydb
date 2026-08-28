#include "actors.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/aclib/aclib.h>

#include <absl/container/flat_hash_set.h>
#include <util/generic/hash.h>

namespace NKafka {
using namespace NKikimr;
using namespace NKikimr::NPQ;

namespace {

class TTopicOffsetsActor: public TBaseActor<TTopicOffsetsActor>
                        , protected TPipeCacheClient
                        , public TConstantLogPrefix {
    static constexpr TDuration RequestTimeout = TDuration::Seconds(30);
    static constexpr size_t StatusMaxRetries = 15;
    static constexpr TDuration StatusRetryInitialDelay = TDuration::MilliSeconds(25);
    static constexpr TDuration StatusRetryMaxDelay = TDuration::MilliSeconds(250);
    // Retry wakeups use the tablet id as the tag. Timeout is 0, which is never a tablet id.
    static constexpr ui64 TimeoutTag = 0;

public:
    TTopicOffsetsActor(TActorId requester, TTopicOffsetsSettings&& settings)
        : TBaseActor<TTopicOffsetsActor>(NKikimrServices::KAFKA_PROXY)
        , TPipeCacheClient(this)
        , Requester(requester)
        , Settings(std::move(settings))
        , Response(MakeHolder<TEvKafka::TEvTopicOffsetsResponse>())
    {
        RequestedPartitions.insert(Settings.PartitionIds.begin(), Settings.PartitionIds.end());
    }

    void Bootstrap() {
        if (Settings.RequireAuthentication && Settings.Token.empty()) {
            return ReplyError(Ydb::StatusIds::UNAUTHORIZED, "unauthenticated access is forbidden");
        }

        RequestStart = TActivationContext::Now();
        Schedule(RequestTimeout, new TEvents::TEvWakeup(TimeoutTag));
        StartDescribe(/*anonymous=*/false);
        Become(&TTopicOffsetsActor::StateWork);
    }

    TString BuildLogPrefix() const override {
        return TStringBuilder() << "[TTopicOffsetsActor][" << Settings.Path << "]";
    }

    bool OnUnhandledException(const std::exception& exc) override {
        DoLogUnhandledException(Service, NPQ_LOG_PREFIX, exc);
        if (Response) {
            ReplyError(
                Ydb::StatusIds::INTERNAL_ERROR,
                TStringBuilder() << "Unhandled exception: " << exc.what());
        } else {
            PassAway();
        }
        return true;
    }

private:
    void PassAway() override {
        if (DescriberId) {
            Send(DescriberId, new TEvents::TEvPoison());
            DescriberId = {};
        }
        TPipeCacheClient::Close();
        TBaseActor::PassAway();
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& message) {
        if (!Response) {
            return;
        }
        Inflight.clear();
        StatusRetryPending.clear();
        Response->Status = status;
        Response->Issues.AddIssue(message);
        Send(Requester, Response.Release());
        PassAway();
    }

    void ReplySuccess() {
        if (!Response) {
            return;
        }
        Inflight.clear();
        StatusRetryPending.clear();
        Response->Status = Ydb::StatusIds::SUCCESS;
        Send(Requester, Response.Release());
        PassAway();
    }

    TDuration Remaining() const {
        const auto deadline = RequestStart + RequestTimeout;
        const auto now = TActivationContext::Now();
        return now >= deadline ? TDuration::Zero() : deadline - now;
    }

    void StartDescribe(bool anonymous) {
        TIntrusiveConstPtr<NACLib::TUserToken> userToken;
        if (!anonymous && !Settings.Token.empty()) {
            userToken = new NACLib::TUserToken(Settings.Token);
        }
        DescriberId = RegisterWithSameMailbox(NDescriber::CreateDescriberActor(
            SelfId(),
            CanonizePath(Settings.Database),
            {Settings.Path},
            {
                .UserToken = userToken,
                .AccessRights = NACLib::EAccessRights::DescribeSchema,
            }));
    }

    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
        DescriberId = {};
        const auto it = ev->Get()->Topics.find(Settings.Path);
        AFL_ENSURE(it != ev->Get()->Topics.end())("path", Settings.Path);
        const auto& topicInfo = it->second;
        if (topicInfo.Status != NDescriber::EStatus::SUCCESS) {
            return HandleDescribeError(topicInfo);
        }

        if (DidUnauthenticatedExistenceCheck) {
            // Anonymous describe found the topic: the user was denied by ACL.
            return ReplyError(Ydb::StatusIds::UNAUTHORIZED, "access denied");
        }

        AFL_ENSURE(topicInfo.Info);

        const TString& selectRowToken = !Settings.SelectRowToken.empty()
            ? Settings.SelectRowToken
            : Settings.Token;
        // Anonymous access (no token) skips SelectRow, matching the previous OffsetFetch actor.
        if (Settings.RequireSelectRow && !selectRowToken.empty()) {
            if (!topicInfo.SecurityObject) {
                return ReplyError(
                    Ydb::StatusIds::INTERNAL_ERROR,
                    TStringBuilder() << "Missing security object for " << Settings.Path);
            }
            TIntrusiveConstPtr<NACLib::TUserToken> userToken = new NACLib::TUserToken(selectRowToken);
            if (!topicInfo.SecurityObject->CheckAccess(NACLib::EAccessRights::SelectRow, *userToken)) {
                return ReplyError(Ydb::StatusIds::UNAUTHORIZED, "unauthenticated access is forbidden");
            }
        }

        const auto& description = topicInfo.Info->Description;
        absl::flat_hash_set<ui32> foundPartitions;
        for (const auto& partition : description.GetPartitions()) {
            const auto partitionId = partition.GetPartitionId();
            if (!RequestedPartitions.empty() && !RequestedPartitions.contains(partitionId)) {
                continue;
            }
            foundPartitions.insert(partitionId);
            Tablets.insert(partition.GetTabletId());
        }

        if (!RequestedPartitions.empty() && Settings.Consumers.empty()) {
            for (auto partitionId : RequestedPartitions) {
                if (!foundPartitions.contains(partitionId)) {
                    return ReplyError(
                        Ydb::StatusIds::SCHEME_ERROR,
                        TStringBuilder() << "No partition " << partitionId << " in topic");
                }
            }
        }

        if (Tablets.empty()) {
            return ReplySuccess();
        }

        for (auto tabletId : Tablets) {
            RequestStatus(tabletId);
            if (!Response) {
                return;
            }
        }
    }

    void HandleDescribeError(const NDescriber::TTopicInfo& topicInfo) {
        auto status = NDescriber::Convert(topicInfo.Status);
        if (Settings.UnauthenticatedExistenceCheck && !Settings.Token.empty()) {
            if (!DidUnauthenticatedExistenceCheck &&
                topicInfo.Status == NDescriber::EStatus::UNAUTHORIZED)
            {
                DidUnauthenticatedExistenceCheck = true;
                StartDescribe(/*anonymous=*/true);
                return;
            }
            if (DidUnauthenticatedExistenceCheck) {
                // Anonymous describe also failed: topic does not exist.
                return ReplyError(
                    Ydb::StatusIds::SCHEME_ERROR,
                    NDescriber::Description(Settings.Path, topicInfo.Status));
            }
        }
        if (status == Ydb::StatusIds::NOT_FOUND) {
            status = Ydb::StatusIds::SCHEME_ERROR;
        }
        return ReplyError(status, NDescriber::Description(Settings.Path, topicInfo.Status));
    }

    std::unique_ptr<TEvPersQueue::TEvStatus> MakeStatusRequest() const {
        auto ev = std::make_unique<TEvPersQueue::TEvStatus>();
        for (const auto& consumer : Settings.Consumers) {
            ev->Record.AddConsumers(consumer);
        }
        return ev;
    }

    void RequestStatus(ui64 tabletId) {
        if (!Remaining()) {
            return ReplyError(Ydb::StatusIds::TIMEOUT, "Request timed out");
        }
        StatusBackoff.try_emplace(tabletId, StatusMaxRetries, StatusRetryInitialDelay, StatusRetryMaxDelay);
        SendToTablet(tabletId, MakeStatusRequest().release(), tabletId);
        Inflight.insert(tabletId);
    }

    void Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev) {
        if (!Response) {
            return;
        }
        const auto tabletId = ev->Cookie;
        if (!Inflight.contains(tabletId)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        bool retry = record.PartResultSize() == 0;
        for (const auto& partResult : record.GetPartResult()) {
            if (partResult.GetStatus() == NKikimrPQ::TStatusResponse::STATUS_INITIALIZING ||
                partResult.GetStatus() == NKikimrPQ::TStatusResponse::STATUS_UNKNOWN)
            {
                retry = true;
                break;
            }
        }
        if (retry) {
            return ScheduleStatusRetry(tabletId);
        }

        AFL_ENSURE(Response);
        for (const auto& part : record.GetPartResult()) {
            const auto partitionId = static_cast<ui32>(part.GetPartition());
            if (!RequestedPartitions.empty() && !RequestedPartitions.contains(partitionId)) {
                continue;
            }

            TEvKafka::TPartitionOffsetsInfo info;
            info.PartitionId = partitionId;
            info.StartOffset = part.GetStartOffset();
            info.EndOffset = part.GetEndOffset();
            info.Generation = part.GetGeneration();
            for (const auto& consumerResult : part.GetConsumerResult()) {
                if (consumerResult.GetErrorCode() != NPersQueue::NErrorCode::OK) {
                    continue;
                }
                std::optional<TString> metadata = consumerResult.HasCommittedMetadata()
                    ? std::optional<TString>(consumerResult.GetCommittedMetadata())
                    : std::nullopt;
                info.Consumers.emplace(
                    consumerResult.GetConsumer(),
                    TEvKafka::PartitionConsumerOffset{
                        static_cast<ui64>(partitionId),
                        static_cast<ui64>(consumerResult.GetCommitedOffset()),
                        metadata});
            }
            Response->Partitions.push_back(std::move(info));
        }

        StatusRetryPending.erase(tabletId);
        Inflight.erase(tabletId);
        if (Inflight.empty()) {
            ReplySuccess();
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        if (!OnUndelivered(ev) || !Inflight.contains(ev->Get()->TabletId)) {
            return;
        }
        ScheduleStatusRetry(ev->Get()->TabletId);
    }

    void ScheduleStatusRetry(ui64 tabletId) {
        if (!Remaining()) {
            return ReplyError(Ydb::StatusIds::TIMEOUT, "Request timed out");
        }
        auto [it, _] = StatusBackoff.try_emplace(
            tabletId, StatusMaxRetries, StatusRetryInitialDelay, StatusRetryMaxDelay);
        if (!it->second.HasMore()) {
            Inflight.erase(tabletId);
            StatusRetryPending.erase(tabletId);
            return ReplyError(
                Ydb::StatusIds::UNAVAILABLE,
                TStringBuilder() << "Tablet " << tabletId << " unresponsive");
        }
        if (!StatusRetryPending.insert(tabletId).second) {
            return;
        }
        Schedule(it->second.Next(), new TEvents::TEvWakeup(tabletId));
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == TimeoutTag) {
            return ReplyError(Ydb::StatusIds::TIMEOUT, "Request timed out");
        }
        const auto tabletId = ev->Get()->Tag;
        if (!StatusRetryPending.erase(tabletId) || !Inflight.contains(tabletId)) {
            return;
        }
        RequestStatus(tabletId);
    }

    void HandlePoison() {
        ReplyError(Ydb::StatusIds::CANCELLED, "Request was cancelled");
    }

    STRICT_STFUNC(StateWork,
        hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
        hFunc(TEvPersQueue::TEvStatusResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        cFunc(TEvents::TEvPoison::EventType, HandlePoison);
    )

    TActorId Requester;
    TTopicOffsetsSettings Settings;
    THolder<TEvKafka::TEvTopicOffsetsResponse> Response;
    TActorId DescriberId;
    bool DidUnauthenticatedExistenceCheck = false;
    TInstant RequestStart;
    absl::flat_hash_set<ui32> RequestedPartitions;
    absl::flat_hash_set<ui64> Tablets;
    absl::flat_hash_set<ui64> Inflight;
    absl::flat_hash_set<ui64> StatusRetryPending;
    THashMap<ui64, TBackoff> StatusBackoff;
};

} // namespace

NActors::IActor* CreateTopicOffsetsActor(
    const NActors::TActorId& requester,
    TTopicOffsetsSettings settings)
{
    return new TTopicOffsetsActor(requester, std::move(settings));
}

} // namespace NKafka
