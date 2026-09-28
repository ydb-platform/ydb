#include "dq_pq_control_plane_actor.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/providers/pq/common/events.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/sdk/cpp/src/library/issue/yql_issue_message.h>

#include <library/cpp/retry/retry_policy.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_COMPUTE

namespace NYql::NDq {

namespace {

using namespace NActors;

class TDqPqControlPlaneActor final : public TActor<TDqPqControlPlaneActor>, public IActorExceptionHandler {
    using TBase = TActor<TDqPqControlPlaneActor>;
    using TDescribeResult = NPq::NProto::TEvDescribeConsumerResult;
    using TRetryPolicy = IRetryPolicy<>;
    using TReplyKey = std::pair<TActorId, ui64>;

    struct TEvPrivate {
        enum EEv : ui32 {
            EvDescribeFinished = TPqControlPlaneEvents::EvEnd,
            EvRetryError,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvDescribeFinished : TEventLocal<TEvDescribeFinished, EvDescribeFinished> {
            TEvDescribeFinished(TString key, NYdb::NTopic::TAsyncDescribeConsumerResult result)
                : Key(std::move(key))
                , Result(std::move(result))
            {}

            const TString Key;
            const NYdb::NTopic::TAsyncDescribeConsumerResult Result;
        };

        struct TEvRetryError : TEventLocal<TEvRetryError, EvRetryError> {
            explicit TEvRetryError(TReplyKey key)
                : Key(std::move(key))
            {}

            const TReplyKey Key;
        };
    };

    struct TErrorReply {
        TRetryPolicy::IRetryState::TPtr RetryState;
        bool RetryScheduled = false;
    };

    struct TDescription {
        ITopicClient::TPtr Client;
        std::optional<TDescribeResult> Result;
        THashMap<ui64, ui32> PartitionIndexes;
        std::vector<TPqControlPlaneEvents::TEvDescribeConsumer::TPtr> Waiters;
    };

public:
    TDqPqControlPlaneActor(NYdb::TDriver driver, IStructuredTokenCredentialsFactory::TPtr credentialsFactory, IPqStaticGateway::TPtr pqGateway, const THashMap<TString, TString>& secureParams)
        : TBase(&TThis::StateFunc)
        , Driver(std::move(driver))
        , CredentialsFactory(std::move(credentialsFactory))
        , PqGateway(std::move(pqGateway))
        , SecureParams(secureParams)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TPqControlPlaneEvents::TEvDescribeConsumer, Handle);
        hFunc(TEvPrivate::TEvDescribeFinished, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        IgnoreFunc(TEvInterconnect::TEvNodeConnected);
    )

    STRICT_STFUNC(StateError,
        hFunc(TPqControlPlaneEvents::TEvDescribeConsumer, ReplyError);
        hFunc(TEvPrivate::TEvRetryError, Handle);
        hFunc(TEvents::TEvUndelivered, HandleError);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleError);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        IgnoreFunc(TEvPrivate::TEvDescribeFinished);
        IgnoreFunc(TEvInterconnect::TEvNodeConnected);
    )

private:
    bool OnUnhandledException(const std::exception& ex) final {
        Fail(Ydb::StatusIds::INTERNAL_ERROR, ex.what());
        return true;
    }

    void PassAway() final {
        YDB_LOG_DEBUG("[PqControlPlane] Stopping actor", {"actorId", SelfId()});
        UnsubscribeFromSessions();
        TBase::PassAway();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        Fail(Ydb::StatusIds::UNAVAILABLE, TStringBuilder()
            << "Failed to deliver PQ control-plane event to " << ev->Sender
            << ", event type: " << ev->Get()->SourceType << ", reason: " << ev->Get()->Reason);
        ReplyError(ev->Sender, ev->Cookie);
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        Fail(Ydb::StatusIds::UNAVAILABLE, TStringBuilder()
            << "PQ control-plane reader node disconnected: " << ev->Get()->NodeId);
    }

    void HandleError(TEvents::TEvUndelivered::TPtr& ev) {
        YDB_LOG_WARN("[PqControlPlane] Failed to deliver fatal error",
            {"actorId", SelfId()},
            {"readerId", ev->Sender},
            {"cookie", ev->Cookie},
            {"eventType", ev->Get()->SourceType},
            {"reason", ev->Get()->Reason});

        if (ev->Get()->SourceType == TPqControlPlaneEvents::TEvDescribeConsumerResult::EventType) {
            const TReplyKey key{ev->Sender, ev->Cookie};
            ErrorReplies.try_emplace(key);
            ScheduleErrorRetry(key);
        }
    }

    void HandleError(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        YDB_LOG_WARN("[PqControlPlane] Reader node disconnected while delivering fatal error",
            {"actorId", SelfId()},
            {"nodeId", ev->Get()->NodeId});

        for (const auto& [key, _] : ErrorReplies) {
            if (key.first.NodeId() == ev->Get()->NodeId) {
                ScheduleErrorRetry(key);
            }
        }
    }

    void Handle(TEvPrivate::TEvRetryError::TPtr& ev) {
        const auto& key = ev->Get()->Key;
        auto& reply = ErrorReplies.at(key);
        reply.RetryScheduled = false;
        YDB_LOG_DEBUG("[PqControlPlane] Retrying fatal error reply",
            {"actorId", SelfId()},
            {"readerId", key.first},
            {"cookie", key.second});

        Reply(FatalError, key.first, key.second);
    }

    void Handle(TPqControlPlaneEvents::TEvDescribeConsumer::TPtr& ev) {
        CurrentRequest = ev.Release();

        const auto& connection = CurrentRequest->Get()->Record.GetConnection();
        const TString key = connection.SerializeAsString();
        const auto [it, inserted] = Descriptions.try_emplace(key);
        auto& description = it->second;
        YDB_LOG_DEBUG("[PqControlPlane] Consumer description requested",
            {"actorId", SelfId()},
            {"readerId", CurrentRequest->Sender},
            {"cookie", CurrentRequest->Cookie},
            {"endpoint", connection.GetEndpoint()},
            {"database", connection.GetDatabase()},
            {"topic", connection.GetTopicPath()},
            {"consumer", connection.GetConsumerName()},
            {"partitionCount", CurrentRequest->Get()->Record.PartitionIdsSize()},
            {"cached", description.Result.has_value()},
            {"inflight", !inserted && !description.Result});

        if (description.Result) {
            Reply(description, CurrentRequest);
            CurrentRequest.Reset();
            return;
        }

        description.Waiters.emplace_back(CurrentRequest.Release());
        if (!inserted) {
            return;
        }

        auto settings = PqGateway->GetTopicClientSettings();
        settings
            .DiscoveryEndpoint(connection.GetEndpoint())
            .Database(connection.GetDatabase())
            .SslCredentials(NYdb::TSslCredentials(connection.GetUseSsl()))
            .CredentialsProviderFactory(CredentialsFactory->Create(SecureParams.Value(connection.GetTokenName(), TString()), connection.GetAddBearerToToken()));

        YDB_LOG_DEBUG("[PqControlPlane] Describing consumer",
            {"actorId", SelfId()},
            {"endpoint", connection.GetEndpoint()},
            {"database", connection.GetDatabase()},
            {"topic", connection.GetTopicPath()},
            {"consumer", connection.GetConsumerName()});

        description.Client = PqGateway->GetTopicClient(Driver, settings);
        description.Client->DescribeConsumer(
            connection.GetTopicPath(),
            connection.GetConsumerName(),
            NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)
        ).Subscribe([key, selfId = SelfId(), actorSystem = TActivationContext::ActorSystem()](const auto& future) {
            actorSystem->Send(selfId, new TEvPrivate::TEvDescribeFinished(key, future));
        });
    }

    void Handle(TEvPrivate::TEvDescribeFinished::TPtr& ev) {
        const auto& result = ev->Get()->Result.GetValue();
        const auto status = static_cast<int>(result.GetStatus());

        auto& description = Descriptions.at(ev->Get()->Key);
        auto& response = description.Result.emplace();
        response.SetStatus(Ydb::StatusIds::StatusCode_IsValid(status)
            ? static_cast<Ydb::StatusIds::StatusCode>(status)
            : Ydb::StatusIds::EXTERNAL_ERROR);
        NYdb::NIssue::IssuesToMessage(result.GetIssues(), response.MutableIssues());

        if (result.IsSuccess()) {
            for (const auto& partition : result.GetConsumerDescription().GetPartitions()) {
                auto* offsets = response.AddPartitions();
                offsets->SetPartitionId(partition.GetPartitionId());
                if (const auto& stats = partition.GetPartitionStats()) {
                    offsets->SetStartOffset(stats->GetStartOffset());
                }
                if (const auto& stats = partition.GetPartitionConsumerStats()) {
                    offsets->SetCommittedOffset(stats->GetCommittedOffset());
                }
            }
        }

        for (ui32 i = 0; i < description.Result->PartitionsSize(); ++i) {
            description.PartitionIndexes.emplace(description.Result->GetPartitions(i).GetPartitionId(), i);
        }

        YDB_LOG_DEBUG("[PqControlPlane] Consumer description finished",
            {"actorId", SelfId()},
            {"status", Ydb::StatusIds::StatusCode_Name(response.GetStatus())},
            {"issues", result.GetIssues().ToOneLineString()},
            {"partitionCount", response.PartitionsSize()},
            {"waiterCount", description.Waiters.size()});

        for (const auto& waiter : description.Waiters) {
            Reply(description, waiter);
        }

        description.Waiters.clear();
        description.Client.Reset();
    }

    void UnsubscribeFromSessions() {
        for (const auto nodeId : SubscribedNodes) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }
        SubscribedNodes.clear();
    }

    void Reply(const TDescription& description, const TPqControlPlaneEvents::TEvDescribeConsumer::TPtr& request) {
        TDescribeResult record;
        record.SetStatus(description.Result->GetStatus());
        *record.MutableIssues() = description.Result->GetIssues();

        const auto& requestedPartitions = request->Get()->Record.GetPartitionIds();
        record.MutablePartitions()->Reserve(requestedPartitions.size());
        for (const auto partitionId : requestedPartitions) {
            if (const auto it = description.PartitionIndexes.find(partitionId); it != description.PartitionIndexes.end()) {
                *record.AddPartitions() = description.Result->GetPartitions(it->second);
            }
        }

        Reply(std::move(record), request->Sender, request->Cookie);
    }

    void Reply(TDescribeResult result, const TActorId& readerId, ui64 cookie) {
        if (result.GetStatus() == Ydb::StatusIds::SUCCESS) {
            YDB_LOG_DEBUG("[PqControlPlane] Sending consumer description",
                {"actorId", SelfId()},
                {"readerId", readerId},
                {"cookie", cookie},
                {"partitionCount", result.PartitionsSize()});
        } else {
            NYdb::NIssue::TIssues issues;
            NYdb::NIssue::IssuesFromMessage(result.GetIssues(), issues);
            YDB_LOG_ERROR("[PqControlPlane] Sending consumer description error",
                {"actorId", SelfId()},
                {"readerId", readerId},
                {"cookie", cookie},
                {"status", Ydb::StatusIds::StatusCode_Name(result.GetStatus())},
                {"issues", issues.ToOneLineString()});
        }

        auto response = std::make_unique<TPqControlPlaneEvents::TEvDescribeConsumerResult>();
        response->Record = std::move(result);

        TEventFlags flags = IEventHandle::FlagTrackDelivery;
        if (const auto nodeId = readerId.NodeId(); nodeId && nodeId != SelfId().NodeId()) {
            SubscribedNodes.emplace(nodeId);
            flags |= IEventHandle::FlagSubscribeOnSession;
        }

        Send(readerId, response.release(), flags, cookie);
    }

    void ReplyError(TPqControlPlaneEvents::TEvDescribeConsumer::TPtr& request) {
        ReplyError(request->Sender, request->Cookie);
    }

    void ReplyError(const TActorId& readerId, ui64 cookie) {
        ErrorReplies.try_emplace(TReplyKey{readerId, cookie});
        Reply(FatalError, readerId, cookie);
    }

    void ScheduleErrorRetry(const TReplyKey& key) {
        const auto it = ErrorReplies.find(key);
        if (it == ErrorReplies.end() || it->second.RetryScheduled) {
            return;
        }

        auto& reply = it->second;
        if (!reply.RetryState) {
            reply.RetryState = TRetryPolicy::GetExponentialBackoffPolicy(
                [] { return ERetryErrorClass::ShortRetry; },
                /* minDelay */ TDuration::MilliSeconds(100),
                /* minLongRetryDelay */ TDuration::MilliSeconds(100),
                /* maxDelay */ TDuration::Seconds(1)
            )->CreateRetryState();
        }

        const auto delay = reply.RetryState->GetNextRetryDelay();
        Y_VALIDATE(delay, "Failed to schedule fatal error reply retry");

        YDB_LOG_DEBUG("[PqControlPlane] Scheduling fatal error reply retry",
            {"actorId", SelfId()},
            {"readerId", key.first},
            {"cookie", key.second},
            {"delay", *delay});
        reply.RetryScheduled = true;
        Schedule(*delay, new TEvPrivate::TEvRetryError(key));
    }

    void Fail(const Ydb::StatusIds::StatusCode status, const TString& message) {
        YDB_LOG_ERROR("[PqControlPlane] Entering fatal state",
            {"actorId", SelfId()},
            {"status", Ydb::StatusIds::StatusCode_Name(status)},
            {"message", message});

        FatalError.SetStatus(status);
        NYdb::NIssue::IssueToMessage(NYdb::NIssue::TIssue(message), FatalError.AddIssues());

        Become(&TThis::StateError);
        UnsubscribeFromSessions();

        if (CurrentRequest) {
            ReplyError(CurrentRequest);
            CurrentRequest.Reset();
        }

        for (auto& [_, description] : Descriptions) {
            for (auto& waiter : description.Waiters) {
                ReplyError(waiter);
            }
        }

        Descriptions.clear();
    }

    const NYdb::TDriver Driver;
    const IStructuredTokenCredentialsFactory::TPtr CredentialsFactory;
    const IPqStaticGateway::TPtr PqGateway;
    const THashMap<TString, TString> SecureParams;
    TPqControlPlaneEvents::TEvDescribeConsumer::TPtr CurrentRequest;
    THashSet<ui32> SubscribedNodes;
    TDescribeResult FatalError;
    THashMap<TString, TDescription> Descriptions;
    THashMap<TReplyKey, TErrorReply> ErrorReplies;
};

} // anonymous namespace

NActors::IActor* CreateDqPqControlPlaneActor(
    NYdb::TDriver driver,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
    IPqStaticGateway::TPtr pqGateway,
    const THashMap<TString, TString>& secureParams)
{
    return new TDqPqControlPlaneActor(std::move(driver), std::move(credentialsFactory), std::move(pqGateway), secureParams);
}

void RegisterDqPqControlPlaneActorFactory(
    TDqAsyncIoFactory& factory,
    NYdb::TDriver driver,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
    IPqStaticGateway::TPtr pqGateway)
{
    factory.RegisterControlPlane(
        PqControlPlaneActorType,
        [driver = std::move(driver), credentialsFactory = std::move(credentialsFactory), pqGateway = std::move(pqGateway)](IDqAsyncIoFactory::TControlPlaneArguments&& args) {
            return CreateDqPqControlPlaneActor(driver, credentialsFactory, pqGateway, args.SecureParams);
        }
    );
}

} // namespace NYql::NDq
