#include "read_rule_deleter.h"

#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::STREAMS, QueryId << ": " << stream)

#define LOG_I(stream) \
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::STREAMS, QueryId << ": " << stream)

#define LOG_D(stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::STREAMS, QueryId << ": " << stream)

namespace NFq {
namespace {

using namespace NActors;

struct TEvPrivate {
    // Event ids.
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        EvSingleReadRuleDeleterResult = EvBegin,
        EvRemoveReadRuleStatus,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events.

    struct TEvSingleReadRuleDeleterResult : TEventLocal<TEvSingleReadRuleDeleterResult, EvSingleReadRuleDeleterResult> {
        TEvSingleReadRuleDeleterResult() = default;

        explicit TEvSingleReadRuleDeleterResult(const NYql::TIssues& issues)
            : Issues(issues)
        {
        }

        NYql::TIssues Issues;
    };

    struct TEvRemoveReadRuleStatus : TEventLocal<TEvRemoveReadRuleStatus, EvRemoveReadRuleStatus> {
        TEvRemoveReadRuleStatus(NYdb::TStatus status)
            : Status(std::move(status))
        {
        }

        NYdb::TStatus Status;
    };
};

// Actor for deletion of read rule for one topic.
class TSingleReadRuleDeleter : public TActorBootstrapped<TSingleReadRuleDeleter> {
public:
    TSingleReadRuleDeleter(
        NActors::TActorId owner,
        TString queryId,
        NYdb::TDriver ydbDriver,
        Fq::Private::TopicConsumer topic,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProvider,
        ui64 index,
        size_t maxRetries
    )
        : Owner(owner)
        , QueryId(std::move(queryId))
        , Topic(std::move(topic))
        , YdbDriver(std::move(ydbDriver))
        , PqClient(YdbDriver, GetPqClientSettings(std::move(credentialsProvider)))
        , Index(index)
        , MaxRetries(maxRetries)
    {
    }

    static constexpr char ActorName[] = "YQ_SINGLE_READ_RULE_DELETER";

    void Bootstrap() {
        Become(&TSingleReadRuleDeleter::StateFunc);
        StartRequest();
    }

    TString GetTopicPath() const {
        TStringBuilder ret;
        ret << Topic.database();
        if (ret && ret.back() != '/') {
            ret << '/';
        }
        ret << Topic.topic_path();
        return std::move(ret);
    }

    void StartRequest() {
        LOG_D("Make request for read rule deletion for topic `" << Topic.topic_path() << "` [" << Index << "]");
        PqClient.RemoveReadRule(
            GetTopicPath(),
            NYdb::NPersQueue::TRemoveReadRuleSettings()
                .ConsumerName(Topic.consumer_name())
        ).Subscribe(
            [actorSystem = TActivationContext::ActorSystem(), selfId = SelfId()](const NYdb::TAsyncStatus& status) {
                actorSystem->Send(selfId, new TEvPrivate::TEvRemoveReadRuleStatus(status.GetValue()));
            }
        );
    }

    void Handle(TEvPrivate::TEvRemoveReadRuleStatus::TPtr& ev) {
        const NYdb::TStatus& status = ev->Get()->Status;
        if (status.IsSuccess() || status.GetStatus() == NYdb::EStatus::NOT_FOUND) {
            Send(Owner, MakeHolder<TEvPrivate::TEvSingleReadRuleDeleterResult>(), 0, Index);
            PassAway();
        } else {
            if (!RetryState) {
                // Choose default retry policy arguments from persqueue.h except maxRetries
                RetryState =
                    NYdb::NPersQueue::IRetryPolicy::GetExponentialBackoffPolicy(
                        TDuration::MilliSeconds(10), // minDelay
                        TDuration::MilliSeconds(200), // minLongRetryDelay
                        TDuration::Seconds(30), // maxDelay
                        MaxRetries,
                        TDuration::Max(), // maxTime
                        2.0 // scaleFactor
                    )->CreateRetryState();
            }
            TMaybe<TDuration> nextRetryDelay = RetryState->GetNextRetryDelay(status.GetStatus());
            if (status.GetStatus() == NYdb::EStatus::SCHEME_ERROR) {
                nextRetryDelay = Nothing(); // No topic => OK. Leave just transient issues.
            }

            LOG_D("Failed to remove read rule from `" << Topic.topic_path() << "`: " << status.GetIssues().ToString() << ". Status: " << status.GetStatus() << ". Retry after: " << nextRetryDelay);
            if (!nextRetryDelay) { // Not retryable
                Send(Owner, MakeHolder<TEvPrivate::TEvSingleReadRuleDeleterResult>(status.GetIssues()), 0, Index);
                PassAway();
            } else {
                Schedule(*nextRetryDelay, new NActors::TEvents::TEvWakeup());
            }
        }
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        StartRequest();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvRemoveReadRuleStatus, Handle);
        hFunc(NActors::TEvents::TEvWakeup, Handle);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    )

private:
    NYdb::NPersQueue::TPersQueueClientSettings GetPqClientSettings(std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProvider) {
        return NYdb::NPersQueue::TPersQueueClientSettings()
            .ClusterDiscoveryMode(NYdb::NPersQueue::EClusterDiscoveryMode::Off)
            .Database(Topic.database())
            .DiscoveryEndpoint(Topic.cluster_endpoint())
            .CredentialsProviderFactory(std::move(credentialsProvider))
            .DiscoveryMode(NYdb::EDiscoveryMode::Async)
            .SslCredentials(NYdb::TSslCredentials(Topic.use_ssl()));
    }

private:
    const NActors::TActorId Owner;
    const TString QueryId;
    const Fq::Private::TopicConsumer Topic;
    NYdb::TDriver YdbDriver;
    NYdb::NPersQueue::TPersQueueClient PqClient;
    ui64 Index = 0;
    const size_t MaxRetries;
    NYdb::NPersQueue::IRetryPolicy::IRetryState::TPtr RetryState;
};

// Actor for deletion of read rules for all topics in the query.
class TReadRuleDeleter : public TActorBootstrapped<TReadRuleDeleter> {
public:
    TReadRuleDeleter(
        NActors::TActorId owner,
        TString queryId,
        NYdb::TDriver ydbDriver,
        const ::google::protobuf::RepeatedPtrField<Fq::Private::TopicConsumer>& topicConsumers,
        TVector<std::shared_ptr<NYdb::ICredentialsProviderFactory>> credentials,
        size_t maxRetries
    )
        : Owner(owner)
        , QueryId(std::move(queryId))
        , YdbDriver(std::move(ydbDriver))
        , Topics(VectorFromProto(topicConsumers))
        , Credentials(std::move(credentials))
        , MaxRetries(maxRetries)
    {
        Y_VERIFY(!Topics.empty());
        Results.resize(Topics.size());
    }

    void Bootstrap() {
        Become(&TReadRuleDeleter::StateFunc);

        Children.reserve(Topics.size());
        Results.reserve(Topics.size());
        for (size_t i = 0; i < Topics.size(); ++i) {
            LOG_D("Create read rule deleter actor for `" << Topics[i].topic_path() << "` [" << i << "]");
            Children.push_back(Register(new TSingleReadRuleDeleter(SelfId(), QueryId, YdbDriver, Topics[i], Credentials[i], i, MaxRetries)));
        }
    }

    static constexpr char ActorName[] = "YQ_READ_RULE_DELETER";

    void Handle(TEvPrivate::TEvSingleReadRuleDeleterResult::TPtr& ev) {
        const ui64 index = ev->Cookie;
        Y_VERIFY(!Results[index]);
        if (ev->Get()->Issues) {
            Ok = false;
        }
        Results[index] = std::move(ev);
        ++ResultsGot;
        SendResultsAndPassAwayIfDone();
    }

    void PassAway() override {
        for (const NActors::TActorId& child : Children) {
            Send(child, new NActors::TEvents::TEvPoison());
        }
        TActorBootstrapped<TReadRuleDeleter>::PassAway();
    }

    void SendResultsAndPassAwayIfDone() {
        Y_VERIFY(ResultsGot <= Topics.size());
        if (ResultsGot == Topics.size()) {
            NYql::TIssues issues;
            if (!Ok) {
                NYql::TIssue mainIssue("Failed to delete read rules for topics");
                for (auto& result : Results) {
                    for (const NYql::TIssue& issue : result->Get()->Issues) {
                        mainIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
                    }
                }
                issues.AddIssue(std::move(mainIssue));
            }
            Send(Owner, MakeHolder<TEvents::TEvDataStreamsReadRulesDeletionResult>(std::move(issues)));
            PassAway();
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvSingleReadRuleDeleterResult, Handle);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    )

private:
    const NActors::TActorId Owner;
    const TString QueryId;
    NYdb::TDriver YdbDriver;
    const TVector<Fq::Private::TopicConsumer> Topics;
    const TVector<std::shared_ptr<NYdb::ICredentialsProviderFactory>> Credentials;
    const size_t MaxRetries;
    size_t ResultsGot = 0;
    bool Ok = true;
    TVector<TEvPrivate::TEvSingleReadRuleDeleterResult::TPtr> Results;
    TVector<NActors::TActorId> Children;
};

} // namespace

NActors::IActor* MakeReadRuleDeleterActor(
    NActors::TActorId owner,
    TString queryId,
    NYdb::TDriver ydbDriver,
    const ::google::protobuf::RepeatedPtrField<Fq::Private::TopicConsumer>& topicConsumers,
    TVector<std::shared_ptr<NYdb::ICredentialsProviderFactory>> credentials, // For each topic
    size_t maxRetries
)
{
    return new TReadRuleDeleter(
        owner,
        std::move(queryId),
        std::move(ydbDriver),
        topicConsumers,
        std::move(credentials),
        maxRetries
    );
}

} // namespace NFq
