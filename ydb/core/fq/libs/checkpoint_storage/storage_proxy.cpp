#include "storage_proxy.h"

#include "gc.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>

#include <ydb/core/fq/libs/config/protos/storage.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>
#include "ydb_checkpoint_storage.h"
#include "ydb_state_storage.h"

#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/fq/libs/checkpoint_storage/events/events.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/util.h>

#include <ydb/core/protos/feature_flags.pb.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/stream/file.h>
#include <util/string/join.h>
#include <util/string/strip.h>

#include <library/cpp/retry/retry_policy.h>

namespace NFq {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr char CHECKPOINTS_TABLE_PREFIX[] = ".metadata/streaming/checkpoints";
constexpr ui64 DELAYED_EVENTS_QUEUE_LIMIT = 10000;

struct TStorageProxyMetrics : public TThrRefBase {
    explicit TStorageProxyMetrics(const ::NMonitoring::TDynamicCounterPtr& counters)
        : Counters(counters)
        , Errors(Counters->GetCounter("Errors", true))
        , Inflight(Counters->GetCounter("Inflight"))
        , LatencyMs(Counters->GetHistogram("LatencyMs", ::NMonitoring::ExplicitHistogram({1, 5, 20, 100, 500, 2000, 10000, 50000})))
    {}

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr Errors;
    ::NMonitoring::TDynamicCounters::TCounterPtr Inflight;
    ::NMonitoring::THistogramPtr LatencyMs;
};

using TStorageProxyMetricsPtr = TIntrusivePtr<TStorageProxyMetrics>;

struct TRequestContext : public TThrRefBase {
    TInstant StartTime = TInstant::Now();
    const TStorageProxyMetricsPtr Metrics;
    
    TRequestContext(const TStorageProxyMetricsPtr& metrics)
        : Metrics(metrics) {
        Metrics->Inflight->Inc();
    }

    ~TRequestContext() {
        Metrics->Inflight->Dec();
        Metrics->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
    }

    void IncError() {
        Metrics->Errors->Inc();
    }
};

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvInitResult = EvBegin,
        EvInitialize,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events
    struct TEvInitResult : public TEventLocal<TEvInitResult, EvInitResult> {
        TEvInitResult(const NYql::TIssues& storageIssues, const NYql::TIssues& stateIssues)
            : StorageIssues(storageIssues)
            , StateIssues(stateIssues) {}
        NYql::TIssues StorageIssues;
        NYql::TIssues StateIssues;
    };
    struct TEvInitialize : public TEventLocal<TEvInitialize, EvInitialize> {
    };
};

class TStorageProxy : public TActorBootstrapped<TStorageProxy> {
private:
    using IRetryPolicy = IRetryPolicy<>;

    TCheckpointStorageSettings Config;
    TString IdsPrefix;
    TExternalStorageSettings StorageConfig;
    TCheckpointStoragePtr CheckpointStorage;
    TStateStoragePtr StateStorage;
    TActorId ActorGC;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    NYdb::TDriver Driver;
    const TStorageProxyMetricsPtr Metrics;
    const IRetryPolicy::TPtr RetryPolicy;
    IRetryPolicy::IRetryState::TPtr RetryState;

    enum class EInitStatus {
        NotStarted,
        Pending,
        Finished,
    };
    EInitStatus InitStatus = EInitStatus::NotStarted;
    std::deque<THolder<IEventHandle>> DelayedEventsQueue;
    NKikimrConfig::TFeatureFlags FeatureFlags;

public:
    explicit TStorageProxy(
        const TCheckpointStorageSettings& config,
        const TString& idsPrefix,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        NYdb::TDriver driver,
        const ::NMonitoring::TDynamicCounterPtr& counters);

    void Bootstrap();
    void StartInitialization();

    static constexpr char ActorName[] = "YQ_STORAGE_PROXY";

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvCheckpointStorage::TEvRegisterCoordinatorRequest, Handle);
        hFunc(TEvCheckpointStorage::TEvCreateCheckpointRequest, Handle);
        hFunc(TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest, Handle);
        hFunc(TEvCheckpointStorage::TEvCompleteCheckpointRequest, Handle);
        hFunc(TEvCheckpointStorage::TEvAbortCheckpointRequest, Handle);
        hFunc(TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest, Handle);

        hFunc(NYql::NDq::TEvDqCompute::TEvSaveTaskState, Handle);
        hFunc(NYql::NDq::TEvDqCompute::TEvGetTaskState, Handle);
        hFunc(TEvPrivate::TEvInitResult, Handle);
        hFunc(TEvPrivate::TEvInitialize, Handle);

        hFunc(NKikimr::NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        hFunc(NKikimr::NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
    )

    void Handle(TEvCheckpointStorage::TEvRegisterCoordinatorRequest::TPtr& ev);

    void Handle(TEvCheckpointStorage::TEvCreateCheckpointRequest::TPtr& ev);
    void Handle(TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest::TPtr& ev);
    void Handle(TEvCheckpointStorage::TEvCompleteCheckpointRequest::TPtr& ev);
    void Handle(TEvCheckpointStorage::TEvAbortCheckpointRequest::TPtr& ev);

    void Handle(TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest::TPtr& ev);

    void Handle(NYql::NDq::TEvDqCompute::TEvSaveTaskState::TPtr& ev);
    void Handle(NYql::NDq::TEvDqCompute::TEvGetTaskState::TPtr& ev);
    void Handle(TEvPrivate::TEvInitResult::TPtr& ev);
    void Handle(TEvPrivate::TEvInitialize::TPtr& ev);
    void Handle(NKikimr::NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev);
    void Handle(NKikimr::NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr& ev);

    template<typename TEvent>
    bool CheckStatus(TEvent& ev);

    void HandleDelayedRequestError(THolder<IEventHandle>& ev, NYql::TIssues issues);
};

static void FillDefaultParameters(TCheckpointStorageSettings& checkpointCoordinatorConfig, TExternalStorageSettings& ydbStorageConfig) {
    if (!checkpointCoordinatorConfig.GetExternalStorage().GetToken() && checkpointCoordinatorConfig.GetExternalStorage().GetTokenFile()) {
        checkpointCoordinatorConfig.MutableExternalStorage().SetToken(StripString(TFileInput(checkpointCoordinatorConfig.GetExternalStorage().GetTokenFile()).ReadAll()));
    }

    if (!ydbStorageConfig.GetToken() && ydbStorageConfig.GetTokenFile()) {
        ydbStorageConfig.SetToken(StripString(TFileInput(ydbStorageConfig.GetTokenFile()).ReadAll()));
    }
}

TStorageProxy::TStorageProxy(
    const TCheckpointStorageSettings& config,
    const TString& idsPrefix,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    NYdb::TDriver driver,
    const ::NMonitoring::TDynamicCounterPtr& counters)
    : Config(config)
    , IdsPrefix(idsPrefix)
    , StorageConfig(Config.GetExternalStorage())
    , CredentialsProviderFactory(credentialsProviderFactory)
    , Driver(std::move(driver))
    , Metrics(MakeIntrusive<TStorageProxyMetrics>(counters))
    , RetryPolicy(IRetryPolicy::GetExponentialBackoffPolicy(
        [](){return ERetryErrorClass::LongRetry;},
        TDuration::MilliSeconds(100), 
        TDuration::MilliSeconds(100),
        TDuration::Seconds(10)
        )) {
    FillDefaultParameters(Config, StorageConfig);
}

void TStorageProxy::Bootstrap() {
    LOG_STREAMS_STORAGE_SERVICE_INFO("Bootstrap");
    IYdbConnection::TPtr ydbConnection;
    if (!StorageConfig.GetEndpoint().empty()) {
        LOG_STREAMS_STORAGE_SERVICE_INFO("Create sdk ydb connection");
        ydbConnection = CreateSdkYdbConnection(StorageConfig, CredentialsProviderFactory, Driver);
    } else {
        LOG_STREAMS_STORAGE_SERVICE_INFO("Create local ydb connection");
        ydbConnection = CreateLocalYdbConnection(NKikimr::AppData()->TenantName, CHECKPOINTS_TABLE_PREFIX);
    }
    CheckpointStorage = NewYdbCheckpointStorage(StorageConfig, CreateEntityIdGenerator(IdsPrefix), ydbConnection);
    StateStorage = NewYdbStateStorage(Config, ydbConnection);

    if (Config.GetCheckpointGarbageConfig().GetEnabled()) {
        const auto& gcConfig = Config.GetCheckpointGarbageConfig();
        ActorGC = Register(NewGC(gcConfig, CheckpointStorage, StateStorage).release());
    }
      
    Send(NKikimr::NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
        new NKikimr::NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({NKikimrConsole::TConfigItem::FeatureFlagsItem}));

    Become(&TStorageProxy::StateFunc);
    FeatureFlags = NKikimr::AppData()->FeatureFlags;

    LOG_STREAMS_STORAGE_SERVICE_INFO("Successfully bootstrapped TStorageProxy " << SelfId() << " with connection to "
        << StorageConfig.GetEndpoint().data()
        << ":" << StorageConfig.GetDatabase().data())
}

void TStorageProxy::StartInitialization() {
    LOG_STREAMS_STORAGE_SERVICE_INFO("StartInitialization");

    NACLib::TDiffACL acl;
    acl.ClearAccess();
    acl.SetInterruptInheritance(FeatureFlags.GetEnableSecureScriptExecutions());

    auto storageInitFuture = CheckpointStorage->Init(acl);
    auto stateInitFuture = StateStorage->Init(acl);

    std::vector<NThreading::TFuture<NYql::TIssues>> futures{storageInitFuture, stateInitFuture};
    auto voidFuture = NThreading::WaitAll(futures);
    voidFuture.Subscribe([futures = std::move(futures), actorId = this->SelfId(), actorSystem = TActivationContext::ActorSystem()](const auto& ) mutable {
            actorSystem->Send(actorId, new TEvPrivate::TEvInitResult(futures[0].GetValue(), futures[1].GetValue()));
        });
}

void TStorageProxy::Handle(TEvCheckpointStorage::TEvRegisterCoordinatorRequest::TPtr& ev) {
    const auto* event = ev->Get();
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->CoordinatorId << "] Got TEvRegisterCoordinatorRequest")
    if (!CheckStatus(ev)) {
        return;
    }
    auto context = MakeIntrusive<TRequestContext>(Metrics);

    CheckpointStorage->RegisterGraphCoordinator(event->CoordinatorId)
        .Apply([coordinatorId = event->CoordinatorId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                actorSystem = TActivationContext::ActorSystem(),
                context] (const NThreading::TFuture<NYql::TIssues>& issuesFuture) {
            auto response = std::make_unique<TEvCheckpointStorage::TEvRegisterCoordinatorResponse>();
            response->Issues = issuesFuture.GetValue();
            if (response->Issues) {
                context->IncError();
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(*actorSystem, "[" << coordinatorId << "] Failed to register graph: " << response->Issues.ToString())
            } else {
                LOG_STREAMS_STORAGE_SERVICE_AS_INFO(*actorSystem, "[" << coordinatorId << "] Graph registered")
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*actorSystem, "[" << coordinatorId << "] Send TEvRegisterCoordinatorResponse")
            actorSystem->Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(TEvCheckpointStorage::TEvCreateCheckpointRequest::TPtr& ev) {
    const auto* event = ev->Get();
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->CoordinatorId << "] [" << event->CheckpointId << "] Got TEvCreateCheckpointRequest")
    if (!CheckStatus(ev)) {
        return;
    }
    auto context = MakeIntrusive<TRequestContext>(Metrics);

    CheckpointStorage->GetTotalCheckpointsStateSize(event->CoordinatorId.GraphId)
        .Apply([checkpointId = event->CheckpointId,
                coordinatorId = event->CoordinatorId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                totalGraphCheckpointsSizeLimit = Config.GetStateStorageLimits().GetMaxGraphCheckpointsSizeBytes(),
                graphDesc = std::move(event->GraphDescription),
                storage = CheckpointStorage,
                context]
               (const NThreading::TFuture<ICheckpointStorage::TGetTotalCheckpointsStateSizeResult>& resultFuture) {
            auto [totalGraphCheckpointsSize, issues] = resultFuture.GetValue();

            if (!issues && totalGraphCheckpointsSize > totalGraphCheckpointsSizeLimit) {
                TStringStream ss;
                ss << "Graph checkpoints size limit exceeded: limit " << totalGraphCheckpointsSizeLimit << ", current checkpoints size: " << totalGraphCheckpointsSize;
                issues.AddIssue(std::move(ss.Str()));
            }
            if (issues) {
                context->IncError();
                return NThreading::MakeFuture(ICheckpointStorage::TCreateCheckpointResult {TString(), std::move(issues) } );
            }
            if (std::holds_alternative<TString>(graphDesc)) {
                return storage->CreateCheckpoint(coordinatorId, checkpointId, std::get<TString>(graphDesc), ECheckpointStatus::Pending);
            } else {
                return storage->CreateCheckpoint(coordinatorId, checkpointId, std::get<NProto::TCheckpointGraphDescription>(graphDesc), ECheckpointStatus::Pending);
            }
        })
        .Apply([checkpointId = event->CheckpointId,
                coordinatorId = event->CoordinatorId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                actorSystem = TActivationContext::ActorSystem(),
                context]
               (const NThreading::TFuture<ICheckpointStorage::TCreateCheckpointResult>& resultFuture) {
            auto [graphDescId, issues] = resultFuture.GetValue();
            auto response = std::make_unique<TEvCheckpointStorage::TEvCreateCheckpointResponse>(checkpointId, std::move(issues), std::move(graphDescId));
            if (response->Issues) {
                context->IncError();
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(*actorSystem, "[" << coordinatorId << "] [" << checkpointId << "] Failed to create checkpoint: " << response->Issues.ToString());
            } else {
                LOG_STREAMS_STORAGE_SERVICE_AS_INFO(*actorSystem, "[" << coordinatorId << "] [" << checkpointId << "] Checkpoint created");
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*actorSystem, "[" << coordinatorId << "] [" << checkpointId << "] Send TEvCreateCheckpointResponse");
            actorSystem->Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest::TPtr& ev) {
    const auto* event = ev->Get();
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->CoordinatorId << "] [" << event->CheckpointId << "] Got TEvSetCheckpointPendingCommitStatusRequest")
    if (!CheckStatus(ev)) {
        return;
    }
  //  DelayedEventsQueue.emplace_back(ev.Release());


    auto context = MakeIntrusive<TRequestContext>(Metrics);
    CheckpointStorage->UpdateCheckpointStatus(event->CoordinatorId, event->CheckpointId, ECheckpointStatus::PendingCommit, ECheckpointStatus::Pending, event->StateSizeBytes)
        .Apply([checkpointId = event->CheckpointId,
                coordinatorId = event->CoordinatorId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                actorSystem = TActivationContext::ActorSystem(),
                context]
               (const NThreading::TFuture<NYql::TIssues>& issuesFuture) {
            auto issues = issuesFuture.GetValue();
            auto response = std::make_unique<TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusResponse>(checkpointId, std::move(issues));
            if (response->Issues) {
                context->IncError();
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(*actorSystem, "[" << coordinatorId << "] [" << checkpointId << "] Failed to set 'PendingCommit' status: " << response->Issues.ToString())
            } else {
                LOG_STREAMS_STORAGE_SERVICE_AS_INFO(*actorSystem, "[" << coordinatorId << "] [" << checkpointId << "] Status updated to 'PendingCommit'")
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*actorSystem, "[" << coordinatorId << "] [" << checkpointId << "] Send TEvSetCheckpointPendingCommitStatusResponse")
            actorSystem->Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(TEvCheckpointStorage::TEvCompleteCheckpointRequest::TPtr& ev) {
    const auto* event = ev->Get();
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->CoordinatorId << "] [" << event->CheckpointId << "] Got TEvCompleteCheckpointRequest")
    if (!CheckStatus(ev)) {
        return;
    }
    auto context = MakeIntrusive<TRequestContext>(Metrics);
    CheckpointStorage->UpdateCheckpointStatus(event->CoordinatorId, event->CheckpointId, ECheckpointStatus::Completed, ECheckpointStatus::PendingCommit, event->StateSizeBytes)
        .Apply([checkpointId = event->CheckpointId,
                coordinatorId = event->CoordinatorId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                type = event->Type,
                gcEnabled = Config.GetCheckpointGarbageConfig().GetEnabled(),
                actorGC = ActorGC,
                actorSystem = TActivationContext::ActorSystem(),
                context]
               (const NThreading::TFuture<NYql::TIssues>& issuesFuture) {
            auto issues = issuesFuture.GetValue();
            auto response = std::make_unique<TEvCheckpointStorage::TEvCompleteCheckpointResponse>(checkpointId, std::move(issues));
            if (response->Issues) {
                context->IncError();
                LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*actorSystem, "[" << coordinatorId << "] [" << checkpointId << "] Failed to set 'Completed' status: " << response->Issues.ToString())
            } else {
                LOG_STREAMS_STORAGE_SERVICE_AS_INFO(*actorSystem, "[" << coordinatorId << "] [" << checkpointId << "] Status updated to 'Completed'")
                if (gcEnabled) {
                    auto request = std::make_unique<TEvCheckpointStorage::TEvNewCheckpointSucceeded>(coordinatorId, checkpointId, type);
                    LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*actorSystem, "[" << coordinatorId << "] [" << checkpointId << "] Send TEvNewCheckpointSucceeded")
                    actorSystem->Send(actorGC, request.release(), 0);
                }
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*actorSystem, "[" << coordinatorId << "] [" << checkpointId << "] Send TEvCompleteCheckpointResponse")
            actorSystem->Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(TEvCheckpointStorage::TEvAbortCheckpointRequest::TPtr& ev) {
    const auto* event = ev->Get();
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->CoordinatorId << "] [" << event->CheckpointId << "] Got TEvAbortCheckpointRequest")
    if (!CheckStatus(ev)) {
        return;
    }
    auto context = MakeIntrusive<TRequestContext>(Metrics);
    CheckpointStorage->AbortCheckpoint(event->CoordinatorId,event->CheckpointId)
        .Apply([checkpointId = event->CheckpointId,
                coordinatorId = event->CoordinatorId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                actorSystem = TActivationContext::ActorSystem(),
                context] (const NThreading::TFuture<NYql::TIssues>& issuesFuture) {
            auto issues = issuesFuture.GetValue();
            auto response = std::make_unique<TEvCheckpointStorage::TEvAbortCheckpointResponse>(checkpointId, std::move(issues));
            if (response->Issues) {
                context->IncError();
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(*actorSystem, "[" << coordinatorId << "] [" << checkpointId << "] Failed to abort checkpoint: " << response->Issues.ToString())
            } else {
                LOG_STREAMS_STORAGE_SERVICE_AS_INFO(*actorSystem, "[" << coordinatorId << "] [" << checkpointId << "] Checkpoint aborted")
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*actorSystem, "[" << coordinatorId << "] [" << checkpointId << "] Send TEvAbortCheckpointResponse")
            actorSystem->Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest::TPtr& ev) {
    const auto* event = ev->Get();
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->GraphId << "] Got TEvGetCheckpointsMetadataRequest");
    if (!CheckStatus(ev)) {
        return;
    }
    auto context = MakeIntrusive<TRequestContext>(Metrics);
    CheckpointStorage->GetCheckpoints(event->GraphId, event->Statuses, event->Limit, event->LoadGraphDescription)
        .Apply([graphId = event->GraphId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                actorSystem = TActivationContext::ActorSystem(),
                context] (const NThreading::TFuture<ICheckpointStorage::TGetCheckpointsResult>& futureResult) {
            auto result = futureResult.GetValue();
            auto response = std::make_unique<TEvCheckpointStorage::TEvGetCheckpointsMetadataResponse>(result.first, result.second);
            if (response->Issues) {
                context->IncError();
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(*actorSystem, "[" << graphId << "] Failed to get checkpoints: " << response->Issues.ToString())
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*actorSystem, "[" << graphId << "] Send TEvGetCheckpointsMetadataResponse")
            actorSystem->Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(NYql::NDq::TEvDqCompute::TEvSaveTaskState::TPtr& ev) {
    auto context = MakeIntrusive<TRequestContext>(Metrics);
    auto* event = ev->Get();
    const auto checkpointId = TCheckpointId(event->Checkpoint.GetGeneration(), event->Checkpoint.GetId());
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->GraphId << "] [" << checkpointId << "] Got TEvSaveTaskState: task " << event->TaskId);

    const size_t stateSize = event->State.ByteSizeLong();
    if (stateSize > Config.GetStateStorageLimits().GetMaxTaskStateSizeBytes()) {
        LOG_STREAMS_STORAGE_SERVICE_WARN("[" << event->GraphId << "] [" << checkpointId << "] Won't save task state because it's too big: task: " << event->TaskId
            << ", state size: " << stateSize << "/" << Config.GetStateStorageLimits().GetMaxTaskStateSizeBytes());
        auto response = std::make_unique<NYql::NDq::TEvDqCompute::TEvSaveTaskStateResult>();
        response->Record.MutableCheckpoint()->SetGeneration(checkpointId.CoordinatorGeneration);
        response->Record.MutableCheckpoint()->SetId(checkpointId.SeqNo);
        response->Record.SetStateSizeBytes(0);
        response->Record.SetTaskId(event->TaskId);
        response->Record.SetStatus(NYql::NDqProto::TEvSaveTaskStateResult::STATE_TOO_BIG);
        Send(ev->Sender, response.release());
        return;
    }

    StateStorage->SaveState(event->TaskId, event->GraphId, checkpointId, event->State)
        .Apply([graphId = event->GraphId,
                checkpointId,
                taskId = event->TaskId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                actorSystem = TActivationContext::ActorSystem(),
                context](const NThreading::TFuture<IStateStorage::TSaveStateResult>& futureResult) {
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*actorSystem, "[" << graphId << "] [" << checkpointId << "] TEvSaveTaskState Apply: task: " << taskId)
            const auto& issues = futureResult.GetValue().second;
            auto response = std::make_unique<NYql::NDq::TEvDqCompute::TEvSaveTaskStateResult>();
            response->Record.MutableCheckpoint()->SetGeneration(checkpointId.CoordinatorGeneration);
            response->Record.MutableCheckpoint()->SetId(checkpointId.SeqNo);
            response->Record.SetStateSizeBytes(futureResult.GetValue().first);
            response->Record.SetTaskId(taskId);

            if (issues) {
                context->IncError();
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(*actorSystem, "[" << graphId << "] [" << checkpointId << "] Failed to save task state: task: " << taskId << ", issues: " << issues.ToString())
                response->Record.SetStatus(NYql::NDqProto::TEvSaveTaskStateResult::STORAGE_ERROR);
            } else {
                response->Record.SetStatus(NYql::NDqProto::TEvSaveTaskStateResult::OK);
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*actorSystem, "[" << graphId << "] [" << checkpointId << "] Send TEvSaveTaskStateResult: task: " << taskId)
            actorSystem->Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(NYql::NDq::TEvDqCompute::TEvGetTaskState::TPtr& ev) {
    auto context = MakeIntrusive<TRequestContext>(Metrics);
    const auto* event = ev->Get();
    const auto checkpointId = TCheckpointId(event->Checkpoint.GetGeneration(), event->Checkpoint.GetId());
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->GraphId << "] [" << checkpointId << "] Got TEvGetTaskState: tasks {" << JoinSeq(", ", event->TaskIds) << "}");

    StateStorage->GetState(event->TaskIds, event->GraphId, checkpointId)
        .Apply([checkpointId = event->Checkpoint,
                generation = event->Generation,
                graphId = event->GraphId,
                taskIds = event->TaskIds,
                cookie = ev->Cookie,
                sender = ev->Sender,
                actorSystem = TActivationContext::ActorSystem(),
                context](const NThreading::TFuture<IStateStorage::TGetStateResult>& resultFuture) {
            auto result = resultFuture.GetValue();

            auto response = std::make_unique<NYql::NDq::TEvDqCompute::TEvGetTaskStateResult>(checkpointId, result.second, generation);
            std::swap(response->States, result.first);
            if (response->Issues) {
                context->IncError();
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(*actorSystem, "[" << graphId << "] [" << checkpointId << "] Failed to get task state: tasks: {" << JoinSeq(", ", taskIds) << "}, issues: " << response->Issues.ToString());
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*actorSystem, "[" << graphId << "] [" << checkpointId << "] Send TEvGetTaskStateResult: tasks: {" << JoinSeq(", ", taskIds) << "}");
            actorSystem->Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(TEvPrivate::TEvInitResult::TPtr& ev) {
    const auto* event = ev->Get();
    if (!event->StorageIssues.Empty()) {
        LOG_STREAMS_STORAGE_SERVICE_ERROR("Failed to init checkpoint storage: " << event->StorageIssues.ToOneLineString());
    }
    if (!event->StateIssues.Empty()) {
        LOG_STREAMS_STORAGE_SERVICE_ERROR("Failed to init state storage: " << event->StateIssues.ToOneLineString());
    }
    bool success = event->StorageIssues.Empty() && event->StateIssues.Empty();
    if (!success) {
        if (RetryState == nullptr) {
            RetryState = RetryPolicy->CreateRetryState();
        }
        if (auto delay = RetryState->GetNextRetryDelay()) {
            LOG_STREAMS_STORAGE_SERVICE_INFO("Schedule init retry after " << delay);
            Schedule(*delay, new TEvPrivate::TEvInitialize());
        }
    } else {
        LOG_STREAMS_STORAGE_SERVICE_INFO("Checkpoint storage and state storage were successfully inited");
        InitStatus = EInitStatus::Finished;
    }
        
    while (!DelayedEventsQueue.empty()) {
        auto ev = std::move(DelayedEventsQueue.front());
        if (success) {
            TActivationContext::Send(ev.Release());
        } else {
            const auto& issues = event->StorageIssues ? event->StorageIssues : event->StateIssues;
            HandleDelayedRequestError(ev, issues);
        }
        DelayedEventsQueue.pop_front();
    }    
}

void TStorageProxy::Handle(TEvPrivate::TEvInitialize::TPtr& /*ev*/) {
    StartInitialization();
}

template<typename TEvent>
bool TStorageProxy::CheckStatus(TEvent& ev) {
    switch (InitStatus) {
        case EInitStatus::NotStarted:
            StartInitialization();
            InitStatus = EInitStatus::Pending;
            [[fallthrough]];
        case EInitStatus::Pending:
            if (DelayedEventsQueue.size() < DELAYED_EVENTS_QUEUE_LIMIT) {
                LOG_STREAMS_STORAGE_SERVICE_WARN("Add to delayed");
                DelayedEventsQueue.emplace_back(ev.Release());
            } else {
                NYql::TIssues issues;
                auto evHolder = THolder<IEventHandle>(ev.Release());
                HandleDelayedRequestError(evHolder, NYql::TIssues{NYql::TIssue{"Too many queued requests"}});
            }
            return false;
        case EInitStatus::Finished:
            return true;
    }
}

void TStorageProxy::HandleDelayedRequestError(THolder<IEventHandle>& ev, NYql::TIssues issues) {
    switch (ev->GetTypeRewrite()) {
        case TEvCheckpointStorage::TEvRegisterCoordinatorRequest::EventType: {
            LOG_STREAMS_STORAGE_SERVICE_WARN("Send TEvRegisterCoordinatorResponse with issues: " << issues.ToOneLineString());
            auto response = std::make_unique<TEvCheckpointStorage::TEvRegisterCoordinatorResponse>(std::move(issues));
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            break;
        }
        case TEvCheckpointStorage::TEvCreateCheckpointRequest::EventType: {
            LOG_STREAMS_STORAGE_SERVICE_WARN("Send TEvCreateCheckpointResponse with issues: " << issues.ToOneLineString());
            auto event = IEventHandle::Release<TEvCheckpointStorage::TEvCreateCheckpointRequest>(ev);
            auto response = std::make_unique<TEvCheckpointStorage::TEvCreateCheckpointResponse>(event->CheckpointId, std::move(issues), TString());
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            break;
        }
        case TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest::EventType: {
            LOG_STREAMS_STORAGE_SERVICE_WARN("Send TEvGetCheckpointsMetadataResponse with issues: " << issues.ToOneLineString());
            auto response = std::make_unique<TEvCheckpointStorage::TEvGetCheckpointsMetadataResponse>(TVector<TCheckpointMetadata>{}, std::move(issues));
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            break;
        }
        case TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest::EventType: {
            LOG_STREAMS_STORAGE_SERVICE_WARN("Send TEvSetCheckpointPendingCommitStatusResponse with issues: " << issues.ToOneLineString());
            auto event = IEventHandle::Release<TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest>(ev);
            auto response = std::make_unique<TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusResponse>(event->CheckpointId, std::move(issues));
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            break;
        }
        case TEvCheckpointStorage::TEvCompleteCheckpointRequest::EventType: {
            LOG_STREAMS_STORAGE_SERVICE_WARN("Send TEvCompleteCheckpointResponse with issues: " << issues.ToOneLineString());
            auto event = IEventHandle::Release<TEvCheckpointStorage::TEvCompleteCheckpointRequest>(ev);
            auto response = std::make_unique<TEvCheckpointStorage::TEvCompleteCheckpointResponse>(event->CheckpointId, std::move(issues));
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            break;
        }
        case TEvCheckpointStorage::TEvAbortCheckpointRequest::EventType: {
            LOG_STREAMS_STORAGE_SERVICE_WARN("Send TEvGetCheckpointsMetadataResponse with issues: " << issues.ToOneLineString());
            auto event = IEventHandle::Release<TEvCheckpointStorage::TEvAbortCheckpointRequest>(ev);
            auto response = std::make_unique<TEvCheckpointStorage::TEvAbortCheckpointResponse>(event->CheckpointId, std::move(issues));
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            break;
        }
        default:
            Y_ABORT("no way!");
    }
}

void TStorageProxy::Handle(NKikimr::NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
    LOG_STREAMS_STORAGE_SERVICE_INFO("Subscribed for config changes.");
}

void TStorageProxy::Handle(NKikimr::NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
    LOG_STREAMS_STORAGE_SERVICE_INFO("Updated config.");
    auto &event = ev->Get()->Record;
    Send(ev->Sender, new NKikimr::NConsole::TEvConsole::TEvConfigNotificationResponse(event), 0, ev->Cookie);
    auto* newFeatureFlags = event.MutableConfig()->MutableFeatureFlags();
    bool changed = newFeatureFlags->GetEnableSecureScriptExecutions() != FeatureFlags.GetEnableSecureScriptExecutions();
    FeatureFlags.Swap(newFeatureFlags);

    if (changed) {
        if (InitStatus != EInitStatus::NotStarted) {
            StartInitialization();
            InitStatus = EInitStatus::Pending;
        }
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewStorageProxy(
    const TCheckpointStorageSettings& config,
    const TString& idsPrefix,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    NYdb::TDriver driver,
    const ::NMonitoring::TDynamicCounterPtr& counters)
{
    return std::unique_ptr<NActors::IActor>(new TStorageProxy(config, idsPrefix, credentialsProviderFactory, std::move(driver), counters));
}

} // namespace NFq
