#include "kqp_workload_service.h"
#include "kqp_workload_service_impl.h"
#include "kqp_workload_service_tables.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/events/workload_service.h>
#include <ydb/core/protos/console_config.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>


namespace NKikimr::NKqp {

namespace NWorkload {

namespace {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadService] " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadService] " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadService] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadService] " << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadService] " << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadService] " << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadService] " << stream)

using namespace NActors;


TWorkloadManagerConfig DefaultWorkloadManagerConfig;


class TKqpWorkloadService : public TActorBootstrapped<TKqpWorkloadService> {
    enum class ETablesCreationStatus {
        Cleanup,
        NotStarted,
        Pending,
        Finished,
    };

public:
    explicit TKqpWorkloadService(NMonitoring::TDynamicCounterPtr counters)
        : WorkloadManagerConfig(DefaultWorkloadManagerConfig)
        , Counters(counters)
    {}

    void Bootstrap() {
        Become(&TKqpWorkloadService::MainState);

        // Subscribe for FeatureFlags
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()), new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
            (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem
        }), IEventHandle::FlagTrackDelivery);

        EnabledResourcePools = AppData()->FeatureFlags.GetEnableResourcePools();
        if (EnabledResourcePools) {
            InitializeWorkloadService();
        }
    }

    void HandleSetConfigSubscriptionResponse() {
        LOG_D("Subscribed for config changes");
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        auto &event = ev->Get()->Record;
        
        auto featureFlags = event.GetConfig().GetFeatureFlags();
        if (featureFlags.GetEnableResourcePools()) {
            LOG_N("Resource pools was enanbled");
            EnabledResourcePools = true;
            InitializeWorkloadService();
        } else {
            LOG_N("Resource pools was disabled");
            EnabledResourcePools = false;
        }

        auto responseEv = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEv.Release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) const {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                LOG_C("Failed to deliver subscription request to config dispatcher");
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                LOG_E("Failed to deliver config notification response");
                break;

            case TKqpWorkloadServiceEvents::EvContinueRequest:
            case TKqpWorkloadServiceEvents::EvCleanupResponse:
                LOG_E("Failed to deliver event " << ev->Get()->SourceType << " to worker acrtor, session was lost");
                break;

            default:
                LOG_E("Undelivered event with unexpected source type: " << ev->Get()->SourceType);
                break;
        }
    }

    void Handle(TEvPlaceRequestIntoPool::TPtr& ev) {
        const TActorId& workerActorId = ev->Sender;
        if (!EnabledResourcePools) {
            ReplyContinueError(workerActorId, Ydb::StatusIds::UNSUPPORTED, "Workload manager is disabled. Please contact your system administrator to enable it");
            return;
        }

        const TString& poolId = ev->Get()->PoolId;
        auto poolState = GetPoolStateSafe(poolId);
        if (!poolState) {
            ReplyContinueError(workerActorId, Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Pool " << poolId << " not found");
            return;
        }

        if (poolState->PlaceRequest(workerActorId, ev->Get()->SessionId) && poolState->TablesRequired()) {
            ScheduleLeaseUpdate();
            PrepareWorkloadServiceTables();
        }
    }

    void Handle(TEvCleanupRequest::TPtr& ev) const {
        auto poolState = GetPoolStateSafe(ev->Get()->PoolId);
        if (!poolState) {
            ReplyCleanupError(ev->Sender, Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Pool " << ev->Get()->PoolId << " not found");
            return;
        }

        poolState->CleanupRequest(ev->Sender, ev->Get()->SessionId);
    }

    STRICT_STFUNC(MainState,
        sFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleSetConfigSubscriptionResponse);
        hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TEvPlaceRequestIntoPool, Handle);
        hFunc(TEvCleanupRequest, Handle);

        sFunc(TEvPrivate::TEvUpdatePoolsLeases, HandleUpdatePoolsLeases);
        hFunc(TEvPrivate::TEvRefreshPoolState, Handle);
        hFunc(TEvPrivate::TEvCleanupTablesFinished, Handle);
        hFunc(TEvPrivate::TEvTablesCreationFinished, Handle);

        hFunc(TEvPrivate::TEvCancelRequest, ForwardEvent);
        hFunc(TEvPrivate::TEvRefreshPoolStateResponse, ForwardEvent);
        hFunc(TEvPrivate::TEvDelayRequestResponse, ForwardEvent);
        hFunc(TEvPrivate::TEvStartRequestResponse, ForwardEvent);
        hFunc(TEvPrivate::TEvCleanupRequestsResponse, ForwardEvent);

        IgnoreFunc(TEvKqp::TEvCancelQueryResponse);
    )

private:
    void HandleUpdatePoolsLeases() {
        ScheduledLeaseUpdate = false;
        LOG_T("Try to start refresh for all pools");

        ui64 localPoolsSize = 0;
        for (auto& [_, poolState] : PoolIdToState) {
            poolState->RefreshState();
            localPoolsSize += poolState->GetLocalPoolSize();
        }
        if (localPoolsSize) {
            ScheduleLeaseUpdate();
        }
    }

    void Handle(TEvPrivate::TEvRefreshPoolState::TPtr& ev) const {
        GetPoolState(ev->Get()->Record.GetPoolId())->RefreshState(true);
    }

    void Handle(TEvPrivate::TEvCleanupTablesFinished::TPtr& ev) {
        if (ev->Get()->Success) {
            LOG_D("Cleanup completed, tables exists: " << ev->Get()->TablesExists);
        } else {
            LOG_C("Failed to cleanup tables, issues: " << ev->Get()->Issues.ToOneLineString());
        }

        TablesCreationStatus = ETablesCreationStatus::NotStarted;
        if (ev->Get()->TablesExists) {
            TablesCreationStatus = ETablesCreationStatus::Finished;
            OnPreparingFinished(Ydb::StatusIds::SUCCESS);
        } else if (TablesCreationRequired) {
            PrepareWorkloadServiceTables();
        }
    }

    void Handle(TEvPrivate::TEvTablesCreationFinished::TPtr& ev) {
        TablesCreationStatus = ev->Get()->Success ? ETablesCreationStatus::Finished : ETablesCreationStatus::NotStarted;

        if (ev->Get()->Success) {
            LOG_D("Succefully created tables, start refresh requests");
            OnPreparingFinished(Ydb::StatusIds::SUCCESS);
            return;
        }

        LOG_E("Failed to create tables, issues: " << ev->Get()->Issues.ToOneLineString());

        NYql::TIssue rootIssue("Failed to create workload service tables");
        for (const NYql::TIssue& issue : ev->Get()->Issues) {
            rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
        }
        OnPreparingFinished(Ydb::StatusIds::INTERNAL_ERROR, {rootIssue});
    }

    template<typename TEvent>
    void ForwardEvent(TEvent ev) const {
        GetPoolState(ev->Get()->PoolId)->Handle(std::move(ev));
    }

private:
    void InitializeWorkloadService() {
        if (ServiceInitialized) {
            return;
        }
        LOG_D("Started workload service initialization");
        ServiceInitialized = true;

        PoolIdToState.reserve(WorkloadManagerConfig.Pools.size());
        for (const auto& [poolId, poolConfig] : WorkloadManagerConfig.Pools) {
            PoolIdToState.insert({poolId, NQueue::CreateState(ActorContext(), poolId, poolConfig, Counters->GetSubgroup("pool", poolId))});
        }

        Register(CreateCleanupTablesActor());
    }

    void ScheduleLeaseUpdate() {
        if (ScheduledLeaseUpdate) {
            return;
        }
        ScheduledLeaseUpdate = true;
        Schedule(LEASE_DURATION / 2, new TEvPrivate::TEvUpdatePoolsLeases());
    }

    void PrepareWorkloadServiceTables() {
        TablesCreationRequired = true;
        if (TablesCreationStatus == ETablesCreationStatus::NotStarted) {
            TablesCreationStatus = ETablesCreationStatus::Pending;
            Register(CreateTablesCreator());
        }
    }

    void OnPreparingFinished(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) const {
        for (auto& [_, poolState] : PoolIdToState) {
            poolState->OnPreparingFinished(status, issues);
        }
    }

    void ReplyContinueError(const TActorId& replyActorId, Ydb::StatusIds::StatusCode status, const TString& message) const {
        LOG_W("Reply continue error " << status << " to " << replyActorId << ": " << message);
        Send(replyActorId, new TEvContinueRequest(status, {}, {NYql::TIssue(message)}));
    }

    void ReplyCleanupError(const TActorId& replyActorId, Ydb::StatusIds::StatusCode status, const TString& message) const {
        LOG_W("Reply cleanup error " << status << " to " << replyActorId << ": " << message);
        Send(replyActorId, new TEvCleanupResponse(status, {NYql::TIssue(message)}));
    }

    NQueue::TStatePtr GetPoolState(const TString& poolId) const {
        auto poolIt = PoolIdToState.find(poolId);
        Y_ENSURE(poolIt != PoolIdToState.end(), "Invalid pool id " << poolId);
        return poolIt->second;
    }

    NQueue::TStatePtr GetPoolStateSafe(const TString& poolId) const {
        auto poolIt = PoolIdToState.find(poolId);
        if (poolIt != PoolIdToState.end()) {
            return poolIt->second;
        }
        return nullptr;
    }

private:
    const TWorkloadManagerConfig WorkloadManagerConfig;
    NMonitoring::TDynamicCounterPtr Counters;

    bool EnabledResourcePools = false;
    bool ServiceInitialized = false;
    bool ScheduledLeaseUpdate = false;
    bool TablesCreationRequired = false;
    ETablesCreationStatus TablesCreationStatus = ETablesCreationStatus::Cleanup;

    std::unordered_map<TString, NQueue::TStatePtr> PoolIdToState;
};

}  // anonymous namespace

void SetWorkloadManagerConfig(const TWorkloadManagerConfig& workloadManagerConfig) {
    DefaultWorkloadManagerConfig = workloadManagerConfig;
}

}  // namespace NWorkload

IActor* CreateKqpWorkloadService(NMonitoring::TDynamicCounterPtr counters) {
    return new NWorkload::TKqpWorkloadService(counters);
}

}  // namespace NKikimr::NKqp
