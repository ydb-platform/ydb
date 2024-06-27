#include "kqp_workload_service.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>

#include <ydb/core/kqp/workload_service/actors/actors.h>
#include <ydb/core/kqp/workload_service/common/events.h>
#include <ydb/core/kqp/workload_service/common/helpers.h>
#include <ydb/core/kqp/workload_service/tables/table_queries.h>

#include <ydb/core/protos/console_config.pb.h>


namespace NKikimr::NKqp {

namespace NWorkload {

namespace {

using namespace NActors;


class TKqpWorkloadService : public TActorBootstrapped<TKqpWorkloadService> {
    enum class ETablesCreationStatus {
        Cleanup,
        NotStarted,
        Pending,
        Finished,
    };

public:
    explicit TKqpWorkloadService(NMonitoring::TDynamicCounterPtr counters)
        : Counters(counters)
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

    void HandlePoison() {
        LOG_W("Got poison, stop workload service");

        for (const auto& [_, handlerActor] : PoolIdToHandlerActor) {
            Send(handlerActor, new TEvents::TEvPoison());
        }

        PassAway();
    }

    void HandleSetConfigSubscriptionResponse() const {
        LOG_D("Subscribed for config changes");
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        const auto& event = ev->Get()->Record;

        EnabledResourcePools = event.GetConfig().GetFeatureFlags().GetEnableResourcePools();
        if (EnabledResourcePools) {
            LOG_I("Resource pools was enanbled");
            InitializeWorkloadService();
        } else {
            LOG_I("Resource pools was disabled");
        }

        auto responseEvent = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEvent.release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) const {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                LOG_C("Failed to deliver subscription request to config dispatcher");
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                LOG_E("Failed to deliver config notification response");
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

        LOG_D("Recieved new request from " << workerActorId << ", Database: " << ev->Get()->Database << ", PoolId: " << ev->Get()->PoolId << ", SessionId: " << ev->Get()->SessionId);
        Register(CreatePoolFetcherActor(std::move(ev)));
    }

    void Handle(TEvCleanupRequest::TPtr& ev) const {
        const TString& database = ev->Get()->Database;
        const TString& poolId = ev->Get()->PoolId;
        auto poolHandler = GetPoolHandler(database, poolId);
        if (!poolHandler) {
            ReplyCleanupError(ev->Sender, Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Pool " << poolId << " not found");
            return;
        }

        LOG_D("Finished request with worker actor " << ev->Sender << ", Database: " << database << ", PoolId: " << poolId << ", SessionId: " << ev->Get()->SessionId);
        Send(ev->Forward(*poolHandler));
    }

    STRICT_STFUNC(MainState,
        sFunc(TEvents::TEvPoison, HandlePoison);
        sFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleSetConfigSubscriptionResponse);
        hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);

        hFunc(TEvPlaceRequestIntoPool, Handle);
        hFunc(TEvCleanupRequest, Handle);

        hFunc(TEvPrivate::TEvFetchPoolResponse, Handle);
        hFunc(TEvPrivate::TEvRefreshPoolState, Handle);
        hFunc(TEvPrivate::TEvPrepareTablesRequest, Handle);
        hFunc(TEvPrivate::TEvCleanupTablesFinished, Handle);
        hFunc(TEvPrivate::TEvTablesCreationFinished, Handle);
    )

private:
    void Handle(TEvPrivate::TEvFetchPoolResponse::TPtr& ev) {
        auto event = std::move(ev->Get()->Event);
        const TString& poolId = event->Get()->PoolId;
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues = GroupIssues(ev->Get()->Issues, TStringBuilder() << "Failed to fetch resource pool " << poolId);
            ReplyContinueError(event->Sender, ev->Get()->Status, std::move(issues));
            return;
        }

        const TString& database = event->Get()->Database;
        LOG_D("Successfully fetched pool " << poolId << ", Database: " << database << ", SessionId: " << event->Get()->SessionId);

        auto poolHandler = GetPoolHandler(database, poolId);
        if (!poolHandler) {
            TString poolName = TStringBuilder() << database << "/" << poolId;
            LOG_I("Creating new handler for pool " << poolName);

            poolHandler = Register(CreatePoolHandlerActor(database, poolId, ev->Get()->PoolConfig, Counters->GetSubgroup("pool", poolName)));
            PoolIdToHandlerActor.insert({poolName, *poolHandler});
        }

        Send(event->Forward(*poolHandler));
    }

    void Handle(TEvPrivate::TEvRefreshPoolState::TPtr& ev) const {
        const auto& event = ev->Get()->Record;
        const TString& database = event.GetDatabase();
        const TString& poolId = event.GetPoolId();
        LOG_T("Got remote refresh request, Database: " << database << ", PoolId: " << poolId << ", NodeId: " << ev->Sender.NodeId());

        auto poolHandler = GetPoolHandler(database, poolId);
        if (poolHandler) {
            Send(ev->Forward(*poolHandler));
        }
    }

    void Handle(TEvPrivate::TEvPrepareTablesRequest::TPtr& ev) {
        const TString& database = ev->Get()->Database;
        const TString& poolId = ev->Get()->PoolId;
        LOG_T("Got create teables request, Database: " << database << ", PoolId: " << poolId);

        auto poolHandler = GetPoolHandler(database, poolId);
        if (!poolHandler) {
            return;
        }

        if (TablesCreationStatus == ETablesCreationStatus::Finished) {
            Send(*poolHandler, new TEvPrivate::TEvTablesCreationFinished(true, {}));
        } else {
            PendingHandlers.emplace(*poolHandler);
            PrepareWorkloadServiceTables();
        }
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
            OnTabelsCreated(true);
        } else {
            PrepareWorkloadServiceTables();
        }
    }

    void Handle(TEvPrivate::TEvTablesCreationFinished::TPtr& ev) {
        TablesCreationStatus = ev->Get()->Success ? ETablesCreationStatus::Finished : ETablesCreationStatus::NotStarted;

        if (ev->Get()->Success) {
            LOG_D("Succefully created tables, send response to handlers");
            OnTabelsCreated(true);
            return;
        }

        LOG_E("Failed to create tables, issues: " << ev->Get()->Issues.ToOneLineString());
        NYql::TIssues issues = GroupIssues(ev->Get()->Issues, "Failed to create workload service tables");
        OnTabelsCreated(false, issues);
    }

private:
    void InitializeWorkloadService() {
        if (ServiceInitialized) {
            return;
        }
        ServiceInitialized = true;

        LOG_I("Started workload service initialization");
        Register(CreateCleanupTablesActor());
    }

    void PrepareWorkloadServiceTables() {
        if (PendingHandlers.empty()) {
            return;
        }

        if (TablesCreationStatus == ETablesCreationStatus::NotStarted) {
            LOG_I("Started workload service tables creation");
            TablesCreationStatus = ETablesCreationStatus::Pending;
            Register(CreateTablesCreator());
        }
    }

    void OnTabelsCreated(bool success, NYql::TIssues issues = {}) {
        for (const TActorId& handlerActor : PendingHandlers) {
            Send(handlerActor, new TEvPrivate::TEvTablesCreationFinished(success, issues));
        }
        PendingHandlers.clear();
    }

private:
    void ReplyContinueError(const TActorId& replyActorId, Ydb::StatusIds::StatusCode status, const TString& message) const {
        ReplyContinueError(replyActorId, status, {NYql::TIssue(message)});
    }

    void ReplyContinueError(const TActorId& replyActorId, Ydb::StatusIds::StatusCode status, NYql::TIssues issues) const {
        LOG_W("Reply continue error " << status << " to " << replyActorId << ": " << issues.ToOneLineString());
        Send(replyActorId, new TEvContinueRequest(status, {}, {}, std::move(issues)));
    }

    void ReplyCleanupError(const TActorId& replyActorId, Ydb::StatusIds::StatusCode status, const TString& message) const {
        LOG_W("Reply cleanup error " << status << " to " << replyActorId << ": " << message);
        Send(replyActorId, new TEvCleanupResponse(status, {NYql::TIssue(message)}));
    }

    std::optional<TActorId> GetPoolHandler(const TString& database, const TString& poolId) const {
        auto poolIt = PoolIdToHandlerActor.find(TStringBuilder() << database << "/" << poolId);
        if (poolIt != PoolIdToHandlerActor.end()) {
            return poolIt->second;
        }
        return std::nullopt;
    }

    TString LogPrefix() const {
        return "[Service] ";
    }

private:
    NMonitoring::TDynamicCounterPtr Counters;

    bool EnabledResourcePools = false;
    bool ServiceInitialized = false;
    ETablesCreationStatus TablesCreationStatus = ETablesCreationStatus::Cleanup;
    std::unordered_set<TActorId> PendingHandlers;

    std::unordered_map<TString, TActorId> PoolIdToHandlerActor;
};

}  // anonymous namespace

}  // namespace NWorkload

IActor* CreateKqpWorkloadService(NMonitoring::TDynamicCounterPtr counters) {
    return new NWorkload::TKqpWorkloadService(counters);
}

}  // namespace NKikimr::NKqp
