#include "kqp_node_service.h"

#include "kqp_node_state.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/mon/mon.h>

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/kqp/runtime/kqp_write_actor_settings.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/common/shutdown/events.h>

#include <ydb/library/wilson_ids/wilson.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/async/wait_for_event.h>

#include <util/string/join.h>

namespace NKikimr {
namespace NKqp {

using namespace NActors;

namespace {

#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)

// Min interval between stats send from scan/compute actor to executor
constexpr TDuration MinStatInterval = TDuration::MilliSeconds(20);
// Max interval in case of no activety
constexpr TDuration MaxStatInterval = TDuration::Seconds(1);

template <class TTasksCollection>
TString TasksIdsStr(const TTasksCollection& tasks) {
    TVector<ui64> ids;
    for (auto& task: tasks) {
        ids.push_back(task.GetId());
    }
    return TStringBuilder() << "[" << JoinSeq(", ", ids) << "]";
}

class TKqpNodeService : public TActorBootstrapped<TKqpNodeService> {
    using TBase = TActorBootstrapped<TKqpNodeService>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_NODE_SERVICE;
    }

    TKqpNodeService(const NKikimrConfig::TTableServiceConfig& config,
        std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
        std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> caFactory,
        const TIntrusivePtr<TKqpCounters>& counters,
        NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup)
        : Config(config.GetResourceManager())
        , Counters(counters)
        , ResourceManager_(std::move(resourceManager))
        , CaFactory_(std::move(caFactory))
        , AsyncIoFactory(std::move(asyncIoFactory))
        , FederatedQuerySetup(federatedQuerySetup)
        , AccountDefaultPoolInScheduler(config.GetComputeSchedulerSettings().GetAccountDefaultPool())
    {
        if (config.HasIteratorReadsRetrySettings()) {
            SetIteratorReadsRetrySettings(config.GetIteratorReadsRetrySettings());
        }
        if (config.HasIteratorReadQuotaSettings()) {
            SetIteratorReadsQuotaSettings(config.GetIteratorReadQuotaSettings());
        }
        if (config.HasWriteActorSettings()) {
            SetWriteActorSettings(config.GetWriteActorSettings());
        }
    }

    void Bootstrap() {
        LOG_I("Starting KQP Node service");

        State_ = std::make_shared<TNodeState>();

        // Subscribe for TableService config changes
        ui32 tableServiceConfigKind = (ui32) NKikimrConsole::TConfigItem::TableServiceConfigItem;
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({tableServiceConfigKind}),
             IEventHandle::FlagTrackDelivery);

        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "kqp_node", "KQP Node", false,
                TActivationContext::ActorSystem(), SelfId());
        }

        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
        Become(&TKqpNodeService::WorkState);
    }

private:
    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpNode::TEvStartKqpTasksRequest, HandleWork);
            hFunc(TEvKqpNode::TEvFinishKqpTask, HandleWork); // used only for unit tests
            hFunc(TEvKqpNode::TEvCancelKqpTasksRequest, HandleWork);
            hFunc(TEvents::TEvWakeup, HandleWork);
            hFunc(TEvKqp::TEvInitiateShutdownRequest, HandleWork);
            // misc
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleWork);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleWork);
            hFunc(TEvents::TEvUndelivered, HandleWork);
            hFunc(TEvents::TEvPoison, HandleWork);
            hFunc(NMon::TEvHttpInfo, HandleWork);
            default: {
                Y_ABORT("Unexpected event 0x%x for TKqpNodeService", ev->GetTypeRewrite());
            }
        }
    }

    STATEFN(ShuttingDownState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvKqpNode::TEvStartKqpTasksRequest, HandleShuttingDown);
            default: {
                LOG_D("Unexpected event" << ev->GetTypeName() << " for TKqpNodeService");
            }
        }
    }

    static constexpr double SecToUsec = 1e6;

    void HandleWork(TEvKqpNode::TEvStartKqpTasksRequest::TPtr ev) {
        NWilson::TSpan sendTasksSpan(TWilsonKqp::KqpNodeSendTasks, NWilson::TTraceId(ev->TraceId), "KqpNode.SendTasks", NWilson::EFlags::AUTO_END);

        NHPTimer::STime workHandlerStart = ev->SendTime;
        auto& msg = ev->Get()->Record;
        Counters->NodeServiceStartEventDelivery->Collect(NHPTimer::GetTimePassed(&workHandlerStart) * SecToUsec);

        auto requester = ev->Sender;

        ui64 txId = msg.GetTxId();
        TMaybe<ui64> lockTxId = msg.HasLockTxId()
            ? TMaybe<ui64>(msg.GetLockTxId())
            : Nothing();
        ui32 lockNodeId = msg.GetLockNodeId();
        TMaybe<NKikimrDataEvents::ELockMode> lockMode = msg.HasLockMode()
            ? TMaybe<NKikimrDataEvents::ELockMode>(msg.GetLockMode())
            : Nothing();

        YQL_ENSURE(msg.GetStartAllOrFail()); // TODO: support partial start

        LOG_D("TxId: " << txId << ", new compute tasks request from " << requester
            << " with " << msg.GetTasks().size() << " tasks: " << TasksIdsStr(msg.GetTasks()));

        const auto& poolId = msg.GetPoolId().empty() ? NResourcePool::DEFAULT_POOL_ID : msg.GetPoolId();
        const auto& databaseId = msg.GetDatabaseId();

        NScheduler::NHdrf::NDynamic::TQueryPtr query;
        if (!databaseId.empty() && (poolId != NResourcePool::DEFAULT_POOL_ID || AccountDefaultPoolInScheduler)) {
            const auto schedulerServiceId = MakeKqpSchedulerServiceId(SelfId().NodeId());

            // TODO: deliberately create the database here - since database doesn't have any useful scheduling properties for now.
            //       Replace with more precise database events in the future.
            auto addDatabaseEvent = MakeHolder<NScheduler::TEvAddDatabase>();
            addDatabaseEvent->Id = databaseId;
            this->Send(schedulerServiceId, addDatabaseEvent.Release());

            // TODO: replace with more precise pool events.
            auto addPoolEvent = MakeHolder<NScheduler::TEvAddPool>(databaseId, poolId);
            this->Send(schedulerServiceId, addPoolEvent.Release());

            auto addQueryEvent = MakeHolder<NScheduler::TEvAddQuery>();
            addQueryEvent->DatabaseId = databaseId;
            addQueryEvent->PoolId = poolId;
            addQueryEvent->QueryId = txId;
            Send(schedulerServiceId, addQueryEvent.Release(), 0, txId);

            query = (co_await ActorWaitForEvent<NScheduler::TEvQueryResponse>(txId))->Get()->Query;
        }

        const auto now = TAppData::TimeProvider->Now();
        const auto executerId = ev->Sender;
        auto request = TNodeRequest(txId, query, executerId, now);
        auto& runtimeSettings = msg.GetRuntimeSettings();
        if (runtimeSettings.GetTimeoutMs() > 0) {
            // compute actor should not arm timer since in case of timeout it will receive TEvAbortExecution from Executer
            auto timeout = TDuration::MilliSeconds(runtimeSettings.GetTimeoutMs());
            request.Deadline = now + timeout + /* gap */ TDuration::Seconds(5);
        }

        if (State_->HasRequest(txId)) {
            LOG_E("TxId: " << txId << ", requester: " << requester << ", request already exists");
            co_return ReplyError(txId, request.ExecuterId, msg, NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR, ev->Cookie);
        }

        for (const auto& dqTask : msg.GetTasks()) {
            request.Tasks.emplace(dqTask.GetId(), std::nullopt);
        }
        State_->AddRequest(std::move(request));

        NRm::EKqpMemoryPool memoryPool;
        if (msg.GetRuntimeSettings().GetExecType() == NYql::NDqProto::TComputeRuntimeSettings::SCAN) {
            memoryPool = NRm::EKqpMemoryPool::ScanQuery;
        } else if (msg.GetRuntimeSettings().GetExecType() == NYql::NDqProto::TComputeRuntimeSettings::DATA) {
            memoryPool = NRm::EKqpMemoryPool::DataQuery;
        } else {
            memoryPool = NRm::EKqpMemoryPool::Unspecified;
        }

        auto reply = MakeHolder<TEvKqpNode::TEvStartKqpTasksResponse>();
        reply->Record.SetTxId(txId);

        NYql::NDq::TComputeRuntimeSettings runtimeSettingsBase;
        runtimeSettingsBase.ReportStatsSettings = NYql::NDq::TReportStatsSettings{MinStatInterval, MaxStatInterval};

        TShardsScanningPolicy scanPolicy(Config.GetShardsScanningPolicy());

        NComputeActor::TComputeStagesWithScan computesByStage;

        const TString& serializedGUCSettings = ev->Get()->Record.HasSerializedGUCSettings() ?
            ev->Get()->Record.GetSerializedGUCSettings() : "";

        // start compute actors
        TMaybe<NYql::NDqProto::TRlPath> rlPath = Nothing();
        if (runtimeSettings.HasRlPath()) {
            rlPath.ConstructInPlace(runtimeSettings.GetRlPath());
        }

        TIntrusivePtr<NRm::TTxState> txInfo = MakeIntrusive<NRm::TTxState>(
            txId, TInstant::Now(), ResourceManager_->GetCounters(),
            poolId, msg.GetMemoryPoolPercent(),
            msg.GetDatabase(), Config.GetVerboseMemoryLimitException());

        const ui32 tasksCount = msg.GetTasks().size();
        for (auto& dqTask: *msg.MutableTasks()) {
            const auto taskId = dqTask.GetId();

            NComputeActor::IKqpNodeComputeActorFactory::TCreateArgs createArgs{
                .ExecuterId = executerId,
                .TxId = txId,
                .LockTxId = lockTxId,
                .LockNodeId = lockNodeId,
                .LockMode = lockMode,
                .Task = &dqTask,
                .TxInfo = txInfo,
                .RuntimeSettings = runtimeSettingsBase,
                .TraceId = NWilson::TTraceId(ev->TraceId),
                .Arena = ev->Get()->Arena,
                .SerializedGUCSettings = serializedGUCSettings,
                .NumberOfTasks = tasksCount,
                .OutputChunkMaxSize = msg.GetOutputChunkMaxSize(),
                .MemoryPool = memoryPool,
                .WithSpilling = runtimeSettings.GetUseSpilling(),
                .StatsMode = runtimeSettings.GetStatsMode(),
                .WithProgressStats = runtimeSettings.GetWithProgressStats(),
                .Deadline = TInstant(),
                .ShareMailbox = false,
                .RlPath = rlPath,
                .ComputesByStages = &computesByStage,
                .State = State_, // pass state to later inform when task is finished
                .Database = msg.GetDatabase(),
                .Query = query,
                // TODO: block tracking mode is not set!
            };
            if (msg.HasUserToken() && msg.GetUserToken()) {
                createArgs.UserToken.Reset(MakeIntrusive<NACLib::TUserToken>(msg.GetUserToken()));
            }

            auto result = CaFactory_->CreateKqpComputeActor(std::move(createArgs));

            // NOTE: keep in mind that a task can start, execute and finish before we reach the end of this method.

            if (const auto* rmResult = std::get_if<NRm::TKqpRMAllocateResult>(&result)) {
                ReplyError(txId, executerId, msg, rmResult->GetStatus(), ev->Cookie, rmResult->GetFailReason());
                TerminateTx(txId, rmResult->GetFailReason());
                co_return;
            }

            TActorId* actorId = std::get_if<TActorId>(&result);
            auto* startedTask = reply->Record.AddStartedTasks();
            Y_ENSURE(actorId);

            startedTask->SetTaskId(taskId);
            ActorIdToProto(*actorId, startedTask->MutableActorId());
            if (State_->OnTaskStarted(txId, taskId, *actorId, executerId)) {
                LOG_D("TxId: " << txId << ", executing task: " << taskId << " on compute actor: " << *actorId);
            } else {
                LOG_D("TxId: " << txId << ", task finished in an instant: " << taskId << " on compute actor: " << *actorId);
            }
        }

        TCPULimits cpuLimits;
        if (msg.GetPoolMaxCpuShare() > 0) {
            // Share <= 0 means disabled limit
            cpuLimits.DeserializeFromProto(msg).Validate();
        }

        for (auto&& i : computesByStage) {
            for (auto&& m : i.second.MutableMetaInfo()) {
                Register(CreateKqpScanFetcher(msg.GetSnapshot(), std::move(m.MutableActorIds()),
                    m.GetMeta(), runtimeSettingsBase, msg.GetDatabase(), txId, lockTxId, lockNodeId, lockMode,
                    scanPolicy, Counters, NWilson::TTraceId(ev->TraceId), cpuLimits));
            }
        }

        Send(executerId, reply.Release(), IEventHandle::FlagTrackDelivery, txId);

        Counters->NodeServiceProcessTime->Collect(NHPTimer::GetTimePassed(&workHandlerStart) * SecToUsec);
    }

    // used only for unit tests
    void HandleWork(TEvKqpNode::TEvFinishKqpTask::TPtr& ev) {
        auto& message = *ev->Get();
        if (auto tasksToAbort = State_->GetTasksByTxId(message.TxId); !tasksToAbort.empty()) {
            TStringBuilder finalReason;
            finalReason << "Node service cancelled the task, because of direct request "
                << ", NodeId: "<< SelfId().NodeId()
                << ", TxId: " << message.TxId;
            LOG_E(finalReason);

            for (const auto& [taskId, computeActorId]: tasksToAbort) {
                if (message.TaskId != taskId) {
                    continue;
                }

                auto abortEv = std::make_unique<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::ABORTED, finalReason);
                Send(computeActorId, abortEv.release());
            }
        }
    }

    void HandleWork(TEvKqpNode::TEvCancelKqpTasksRequest::TPtr& ev) {
        THPTimer timer;
        ui64 txId = ev->Get()->Record.GetTxId();
        auto& reason = ev->Get()->Record.GetReason();

        LOG_W("TxId: " << txId << ", terminate transaction, reason: " << reason);
        TerminateTx(txId, reason);

        Counters->NodeServiceProcessCancelTime->Collect(timer.Passed() * SecToUsec);
    }

    void TerminateTx(ui64 txId, const TString& reason, NYql::NDqProto::StatusIds_StatusCode status = NYql::NDqProto::StatusIds::UNSPECIFIED) {
        if (auto tasksToAbort = State_->GetTasksByTxId(txId); !tasksToAbort.empty()) {
            TStringBuilder finalReason;
            finalReason << "node service cancelled the task, because it " << reason
                << ", NodeId: "<< SelfId().NodeId()
                << ", TxId: " << txId;

            LOG_E(finalReason);
            for (const auto& [taskId, computeActorId]: tasksToAbort) {
                auto abortEv = std::make_unique<TEvKqp::TEvAbortExecution>(status, reason);
                Send(computeActorId, abortEv.release());
            }
        }
    }

    void HandleWork(TEvents::TEvWakeup::TPtr& ev) {
        Schedule(TDuration::Seconds(1), ev->Release().Release());
        auto expiredRequests = State_->ClearExpiredRequests();
        for (auto txId : expiredRequests) {
            TerminateTx(txId, "reached execution deadline", NYql::NDqProto::StatusIds::TIMEOUT);
        }
    }

    void HandleWork(TEvKqp::TEvInitiateShutdownRequest::TPtr& ev) {
        LOG_I("Prepare to shutdown: do not acccept any messages from this time");
        ShutdownState_.Reset(ev->Get()->ShutdownState.Get());
        Become(&TKqpNodeService::ShuttingDownState);
    }

    void HandleShuttingDown(TEvKqpNode::TEvStartKqpTasksRequest::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        if (msg.HasSupportShuttingDown() && msg.GetSupportShuttingDown()) {
            LOG_D("Can't handle StartRequest TxId" << msg.GetTxId() << " in ShuttingDown State");
            ReplyError(msg.GetTxId(), ev->Sender, msg, NKikimrKqp::TEvStartKqpTasksResponse::NODE_SHUTTING_DOWN, ev->Cookie);
        } else {
            HandleWork(ev);
        }
    }
private:
    static void HandleWork(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        LOG_D("Subscribed for config changes");
    }

    void HandleWork(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        auto &event = ev->Get()->Record;

        if (event.GetConfig().GetTableServiceConfig().GetResourceManager().IsInitialized()) {
            Config.Swap(event.MutableConfig()->MutableTableServiceConfig()->MutableResourceManager());

#define FORCE_VALUE(name) if (!Config.Has ## name ()) Config.Set ## name(Config.Get ## name());
            FORCE_VALUE(ComputeActorsCount)
            FORCE_VALUE(ChannelBufferSize)
            FORCE_VALUE(MkqlLightProgramMemoryLimit)
            FORCE_VALUE(MkqlHeavyProgramMemoryLimit)
            FORCE_VALUE(QueryMemoryLimit)
            FORCE_VALUE(PublishStatisticsIntervalSec);
            FORCE_VALUE(MaxTotalChannelBuffersSize);
            FORCE_VALUE(MinChannelBufferSize);
            FORCE_VALUE(MinMemAllocSize);
            FORCE_VALUE(MinMemFreeSize);
#undef FORCE_VALUE

            LOG_I("Updated table service config: " << Config.DebugString());
        }

        CaFactory_->ApplyConfig(event.GetConfig().GetTableServiceConfig().GetResourceManager());

        if (event.GetConfig().GetTableServiceConfig().HasIteratorReadsRetrySettings()) {
            SetIteratorReadsRetrySettings(event.GetConfig().GetTableServiceConfig().GetIteratorReadsRetrySettings());
        }

        if (event.GetConfig().GetTableServiceConfig().HasIteratorReadQuotaSettings()) {
            SetIteratorReadsQuotaSettings(event.GetConfig().GetTableServiceConfig().GetIteratorReadQuotaSettings());
        }

        if (event.GetConfig().GetTableServiceConfig().HasWriteActorSettings()) {
            SetWriteActorSettings(event.GetConfig().GetTableServiceConfig().GetWriteActorSettings());
        }

        auto responseEv = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEv.Release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void SetIteratorReadsQuotaSettings(const NKikimrConfig::TTableServiceConfig::TIteratorReadQuotaSettings& settings) {
        SetDefaultIteratorQuotaSettings(settings.GetMaxRows(), settings.GetMaxBytes());
    }

    void SetIteratorReadsRetrySettings(const NKikimrConfig::TTableServiceConfig::TIteratorReadsRetrySettings& settings) {
        auto ptr = MakeIntrusive<NKikimr::NKqp::TIteratorReadBackoffSettings>();
        ptr->StartRetryDelay = TDuration::MilliSeconds(settings.GetStartDelayMs());
        ptr->MaxShardAttempts = settings.GetMaxShardRetries();
        ptr->MaxShardResolves = settings.GetMaxShardResolves();
        ptr->UnsertaintyRatio = settings.GetUnsertaintyRatio();
        ptr->Multiplier = settings.GetMultiplier();
        if (settings.GetMaxTotalRetries()) {
            ptr->MaxTotalRetries = settings.GetMaxTotalRetries();
        }
        if (settings.GetIteratorResponseTimeoutMs()) {
            ptr->ReadResponseTimeout = TDuration::MilliSeconds(settings.GetIteratorResponseTimeoutMs());
        }
        ptr->MaxRetryDelay = TDuration::MilliSeconds(settings.GetMaxDelayMs());
        ptr->MaxRowsProcessingStreamLookup = settings.GetMaxRowsProcessingStreamLookup();
        ptr->MaxTotalBytesQuotaStreamLookup = settings.GetMaxTotalBytesQuotaStreamLookup();
        SetReadIteratorBackoffSettings(ptr);
    }

    void SetWriteActorSettings(const NKikimrConfig::TTableServiceConfig::TWriteActorSettings& settings) {
        auto ptr = MakeIntrusive<NKikimr::NKqp::TWriteActorSettings>();

        ptr->InFlightMemoryLimitPerActorBytes = settings.GetInFlightMemoryLimitPerActorBytes();

        ptr->StartRetryDelay = TDuration::MilliSeconds(settings.GetStartRetryDelayMs());
        ptr->MaxRetryDelay = TDuration::MilliSeconds(settings.GetMaxRetryDelayMs());
        ptr->UnsertaintyRatio = settings.GetUnsertaintyRatio();
        ptr->Multiplier = settings.GetMultiplier();

        ptr->MaxWriteAttempts = settings.GetMaxWriteAttempts();
        ptr->MaxResolveAttempts = settings.GetMaxResolveAttempts();

        NKikimr::NKqp::SetWriteActorSettings(ptr);
    }

    void HandleWork(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case TEvKqpNode::TEvStartKqpTasksResponse::EventType: {
                ui64 txId = ev->Cookie;
                TStringBuilder reason;
                reason << "executer lost: " << (int) ev->Get()->Reason;
                TerminateTx(txId, reason, NYql::NDqProto::StatusIds::ABORTED);
                break;
            }

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

    void HandleWork(TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    void HandleWork(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;
        HTML(str) {
            PRE() {
                str << "Current config:" << Endl;
                str << Config.DebugString() << Endl;
                str << Endl;

                str << Endl << "Transactions:" << Endl;
                State_->DumpInfo(str);
            }
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

private:
    void ReplyError(ui64 txId, TActorId executer, const NKikimrKqp::TEvStartKqpTasksRequest& request,
        NKikimrKqp::TEvStartKqpTasksResponse::ENotStartedTaskReason reason, ui64 requestId, const TString& message = "")
    {
        auto ev = MakeHolder<TEvKqpNode::TEvStartKqpTasksResponse>();
        ev->Record.SetTxId(txId);
        for (auto& task : request.GetTasks()) {
            auto* resp = ev->Record.AddNotStartedTasks();
            resp->SetTaskId(task.GetId());
            resp->SetReason(reason);
            resp->SetMessage(message);
            resp->SetRequestId(requestId);
        }
        Send(executer, ev.Release());
    }

private:
    NKikimrConfig::TTableServiceConfig::TResourceManager Config;
    TIntrusivePtr<TKqpCounters> Counters;
    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager_;
    std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> CaFactory_;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    const std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;

    // state sharded by TxId
    std::shared_ptr<TNodeState> State_;
    TIntrusivePtr<TKqpShutdownState> ShutdownState_;
    bool AccountDefaultPoolInScheduler = false;
};


} // anonymous namespace

IActor* CreateKqpNodeService(const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
    std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> caFactory,
    TIntrusivePtr<TKqpCounters> counters, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup)
{
    return new TKqpNodeService(tableServiceConfig, std::move(resourceManager), std::move(caFactory),
        counters, std::move(asyncIoFactory), federatedQuerySetup);
}

} // namespace NKqp
} // namespace NKikimr
