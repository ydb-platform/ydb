#include "kqp_node_service.h"

#include "kqp_node_state.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/util/stlog.h>

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
#define STLOG_C(MESSAGE, ...) STLOG(PRI_CRIT, NKikimrServices::KQP_NODE, KQPNS, MESSAGE, __VA_ARGS__)
#define STLOG_E(MESSAGE, ...) STLOG(PRI_ERROR, NKikimrServices::KQP_NODE, KQPNS, MESSAGE, __VA_ARGS__)
#define STLOG_W(MESSAGE, ...) STLOG(PRI_WARN, NKikimrServices::KQP_NODE, KQPNS, MESSAGE, __VA_ARGS__)
#define STLOG_N(MESSAGE, ...) STLOG(PRI_NOTICE, NKikimrServices::KQP_NODE, KQPNS, MESSAGE, __VA_ARGS__)
#define STLOG_I(MESSAGE, ...) STLOG(PRI_INFO, NKikimrServices::KQP_NODE, KQPNS, MESSAGE, __VA_ARGS__)
#define STLOG_D(MESSAGE, ...) STLOG(PRI_DEBUG, NKikimrServices::KQP_NODE, KQPNS, MESSAGE, __VA_ARGS__)
#define STLOG_T(MESSAGE, ...) STLOG(PRI_TRACE, NKikimrServices::KQP_NODE, KQPNS, MESSAGE, __VA_ARGS__)

// Min interval between stats send from scan/compute actor to executor
constexpr TDuration MinStatInterval = TDuration::MilliSeconds(20);
// Max interval in case of no activity
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
        STLOG_I("Starting KQP Node service",
            (node_id, SelfId().NodeId()));

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
                Y_ABORT("Unexpected event 0x%x for TKqpNodeService in WorkState", ev->GetTypeRewrite());
            }
        }
    }

    STATEFN(ShuttingDownState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvKqpNode::TEvStartKqpTasksRequest, HandleShuttingDown);
            hFunc(TEvKqpNode::TEvCancelKqpTasksRequest , HandleWork);

            // misc
            hFunc(TEvents::TEvWakeup, HandleShuttingDown);
            hFunc(TEvents::TEvPoison, HandleShuttingDown);
            hFunc(NMon::TEvHttpInfo, HandleShuttingDown);
            hFunc(TEvents::TEvUndelivered, HandleWork);

            IgnoreFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
            IgnoreFunc(NConsole::TEvConsole::TEvConfigNotificationRequest);
            
            default: {
                STLOG_W("Ignoring unexpected event 0x%x (" << ev->GetTypeName() 
                    << ") during graceful shutdown",
                    (node_id, SelfId().NodeId()),
                    (sender, ev->Sender));
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

        STLOG_D("HandleStartKqpTasksRequest",
            (node_id, SelfId().NodeId()),
            (tx_id, txId),
            (requester, requester),
            (tasks_count, msg.GetTasks().size()),
            (task_ids, TasksIdsStr(msg.GetTasks())),
            (trace_id, ev->TraceId.GetHexTraceId()));

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

        TVector<ui64> requestTaskIds;
        for (const auto& dqTask : msg.GetTasks()) {
            requestTaskIds.push_back(dqTask.GetId());
        }

        bool isLocalRequest = (ev->Sender.NodeId() == SelfId().NodeId());
        
        if (State_->HasRequest(txId)) {
            if (State_->IsRequestCancelled(txId, executerId)) {
                co_return ReplyError(txId, executerId, msg, NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR, 
                    ev->Cookie, "Request was cancelled");
            }
            if (isLocalRequest && State_->AddTasksToRequest(txId, executerId, requestTaskIds)) {
                STLOG_D("Added tasks to existing local request",
                    (node_id, SelfId().NodeId()),
                    (tx_id, txId),
                    (tasks_count, requestTaskIds.size()),
                    (executer, executerId),
                    (trace_id, ev->TraceId.GetHexTraceId()));
            } else {
                STLOG_D("Creating new request",
                    (node_id, SelfId().NodeId()),
                    (tx_id, txId),
                    (tasks_count, requestTaskIds.size()),
                    (executer, executerId),
                    (is_local, isLocalRequest),
                    (trace_id, ev->TraceId.GetHexTraceId()));
                for (ui64 taskId : requestTaskIds) {
                    request.Tasks.emplace(taskId, std::nullopt);
                }
                State_->AddRequest(std::move(request));
            }
        } else {
            STLOG_D("Creating new request",
                (node_id, SelfId().NodeId()),
                (tx_id, txId),
                (tasks_count, requestTaskIds.size()),
                (executer, executerId),
                (is_local, isLocalRequest),
                (trace_id, ev->TraceId.GetHexTraceId()));
            for (ui64 taskId : requestTaskIds) {
                request.Tasks.emplace(taskId, std::nullopt);
            }
            State_->AddRequest(std::move(request));
        }

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
        runtimeSettingsBase.ReportStatsSettings = NYql::NDq::TReportStatsSettings{
            .MinInterval = runtimeSettings.HasMinStatsSendIntervalMs() ? TDuration::MilliSeconds(runtimeSettings.GetMinStatsSendIntervalMs()) : MinStatInterval,
            .MaxInterval = runtimeSettings.HasMaxStatsSendIntervalMs() ? TDuration::MilliSeconds(runtimeSettings.GetMaxStatsSendIntervalMs()) : MaxStatInterval,
        };

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
                STLOG_D("Executing task",
                    (node_id, SelfId().NodeId()),
                    (tx_id, txId),
                    (task_id, taskId),
                    (compute_actor_id, *actorId),
                    (trace_id, ev->TraceId.GetHexTraceId()));
            } else {
                STLOG_D("Task finished in an instant",
                    (node_id, SelfId().NodeId()),
                    (tx_id, txId),
                    (task_id, taskId),
                    (compute_actor_id, *actorId),
                    (trace_id, ev->TraceId.GetHexTraceId()));
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
                STLOG_E(finalReason,
                    (node_id, SelfId().NodeId()));


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

        STLOG_W("Terminate transaction",
            (node_id, SelfId().NodeId()),
            (tx_id, txId),
            (reason, reason));
        TerminateTx(txId, reason);

        Counters->NodeServiceProcessCancelTime->Collect(timer.Passed() * SecToUsec);
    }

    void TerminateTx(ui64 txId, const TString& reason, NYql::NDqProto::StatusIds_StatusCode status = NYql::NDqProto::StatusIds::UNSPECIFIED) {
        State_->MarkRequestAsCancelled(txId);

        if (auto tasksToAbort = State_->GetTasksByTxId(txId); !tasksToAbort.empty()) {
            STLOG_E("Node service cancelled the task, because it " << reason,
                (node_id, SelfId().NodeId()),
                (tx_id, txId));
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
        if (!AppData()->FeatureFlags.GetEnableShuttingDownNodeState()) {
            STLOG_I("Feature flag EnableShuttingDownNodeState is disabled, ignoring shutdown request",
                (node_id, SelfId().NodeId()));
            return;
        }
        STLOG_I("Prepare to shutdown: do not accept any messages from this time",
            (node_id, SelfId().NodeId()));
        ShutdownState_.Reset(ev->Get()->ShutdownState.Get());
        Become(&TKqpNodeService::ShuttingDownState);
    }

    void HandleShuttingDown(TEvKqpNode::TEvStartKqpTasksRequest::TPtr& ev) {
        // in shutting down state do not accept new tasks, but accept local requests
        // continue to process tasks that are already started before shutdown
        auto& msg = ev->Get()->Record;        
        if (ev->Sender.NodeId() == SelfId().NodeId()) {
            STLOG_D("Accepting local StartRequest during shutdown",
                (node_id, SelfId().NodeId()),
                (tx_id, msg.GetTxId()));
            HandleWork(ev);
        } else if (msg.HasSupportShuttingDown() && msg.GetSupportShuttingDown()) {
            STLOG_D("Rejecting remote StartRequest in ShuttingDown State",
                (node_id, SelfId().NodeId()),
                (tx_id, msg.GetTxId()));
            ReplyError(msg.GetTxId(), ev->Sender, msg, NKikimrKqp::TEvStartKqpTasksResponse::NODE_SHUTTING_DOWN, ev->Cookie);
        } else {
            HandleWork(ev);
        }
    }

    void HandleShuttingDown(TEvents::TEvWakeup::TPtr& ev) {
        HandleWork(ev);
    }
    void HandleShuttingDown(TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    void HandleShuttingDown(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;
        HTML(str) {
            PRE() {
                str << "This node is in graceful shutdown mode and will not accept new requests." << Endl;
                str << "Current config:" << Endl;
                str << Config.DebugString() << Endl;
                str << Endl;

                str << "Active Transactions:" << Endl;
                State_->DumpInfo(str);
                str << Endl;
            }
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }
private:
    static void HandleWork(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        STLOG_D("Subscribed for config changes");
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

            STLOG_I("Updated table service config",
                (node_id, SelfId().NodeId()),
                (config, Config.DebugString()));
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
                STLOG_C("Failed to deliver subscription request to config dispatcher",
                    (node_id, SelfId().NodeId()));
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                STLOG_E("Failed to deliver config notification response",
                    (node_id, SelfId().NodeId()));
                break;

            default:
                STLOG_E("Undelivered event with unexpected source type",
                    (node_id, SelfId().NodeId()),
                    (source_type, ev->Get()->SourceType));
                break;
        }
    }

    void HandleWork(TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    void HandleWork(NMon::TEvHttpInfo::TPtr& ev) {

        const TCgiParameters &cgi = ev->Get()->Request.GetParams();
        TActorId id;

        auto caId = cgi.Get("ca");
        if (caId && State_->ValidateComputeActorId(caId, id)) {
            TActivationContext::Send(ev->Forward(id));
            return;
        }

        auto exId = cgi.Get("ex");
        if (exId && State_->ValidateKqpExecuterId(exId, SelfId().NodeId(), id)) {
            TActivationContext::Send(ev->Forward(id));
            return;
        }

        TStringStream str;
        HTML(str) {
            PRE() {
                str << "TKqpNodeService, SelfId=" << SelfId() << Endl;
                str << Endl << "Current config:" << Endl;
                str << Config.DebugString() << Endl;
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
