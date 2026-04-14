#include "kqp_node_service.h"
#include "kqp_query_control_plane.h"

#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/wilson/wilson_span.h>

#include <ydb/library/wilson_ids/wilson.h>

#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>

namespace NKikimr::NKqp {

template <class TTasksCollection>
TString TasksIdsStr(const TTasksCollection& tasks) {
    TVector<ui64> ids;
    for (auto& task: tasks) {
        ids.push_back(task.GetId());
    }
    return TStringBuilder() << "[" << JoinSeq(", ", ids) << "]";
}

class TKqpQueryManager : public NActors::TActor<TKqpQueryManager> {
public:
    TKqpQueryManager(TIntrusivePtr<TKqpCounters>& counters, std::shared_ptr<TNodeState>& state, std::shared_ptr<NRm::IKqpResourceManager>& resourceManager, std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory>& caFactory)
        : TActor(&TThis::StateFunc)
        , Counters_(counters)
        , State_(state)
        , ResourceManager_(resourceManager)
        , CaFactory_(caFactory)
    {
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvPoison, HandlePoison);
            hFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
            hFunc(NActors::TEvents::TEvUndelivered, HandleUndelivered);
            hFunc(TEvKqpNode::TEvStartKqpTasksRequest, HandleStart);
        }
    }

    void HandlePoison(NActors::TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        SendProfileStats();
        Schedule(StatsReportPeriod, new TEvents::TEvWakeup());
    }

    void HandleUndelivered(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case NYql::NDq::TDqComputeEvents::EvNodeState:
                PassAway();
                break;
        }
    }

    void HandleStart(TEvKqpNode::TEvStartKqpTasksRequest::TPtr ev) {
        NWilson::TSpan createTasksSpan(TWilsonKqp::KqpNodeCreateTasks, NWilson::TTraceId(ev->TraceId), "KqpNode.CreateTasks", NWilson::EFlags::AUTO_END);
        NHPTimer::STime workHandlerStart = ev->SendTime;
        Counters_->NodeServiceStartEventDelivery->Collect(NHPTimer::GetTimePassed(&workHandlerStart) * SecToUsec);

        const auto executerId = ev->Sender;
        auto& msg = ev->Get()->Record;

        if (ExecuterId) {
            YQL_ENSURE(ExecuterId == executerId);
        } else {
            ExecuterId = executerId;
        }
        YQL_ENSURE(msg.GetStartAllOrFail()); // TODO: support partial start
        YQL_ENSURE(!msg.GetTasks().empty(), "TEvStartKqpTasksRequest with empty task list");

        ui64 txId = msg.GetTxId();
        TMaybe<ui64> lockTxId = msg.HasLockTxId() ? TMaybe<ui64>(msg.GetLockTxId()) : Nothing();
        ui32 lockNodeId = msg.GetLockNodeId();
        TMaybe<NKikimrDataEvents::ELockMode> lockMode = msg.HasLockMode() ? TMaybe<NKikimrDataEvents::ELockMode>(msg.GetLockMode()) : Nothing();

        STLOG_D("HandleStartKqpTasksRequest",
            (node_id, SelfId().NodeId()),
            (tx_id, txId),
            (requester, executerId),
            (tasks_count, msg.GetTasks().size()),
            (task_ids, TasksIdsStr(msg.GetTasks())),
            (trace_id, ev->TraceId.GetHexTraceIdLowerCase()));

        const auto& poolId = msg.GetPoolId().empty() ? NResourcePool::DEFAULT_POOL_ID : msg.GetPoolId();
        const auto& databaseId = msg.GetDatabaseId();

        NScheduler::NHdrf::NDynamic::TQueryPtr query;
        if (!databaseId.empty() && (poolId != NResourcePool::DEFAULT_POOL_ID || CaFactory_->AccountDefaultPoolInScheduler.load())) {
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

        auto& runtimeSettings = msg.GetRuntimeSettings();

        const auto now = TAppData::TimeProvider->Now();
        TInstant deadline;
        if (runtimeSettings.GetTimeoutMs() > 0) {
            // compute actor should not arm timer since in case of timeout it will receive TEvAbortExecution from Executer
            deadline = now + TDuration::MilliSeconds(runtimeSettings.GetTimeoutMs()) + /* gap */ TDuration::Seconds(5);
        }

        std::vector<ui64> tasks;
        tasks.reserve(msg.GetTasks().size());
        for (const auto& dqTask : msg.GetTasks()) {
            tasks.push_back(dqTask.GetId());
        }

        ui64 taskCount = 0;
        if (!State_->UpdateRequest(executerId, txId, query, now, deadline, tasks, taskCount)) {
            co_return ReplyError(msg, NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR,
                ev->Cookie, "Request was cancelled");
        }

        STLOG_D(((tasks.size() == taskCount) ? "Created new request" : "Added tasks to existing request"),
                (node_id, SelfId().NodeId()),
                (tx_id, txId),
                (tasks_count, tasks.size()),
                (executer, executerId),
                (trace_id, ev->TraceId.GetHexTraceIdLowerCase()));

        auto reply = MakeHolder<TEvKqpNode::TEvStartKqpTasksResponse>();
        reply->Record.SetTxId(txId);

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
            msg.GetDatabase(),  CaFactory_->GetVerboseMemoryLimitException());

        auto reportStatsSettings = ReportStatsSettingsFromProto(runtimeSettings);

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
                .ReportStatsSettings = reportStatsSettings,
                .TraceId = NWilson::TTraceId(ev->TraceId),
                .Arena = ev->Get()->Arena,
                .SerializedGUCSettings = serializedGUCSettings,
                .NumberOfTasks = tasksCount,
                .OutputChunkMaxSize = msg.GetOutputChunkMaxSize(),
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

            if (const auto* rmResult = std::get_if<NRm::TKqpRMAllocateResult>(&result)) {

                ReplyError(msg, rmResult->GetStatus(), ev->Cookie, rmResult->GetFailReason());

                // TerminateTx(txId, executerId, rmResult->GetFailReason());
                State_->MarkRequestAsCancelled(executerId);

                if (auto tasksToAbort = State_->GetTasksByExecuterId(executerId); !tasksToAbort.empty()) {
                    STLOG_E("Node service cancelled the task, because it " << rmResult->GetFailReason(),
                        (node_id, SelfId().NodeId()),
                        (tx_id, txId));
                    for (const auto& [taskId, computeActorId]: tasksToAbort) {
                        auto abortEv = std::make_unique<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::UNSPECIFIED, rmResult->GetFailReason());
                        Send(computeActorId, abortEv.release());
                    }
                }

                co_return;
            }

            TActorId* actorId = std::get_if<TActorId>(&result);
            auto* startedTask = reply->Record.AddStartedTasks();
            Y_ENSURE(actorId);

            startedTask->SetTaskId(taskId);
            ActorIdToProto(*actorId, startedTask->MutableActorId());
            if (State_->OnTaskStarted(executerId, taskId, *actorId)) {
                STLOG_D("Executing task",
                    (node_id, SelfId().NodeId()),
                    (tx_id, txId),
                    (task_id, taskId),
                    (compute_actor_id, *actorId),
                    (trace_id, ev->TraceId.GetHexTraceIdLowerCase()));
            } else {
                STLOG_D("Task finished in an instant",
                    (node_id, SelfId().NodeId()),
                    (tx_id, txId),
                    (task_id, taskId),
                    (compute_actor_id, *actorId),
                    (trace_id, ev->TraceId.GetHexTraceIdLowerCase()));
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
                    m.GetMeta(), NYql::NDq::TComputeRuntimeSettings(), msg.GetDatabase(), txId, lockTxId, lockNodeId, lockMode,
                    CaFactory_->GetShardsScanningPolicy(), Counters_, NWilson::TTraceId(ev->TraceId), cpuLimits));
            }
        }

        if (StatsMode == NYql::NDqProto::DQ_STATS_MODE_NONE) {
            StatsMode = runtimeSettings.GetStatsMode();
            if (StatsMode == NYql::NDqProto::DQ_STATS_MODE_PROFILE) {
                StatsReportPeriod = reportStatsSettings.MinInterval;

                auto channelCounters = Counters_->GetChannelCounters();
                InputBufferInflightBytes = channelCounters->GetCounter("InputBuffer/InflightBytes", false);
                OutputBufferInflightBytes = channelCounters->GetCounter("OutputBuffer/InflightBytes", false);
                OutputBufferWaiterBytes = channelCounters->GetCounter("OutputBuffer/WaiterBytes", false);
                LocalBufferInflightBytes = channelCounters->GetCounter("LocalBuffer/InflightBytes", false);

                SendProfileStats();
                Schedule(StatsReportPeriod, new TEvents::TEvWakeup());
            }
        }

        Send(executerId, reply.Release(), IEventHandle::FlagTrackDelivery, txId);
    }

    void SendProfileStats() {
        auto ev = MakeHolder<NYql::NDq::TEvDqCompute::TEvNodeState>();

        ev->Record.SetNodeId(SelfId().NodeId());

        if (auto p = tcmalloc::MallocExtension::GetNumericProperty("generic.physical_memory_used"); p) {
            ev->Record.SetMemPhysicalUsage(*p);
        }
        if (auto p = tcmalloc::MallocExtension::GetNumericProperty("generic.current_allocated_bytes"); p) {
            ev->Record.SetMemSysAllocated(*p);
        }
        if (auto p = tcmalloc::MallocExtension::GetNumericProperty("generic.realized_fragmentation"); p) {
            ev->Record.SetMemSysFragmented(*p);
        }

        ev->Record.SetMemArrowDefault(arrow::default_memory_pool()->bytes_allocated());
        ev->Record.SetMemMkqlAllocated(GetTotalMmapedBytes<>());
        ev->Record.SetMemMkqlFreeList(GetTotalFreeListBytes<>());

        if (InputBufferInflightBytes) {
            ev->Record.SetInputInflightBytes(InputBufferInflightBytes->Val());
        }
        if (OutputBufferInflightBytes && OutputBufferWaiterBytes) {
            ev->Record.SetOutputInflightBytes(OutputBufferInflightBytes->Val() + OutputBufferWaiterBytes->Val());
        }
        if (LocalBufferInflightBytes) {
            ev->Record.SetLocalInflightBytes(LocalBufferInflightBytes->Val());
        }

        Send(ExecuterId, ev.Release());
    }

    void ReplyError(const NKikimrKqp::TEvStartKqpTasksRequest& request,
        NKikimrKqp::TEvStartKqpTasksResponse::ENotStartedTaskReason reason, ui64 requestId, const TString& message = "")
    {
        auto ev = MakeHolder<TEvKqpNode::TEvStartKqpTasksResponse>();
        ev->Record.SetTxId(request.GetTxId());
        for (auto& task : request.GetTasks()) {
            auto* resp = ev->Record.AddNotStartedTasks();
            resp->SetTaskId(task.GetId());
            resp->SetReason(reason);
            resp->SetMessage(message);
            resp->SetRequestId(requestId);
        }
        Send(ExecuterId, ev.Release());
    }
private:
    TIntrusivePtr<TKqpCounters> Counters_;
    std::shared_ptr<TNodeState> State_;
    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager_;
    std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> CaFactory_;
    TActorId ExecuterId;
    NYql::NDqProto::EDqStatsMode StatsMode = NYql::NDqProto::DQ_STATS_MODE_NONE;
    TDuration StatsReportPeriod;
    ::NMonitoring::TDynamicCounters::TCounterPtr InputBufferInflightBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBufferInflightBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBufferWaiterBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr LocalBufferInflightBytes;
};

NActors::IActor* CreateKqpQueryManager(TIntrusivePtr<TKqpCounters>& counters, std::shared_ptr<TNodeState>& state,
    std::shared_ptr<NRm::IKqpResourceManager>& resourceManager, std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory>& caFactory) {
    return new TKqpQueryManager(counters, state, resourceManager, caFactory);
}

} // namespace NKikimr::NKqp
