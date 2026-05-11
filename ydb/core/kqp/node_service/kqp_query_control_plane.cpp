#include "kqp_node_service.h"
#include "kqp_query_control_plane.h"

#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/wilson/wilson_span.h>

#include <ydb/library/wilson_ids/wilson.h>

#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>

namespace NKikimr::NKqp {

// for CA/task, is NOT thread safe

struct TMemoryQuotaManager : public NYql::NDq::TGuaranteeQuotaManager {

    TMemoryQuotaManager(std::shared_ptr<NRm::IKqpResourceManager> resourceManager
        , TIntrusivePtr<NRm::TTxState> tx
        , ui64 taskId
        , ui64 limit)
    : NYql::NDq::TGuaranteeQuotaManager(limit, limit)
    , ResourceManager(std::move(resourceManager))
    , Tx(std::move(tx))
    , TaskId(taskId)
    {}

    ~TMemoryQuotaManager() override {
        ResourceManager->FreeResources(*Tx, TaskId, NRm::TKqpResourcesRequest{
            .ExecutionUnits = 1,
            .Memory = Limit - Guarantee,
            .ExternalMemory = Guarantee,
        });
    }

    bool AllocateExtraQuota(ui64 extraSize) override {
        auto result = ResourceManager->AllocateResources(*Tx, TaskId,
            NRm::TKqpResourcesRequest{.Memory = extraSize});

        if (!result) {
            AFL_WARN(NKikimrServices::KQP_COMPUTE)
                ("problem", "cannot_allocate_memory")
                ("tx_id", Tx->TxId)
                ("task_id", TaskId)
                ("memory", extraSize);

            return false;
        }

        return true;
    }

    void FreeExtraQuota(ui64 extraSize) override {
        ResourceManager->FreeResources(*Tx, TaskId, NRm::TKqpResourcesRequest{.Memory = extraSize});
    }

    bool IsReasonableToUseSpilling() const override {
        return Tx->IsReasonableToStartSpilling();
    }

    TString MemoryConsumptionDetails() const override {
        return Tx->ToString();
    }

    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager;
    TIntrusivePtr<NRm::TTxState> Tx;
    ui64 TaskId;
};

NYql::NDq::IMemoryQuotaManager::TPtr CreateTaskQuotaManager(std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
    TIntrusivePtr<NRm::TTxState> tx, ui64 taskId, ui64 initialMemoryLimit) {
    return std::make_shared<TMemoryQuotaManager>(resourceManager, tx, taskId, initialMemoryLimit);
}

// for event/messages, IS THREAD SAFE, allows little overquoating

struct TChannelQuotaManager : public NYql::NDq::IMemoryQuotaManager {

    TChannelQuotaManager(std::shared_ptr<NRm::IKqpResourceManager> resourceManager
        , TIntrusivePtr<NRm::TTxState> tx
        , ui64 limit, ui64 step = 1_MB)
    : ResourceManager(std::move(resourceManager))
    , Tx(std::move(tx))
    , AvailableQuota(limit)
    , Limit(limit)
    , DataMemoryLimit(limit)
    , AllocationStep(step)
    {}

    ~TChannelQuotaManager() {
        ResourceManager->FreeResources(*Tx, 0, NRm::TKqpResourcesRequest{
            .Memory = Limit.load() - DataMemoryLimit,
            .ExternalMemory = DataMemoryLimit,
        });
    }

    bool AllocateQuota(ui64 memorySize) override {
        i64 quota = AvailableQuota.fetch_sub(memorySize);

        if (static_cast<i64>(memorySize) > quota) {
            ui64 memoryRequired = memorySize - quota;
            memoryRequired += AllocationStep - 1;
            memoryRequired &= ~(AllocationStep - 1);

            auto result = ResourceManager->AllocateResources(*Tx, 0, NRm::TKqpResourcesRequest{.Memory = memoryRequired});
            if (result) {
                AvailableQuota.fetch_add(memoryRequired);
                Limit.fetch_add(memoryRequired);
            } else {
                AFL_WARN(NKikimrServices::KQP_COMPUTE)
                    ("problem", "cannot_allocate_memory")
                    ("tx_id", Tx->TxId)
                    ("task_id", 0)
                    ("memory", memoryRequired);
                if (memoryRequired >= AllocationStep * 10) {
                    AvailableQuota.fetch_add(memorySize);
                    return false;
                }
            }
        }

        AllocatedQuota.fetch_add(memorySize);
        return true;
    }

    bool IsReasonableToUseSpilling() const override {
        return false;
    }

    void FreeQuota(ui64 memorySize) override {
        auto prevQuota = AllocatedQuota.fetch_sub(memorySize);
        Y_DEBUG_ABORT_UNLESS(prevQuota >= memorySize);
        i64 quota = AvailableQuota.fetch_add(memorySize);
        if (quota > static_cast<i64>(AllocationStep * 10 + DataMemoryLimit)) {
            AvailableQuota.fetch_sub(AllocationStep);
            Limit.fetch_sub(AllocationStep);
            ResourceManager->FreeResources(*Tx, 0, NRm::TKqpResourcesRequest{.Memory = AllocationStep});
        }
    }

    ui64 GetCurrentQuota() const override {
        return AllocatedQuota.load();
    }

    ui64 GetMaxMemorySize() const override {
        return AllocatedQuota.load();
    };

    TString MemoryConsumptionDetails() const override {
        return TString();
    }

    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager;
    TIntrusivePtr<NRm::TTxState> Tx;
    std::atomic<ui64> AllocatedQuota = 0;
    std::atomic<i64> AvailableQuota;
    std::atomic<ui64> Limit;
    const ui64 DataMemoryLimit;
    const ui64 AllocationStep;
};

NYql::NDq::IMemoryQuotaManager::TPtr CreateChannelQuotaManager(std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
    TIntrusivePtr<NRm::TTxState> tx, ui64 initialMemoryLimit, ui64 allocationStep) {
    return std::make_shared<TChannelQuotaManager>(resourceManager, tx, initialMemoryLimit, allocationStep);
}

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
            auto addDatabaseEvent = MakeHolder<NScheduler::TEvAddDatabase>(databaseId);
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

        auto initialMemoryLimit = CaFactory_->MkqlLightProgramMemoryLimit.load();
        const ui32 tasksCount = msg.GetTasks().size();
        auto externalMemory = initialMemoryLimit * tasksCount;
        auto channelMemory = 0;

        if (!TxInfo) {
            // - for the very 1st start request we reserve the same amount of memory for channels as well
            // - for following start requests (unlikely) we allocate no extra mempry for channels
            channelMemory = externalMemory;
            externalMemory += channelMemory;
            TxInfo = MakeIntrusive<NRm::TTxState>(ResourceManager_, txId, TInstant::Now(),
                poolId, msg.GetMemoryPoolPercent(),
                msg.GetDatabase(),  CaFactory_->GetVerboseMemoryLimitException());
        }

        auto rmResult = ResourceManager_->AllocateResources(
            *TxInfo, 0, NRm::TKqpResourcesRequest{.ExecutionUnits = tasksCount, .ExternalMemory = externalMemory});

        if (!rmResult) {
            ReplyError(msg, rmResult.GetStatus(), ev->Cookie, rmResult.GetFailReason());

            State_->MarkRequestAsCancelled(executerId);

            if (auto tasksToAbort = State_->GetTasksByExecuterId(executerId); !tasksToAbort.empty()) {
                STLOG_E("Node service unable to allocate " << tasksCount << " tasks, reason: " << rmResult.GetFailReason(),
                    (node_id, SelfId().NodeId()),
                    (tx_id, txId));
                for (const auto& [taskId, computeActorId]: tasksToAbort) {
                    auto abortEv = std::make_unique<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::UNSPECIFIED, rmResult.GetFailReason());
                    Send(computeActorId, abortEv.release());
                }
            }

            co_return;
        }

        if (!ChannelQuotaManager) {
            ChannelQuotaManager = CreateChannelQuotaManager(ResourceManager_, TxInfo, channelMemory);
        }

        auto reportStatsSettings = ReportStatsSettingsFromProto(runtimeSettings);

        for (auto& dqTask: *msg.MutableTasks()) {

            const auto taskId = dqTask.GetId();

            NComputeActor::IKqpNodeComputeActorFactory::TCreateArgs createArgs{
                .ExecuterId = executerId,
                .TxId = txId,
                .LockTxId = lockTxId,
                .LockNodeId = lockNodeId,
                .LockMode = lockMode,
                .Task = &dqTask,
                .TxInfo = TxInfo,
                .TaskQuotaManager = CreateTaskQuotaManager(ResourceManager_, TxInfo, taskId, initialMemoryLimit),
                .ChannelQuotaManager = ChannelQuotaManager,
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

            auto actorId = CaFactory_->CreateKqpComputeActor(std::move(createArgs));
            auto* startedTask = reply->Record.AddStartedTasks();
            startedTask->SetTaskId(taskId);
            ActorIdToProto(actorId, startedTask->MutableActorId());
            if (State_->OnTaskStarted(executerId, taskId, actorId)) {
                STLOG_D("Executing task",
                    (node_id, SelfId().NodeId()),
                    (tx_id, txId),
                    (task_id, taskId),
                    (compute_actor_id, actorId),
                    (trace_id, ev->TraceId.GetHexTraceIdLowerCase()));
            } else {
                STLOG_D("Task finished in an instant",
                    (node_id, SelfId().NodeId()),
                    (tx_id, txId),
                    (task_id, taskId),
                    (compute_actor_id, actorId),
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
    TIntrusivePtr<NRm::TTxState> TxInfo;
    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager_;
    std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> CaFactory_;
    TActorId ExecuterId;
    NYql::NDqProto::EDqStatsMode StatsMode = NYql::NDqProto::DQ_STATS_MODE_NONE;
    TDuration StatsReportPeriod;
    ::NMonitoring::TDynamicCounters::TCounterPtr InputBufferInflightBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBufferInflightBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputBufferWaiterBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr LocalBufferInflightBytes;
    NYql::NDq::IMemoryQuotaManager::TPtr ChannelQuotaManager;
};

NActors::IActor* CreateKqpQueryManager(TIntrusivePtr<TKqpCounters>& counters, std::shared_ptr<TNodeState>& state,
    std::shared_ptr<NRm::IKqpResourceManager>& resourceManager, std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory>& caFactory) {
    return new TKqpQueryManager(counters, state, resourceManager, caFactory);
}

} // namespace NKikimr::NKqp
