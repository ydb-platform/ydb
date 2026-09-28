#include "kqp_node_service.h"
#include "kqp_query_control_plane.h"

#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/wilson/wilson_span.h>

#include <ydb/library/wilson_ids/wilson.h>

#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>

#include <atomic>
#include <util/generic/bitops.h>

namespace NKikimr::NKqp {

namespace {

class TQueryQuotaManager final : public IQueryQuotaManager {
public:
    explicit TQueryQuotaManager(TIntrusivePtr<NRm::TTxState> tx)
        : Tx(std::move(tx))
    {
        Y_ABORT_UNLESS(Tx && Tx->ResourceManager);
    }

    // the task and channel quota managers keep it alive: their Memory is back by now. The external memory left is the
    // one of the channels and of the tasks whose compute actors did not terminate
    ~TQueryQuotaManager() override {
        const ui64 externalMemory = ExternalMemory.load();
        Y_DEBUG_ABORT_UNLESS(AllocatedMemory.load() == externalMemory, "TxId: %" PRIu64 ", %" PRIu64 " bytes of Memory not freed",
            Tx->TxId, AllocatedMemory.load() - externalMemory);
        const ui64 executionUnits = ExecutionUnits.load();
        if (executionUnits || externalMemory) {
            Tx->ResourceManager->FreeResources(*Tx, 0, NRm::TKqpResourcesRequest{
                .ExecutionUnits = executionUnits,
                .ExternalMemory = externalMemory,
            });
        }
    }

    NRm::TKqpRMAllocateResult AllocateTasks(ui64 executionUnits, ui64 externalMemory) override {
        auto result = Tx->ResourceManager->AllocateResources(*Tx, 0,
            NRm::TKqpResourcesRequest{.ExecutionUnits = executionUnits, .ExternalMemory = externalMemory});
        if (result) {
            ExecutionUnits.fetch_add(executionUnits);
            ExternalMemory.fetch_add(externalMemory);
            Allocated(externalMemory);
        }
        return result;
    }

    void FreeTasks(ui64 executionUnits, ui64 externalMemory) override {
        // uncounted before the release: never more than the tx holds
        const ui64 prevUnits = ExecutionUnits.fetch_sub(executionUnits);
        const ui64 prevExternal = ExternalMemory.fetch_sub(externalMemory);
        const ui64 prevAllocated = AllocatedMemory.fetch_sub(externalMemory);
        Y_DEBUG_ABORT_UNLESS(prevUnits >= executionUnits && prevExternal >= externalMemory && prevAllocated >= externalMemory,
            "TxId: %" PRIu64 ", freeing %" PRIu64 " execution units of %" PRIu64 ", %" PRIu64 " bytes of external memory of %" PRIu64,
            Tx->TxId, executionUnits, prevUnits, externalMemory, prevExternal);
        Tx->ResourceManager->FreeResources(*Tx, 0, NRm::TKqpResourcesRequest{
            .ExecutionUnits = executionUnits,
            .ExternalMemory = externalMemory,
        });
    }

    const TIntrusivePtr<NRm::TTxState>& GetTx() const override {
        return Tx;
    }

    bool AllocateQuota(ui64 memorySize, bool isOptional) override {
        auto result = Tx->ResourceManager->AllocateResources(*Tx, 0,
            NRm::TKqpResourcesRequest{.Memory = memorySize, .Optional = isOptional});

        if (!result) {
            // an optional refusal is the spilling signal, the caller logs it
            if (!isOptional) {
                YDB_LOG_WARN_COMP(NKikimrServices::KQP_COMPUTE, "",
                    {"problem", "cannot_allocate_memory"},
                    {"txId", Tx->TxId},
                    {"memory", memorySize});
            }

            return false;
        }

        Allocated(memorySize);
        return true;
    }

    void FreeQuota(ui64 memorySize) override {
        // uncounted before the release: never more than the tx holds
        const ui64 prev = AllocatedMemory.fetch_sub(memorySize);
        Y_DEBUG_ABORT_UNLESS(prev >= memorySize, "TxId: %" PRIu64 ", freeing %" PRIu64 " bytes of %" PRIu64 " allocated",
            Tx->TxId, memorySize, prev);
        Tx->ResourceManager->FreeResources(*Tx, 0, NRm::TKqpResourcesRequest{.Memory = memorySize});
    }

    ui64 GetCurrentQuota() const override {
        return AllocatedMemory.load();
    }

    ui64 GetMaxMemorySize() const override {
        return MaxAllocatedMemory.load();
    }

    i64 GetMemoryAvailability() const override {
        return Tx->GetMemoryAvailability();
    }

    TString MemoryConsumptionDetails() const override {
        return Tx->ToString();
    }

private:
    // counted after the grant: never more than the tx holds
    void Allocated(ui64 bytes) {
        const ui64 allocated = AllocatedMemory.fetch_add(bytes) + bytes;
        ui64 peak = MaxAllocatedMemory.load();
        while (peak < allocated && !MaxAllocatedMemory.compare_exchange_weak(peak, allocated)) {
        }
    }

    const TIntrusivePtr<NRm::TTxState> Tx;
    // not returned yet
    std::atomic<ui64> ExecutionUnits = 0;
    std::atomic<ui64> ExternalMemory = 0;
    // Memory + ExternalMemory
    std::atomic<ui64> AllocatedMemory = 0;
    std::atomic<ui64> MaxAllocatedMemory = 0;
};

} // namespace

TQueryQuotaManagerPtr CreateQueryQuotaManager(TIntrusivePtr<NRm::TTxState> tx) {
    return std::make_shared<TQueryQuotaManager>(std::move(tx));
}

// for CA/task, is NOT thread safe

struct TMemoryQuotaManager : public NYql::NDq::TGuaranteeQuotaManager {

    // the limit is external memory of the query quota manager, it returns it when it dies
    TMemoryQuotaManager(NYql::NDq::IMemoryQuotaManager::TPtr query
        , ui64 limit, ui64 step = 1_MB)
    : NYql::NDq::TGuaranteeQuotaManager(limit, limit, step)
    , Query(std::move(query))
    {}

    ~TMemoryQuotaManager() override {
        if (Limit > Guarantee) {
            Query->FreeQuota(Limit - Guarantee);
        }
    }

    bool AllocateExtraQuota(ui64 extraSize, bool isOptional) override {
        return Query->AllocateQuota(extraSize, isOptional);
    }

    void FreeExtraQuota(ui64 extraSize) override {
        Query->FreeQuota(extraSize);
    }

    i64 GetExtraMemoryAvailability() const override {
        return Query->GetMemoryAvailability();
    }

    TString MemoryConsumptionDetails() const override {
        return Query->MemoryConsumptionDetails();
    }

    const NYql::NDq::IMemoryQuotaManager::TPtr Query;
};

NYql::NDq::IMemoryQuotaManager::TPtr CreateTaskQuotaManager(NYql::NDq::IMemoryQuotaManager::TPtr queryQuotaManager,
    ui64 initialMemoryLimit, ui64 allocationStep) {
    return std::make_shared<TMemoryQuotaManager>(std::move(queryQuotaManager), initialMemoryLimit, allocationStep);
}

// for event/messages, IS THREAD SAFE, allows little overquoating

struct TChannelQuotaManager : public NYql::NDq::IMemoryQuotaManager {

    // the limit is external memory of the query quota manager, it returns it when it dies
    TChannelQuotaManager(NYql::NDq::IMemoryQuotaManager::TPtr query
        , ui64 limit, ui64 step = 1_MB)
    : Query(std::move(query))
    , AvailableQuota(limit)
    , Limit(limit)
    , DataMemoryLimit(limit)
    , AllocationStep(step)
    {
        Y_ABORT_UNLESS(IsPowerOf2(AllocationStep), "the allocation step must be a power of two"); // it is used as an alignment mask
    }

    ~TChannelQuotaManager() {
        if (const ui64 memory = Limit.load() - DataMemoryLimit) {
            Query->FreeQuota(memory);
        }
    }

    bool AllocateQuota(ui64 memorySize, bool isOptional) override {
        i64 quota = AvailableQuota.fetch_sub(memorySize);

        if (static_cast<i64>(memorySize) > quota) {
            ui64 memoryRequired = memorySize - quota;
            memoryRequired += AllocationStep - 1;
            memoryRequired &= ~(AllocationStep - 1);

            // the resource manager refuses an optional request at the spilling threshold
            if (Query->AllocateQuota(memoryRequired, isOptional)) {
                AvailableQuota.fetch_add(memoryRequired);
                Limit.fetch_add(memoryRequired);
            } else {
                // a little over-quoting is tolerated for mandatory requests only: the caller of an optional
                // request can do without the memory, it must not get what the resource manager refused
                if (isOptional || memoryRequired >= AllocationStep * 10) {
                    AvailableQuota.fetch_add(memorySize);
                    return false;
                }
            }
        }

        AllocatedQuota.fetch_add(memorySize);
        return true;
    }

    // Node level memory availability of the tx (see NRm::TTxState::GetMemoryAvailability) plus the locally
    // prepaid quota. Channels do not spill on a negative value, but propagate it as back pressure,
    // see TInputDescriptor::MemoryPressure.
    i64 GetMemoryAvailability() const override {
        return NYql::NDq::CombineMemoryAvailability(AvailableQuota.load(), Query->GetMemoryAvailability());
    }

    void FreeQuota(ui64 memorySize) override {
        auto prevQuota = AllocatedQuota.fetch_sub(memorySize);
        Y_DEBUG_ABORT_UNLESS(prevQuota >= memorySize);
        i64 quota = AvailableQuota.fetch_add(memorySize);
        if (quota > static_cast<i64>(AllocationStep * 10 + DataMemoryLimit)) {
            AvailableQuota.fetch_sub(AllocationStep);
            Limit.fetch_sub(AllocationStep);
            Query->FreeQuota(AllocationStep);
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

    const NYql::NDq::IMemoryQuotaManager::TPtr Query;
    std::atomic<ui64> AllocatedQuota = 0;
    std::atomic<i64> AvailableQuota;
    std::atomic<ui64> Limit;
    const ui64 DataMemoryLimit;
    const ui64 AllocationStep;
};

NYql::NDq::IMemoryQuotaManager::TPtr CreateChannelQuotaManager(NYql::NDq::IMemoryQuotaManager::TPtr queryQuotaManager,
    ui64 initialMemoryLimit, ui64 allocationStep) {
    return std::make_shared<TChannelQuotaManager>(std::move(queryQuotaManager), initialMemoryLimit, allocationStep);
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
    TKqpQueryManager(TIntrusivePtr<TKqpCounters>& counters, std::shared_ptr<TNodeState>& state,
        std::shared_ptr<NRm::IKqpResourceManager>& resourceManager, std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory>& caFactory,
        bool enableSmallComputeMemoryAllocations, bool enableChannelMemoryTracking)
        : TActor(&TThis::StateFunc)
        , Counters_(counters)
        , State_(state)
        , ResourceManager_(resourceManager)
        , CaFactory_(caFactory)
        , EnableSmallComputeMemoryAllocations(enableSmallComputeMemoryAllocations)
        , EnableChannelMemoryTracking(enableChannelMemoryTracking)
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
        NWilson::TSpan createTasksSpan(TWilsonKqp::KqpNodeCreateTasks, NWilson::TTraceId(ev->TraceId), "Start tasks", NWilson::EFlags::AUTO_END);
        createTasksSpan.Attribute("ydb.actor.type", TString("TKqpQueryManager"));
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

        YDB_LOG_DEBUG_COMP(NKikimrServices::KQP_NODE, "HandleStartKqpTasksRequest",
            {"marker", "KQPNS"},
            {"nodeId", SelfId().NodeId()},
            {"txId", txId},
            {"requester", executerId},
            {"tasksCount", msg.GetTasks().size()},
            {"taskIds", TasksIdsStr(msg.GetTasks())},
            {"traceId", ev->TraceId.GetHexTraceIdLowerCase()});

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

        YDB_LOG_DEBUG_COMP(NKikimrServices::KQP_NODE, ((tasks.size() == taskCount) ? "Created new request" : "Added tasks to existing request"),
            {"marker", "KQPNS"},
            {"nodeId", SelfId().NodeId()},
            {"txId", txId},
            {"tasksCount", tasks.size()},
            {"executer", executerId},
            {"traceId", ev->TraceId.GetHexTraceIdLowerCase()});

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

        auto lightLimit = CaFactory_->MkqlLightProgramMemoryLimit.load();
        auto heavyLimit = CaFactory_->MkqlHeavyProgramMemoryLimit.load();
        const ui32 tasksCount = msg.GetTasks().size();
        ui64 externalMemory = 0;
        if (EnableSmallComputeMemoryAllocations) {
            externalMemory = tasksCount * lightLimit;
        } else {
            for (const auto& dqTask: msg.GetTasks()) {
                auto& taskOpts = dqTask.GetProgram().GetSettings();
                externalMemory += taskOpts.GetHasMapJoin() || taskOpts.GetHasStateAggregation() ? heavyLimit : lightLimit;
            }
        }
        ui64 channelMemory = 0;

        if (!QueryQuotaManager) {
            // - for the very 1st start request we reserve the same amount of memory for channels as well
            // - for following start requests (unlikely) we allocate no extra memory for channels
            if (EnableChannelMemoryTracking) {
                channelMemory = tasksCount * lightLimit;
                externalMemory += channelMemory;
            }
            QueryQuotaManager = CreateQueryQuotaManager(MakeIntrusive<NRm::TTxState>(ResourceManager_, txId, TInstant::Now(),
                poolId, msg.GetMemoryPoolPercent(),
                msg.GetDatabase(),  CaFactory_->GetVerboseMemoryLimitException()));
        }

        // the tasks and the channels start with their part of it; a task returns its part when its compute actor
        // terminates, the rest is returned when the query quota manager dies
        auto rmResult = QueryQuotaManager->AllocateTasks(tasksCount, externalMemory);

        if (!rmResult) {
            ReplyError(msg, rmResult.GetStatus(), ev->Cookie, rmResult.GetFailReason());

            State_->MarkRequestAsCancelled(executerId);

            if (auto tasksToAbort = State_->GetTasksByExecuterId(executerId); !tasksToAbort.empty()) {
                YDB_LOG_ERROR_COMP(NKikimrServices::KQP_NODE, "Node service unable to allocate tasks",
                    {"marker", "KQPNS"},
                    {"tasksCount", tasksCount},
                    {"reason", rmResult.GetFailReason()},
                    {"nodeId", SelfId().NodeId()},
                    {"txId", txId});
                for (const auto& [taskId, computeActorId]: tasksToAbort) {
                    auto abortEv = std::make_unique<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::UNSPECIFIED, rmResult.GetFailReason());
                    Send(computeActorId, abortEv.release());
                }
            }

            // the tasks of this request never start: the request ends with the aborted ones, and this actor, the
            // owner of the query quota manager that returns what the earlier requests took, is poisoned
            for (const auto taskId : tasks) {
                State_->OnTaskFinished(txId, executerId, taskId, /* success */ false);
            }

            co_return;
        }

        if (EnableChannelMemoryTracking && !ChannelQuotaManager) {
            ChannelQuotaManager = CreateChannelQuotaManager(QueryQuotaManager, channelMemory);
        }

        auto reportStatsSettings = ReportStatsSettingsFromProto(runtimeSettings);

        for (auto& dqTask: *msg.MutableTasks()) {

            const auto taskId = dqTask.GetId();

            auto& taskOpts = dqTask.GetProgram().GetSettings();
            auto initialMemoryLimit = !EnableSmallComputeMemoryAllocations && (taskOpts.GetHasMapJoin() || taskOpts.GetHasStateAggregation()) ? heavyLimit : lightLimit;

            NComputeActor::IKqpNodeComputeActorFactory::TCreateArgs createArgs{
                .ExecuterId = executerId,
                .TxId = txId,
                .LockTxId = lockTxId,
                .LockNodeId = lockNodeId,
                .LockMode = lockMode,
                .Task = &dqTask,
                .TxInfo = QueryQuotaManager->GetTx(),
                .TaskQuotaManager = CreateTaskQuotaManager(QueryQuotaManager, initialMemoryLimit),
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
                .QueryQuotaManager = QueryQuotaManager,
                .InitialMemoryLimit = initialMemoryLimit,
                .Database = msg.GetDatabase(),
                .Query = query,
                .UseBatchPool = msg.GetUseBatchPool(),
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
                YDB_LOG_DEBUG_COMP(NKikimrServices::KQP_NODE, "Executing task",
                    {"marker", "KQPNS"},
                    {"nodeId", SelfId().NodeId()},
                    {"txId", txId},
                    {"taskId", taskId},
                    {"computeActorId", actorId},
                    {"traceId", ev->TraceId.GetHexTraceIdLowerCase()});
            } else {
                YDB_LOG_DEBUG_COMP(NKikimrServices::KQP_NODE, "Task finished in an instant",
                    {"marker", "KQPNS"},
                    {"nodeId", SelfId().NodeId()},
                    {"txId", txId},
                    {"taskId", taskId},
                    {"computeActorId", actorId},
                    {"traceId", ev->TraceId.GetHexTraceIdLowerCase()});
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
                    CaFactory_->GetShardsScanningPolicy(), Counters_, NWilson::TTraceId(m.TraceId), cpuLimits,
                    msg.GetUseBatchPool()));
            }
        }

        if (StatsMode == NYql::NDqProto::DQ_STATS_MODE_NONE) {
            StatsMode = runtimeSettings.GetStatsMode();
            if (EnableChannelMemoryTracking && StatsMode == NYql::NDqProto::DQ_STATS_MODE_PROFILE) {
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
        if (QueryQuotaManager) {
            ev->Record.SetMemQueryAllocated(QueryQuotaManager->GetCurrentQuota());
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
    TQueryQuotaManagerPtr QueryQuotaManager;
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
    const bool EnableSmallComputeMemoryAllocations;
    const bool EnableChannelMemoryTracking;
};

NActors::IActor* CreateKqpQueryManager(TIntrusivePtr<TKqpCounters>& counters, std::shared_ptr<TNodeState>& state,
    std::shared_ptr<NRm::IKqpResourceManager>& resourceManager, std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory>& caFactory,
    bool enableSmallComputeMemoryAllocations, bool enableChannelMemoryTracking) {
    return new TKqpQueryManager(counters, state, resourceManager, caFactory, enableSmallComputeMemoryAllocations, enableChannelMemoryTracking);
}

} // namespace NKikimr::NKqp
