#include "kqp_executer_stats.h"
#include "kqp_planner.h"
#include "kqp_planner_strategy.h"
#include "kqp_shards_resolver.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/base/appdata.h>

#include <util/generic/set.h>

#include <ydb/core/kqp/compute_actor/kqp_pure_compute_actor.h>

using namespace NActors;

namespace NKikimr::NKqp {

#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << "Ctx: " << *UserRequestContext << ". " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << "Ctx: " << *UserRequestContext << ". " << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << "Ctx: " << *UserRequestContext << ". " << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << "Ctx: " << *UserRequestContext << ". " << stream)

using namespace NYql;

namespace {

const ui64 MaxTaskSize = 48_MB;

template <class TCollection>
std::unique_ptr<TEvKqp::TEvAbortExecution> CheckTaskSize(ui64 TxId, const TIntrusivePtr<TUserRequestContext>& UserRequestContext, const TCollection& tasks) {
    for (const auto& task : tasks) {
        if (ui32 size = task.ByteSize(); size > MaxTaskSize) {
            LOG_E("Abort execution. Task #" << task.GetId() << " size is too big: " << size << " > " << MaxTaskSize);
            return std::make_unique<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::ABORTED,
                TStringBuilder() << "Datashard program size limit exceeded (" << size << " > " << MaxTaskSize << ")");
        }
    }
    return nullptr;
}

std::unique_ptr<IEventHandle> MakeActorStartFailureError(const TActorId& executerId, const TString& reason) {
    auto ev = std::make_unique<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::OVERLOADED, reason);
    return std::make_unique<IEventHandle>(executerId, executerId, ev.release());
}

void BuildInitialTaskResources(const TKqpTasksGraph& graph, ui64 taskId, TTaskResourceEstimation& ret) {
    const auto& task = graph.GetTask(taskId);
    const auto& stageInfo = graph.GetStageInfo(task.StageId);
    const NKqpProto::TKqpPhyStage& stage = stageInfo.Meta.GetStage(stageInfo.Id);
    const auto& opts = stage.GetProgram().GetSettings();
    ret.TaskId = task.Id;
    ret.ChannelBuffersCount += task.Inputs.size() ? 1 : 0;
    ret.ChannelBuffersCount += task.Outputs.size() ? 1 : 0;
    ret.HeavyProgram = opts.GetHasMapJoin();
}

bool LimitCPU(TIntrusivePtr<TUserRequestContext> ctx) {
    return ctx->PoolId && ctx->PoolConfig.has_value() && ctx->PoolConfig->TotalCpuLimitPercentPerNode > 0;
}

}

bool TKqpPlanner::UseMockEmptyPlanner = false;

// Task can allocate extra memory during execution.
// So, we estimate total memory amount required for task as apriori task size multiplied by this constant.
constexpr ui32 MEMORY_ESTIMATION_OVERFLOW = 2;

TKqpPlanner::TKqpPlanner(TKqpPlanner::TArgs&& args)
    : TxId(args.TxId)
    , LockTxId(args.LockTxId)
    , LockNodeId(args.LockNodeId)
    , ExecuterId(args.Executer)
    , Snapshot(args.Snapshot)
    , Database(args.Database)
    , UserToken(args.UserToken)
    , Deadline(args.Deadline)
    , StatsMode(args.StatsMode)
    , WithSpilling(args.WithSpilling)
    , RlPath(args.RlPath)
    , ResourcesSnapshot(std::move(args.ResourcesSnapshot))
    , ExecuterSpan(args.ExecuterSpan)
    , ExecuterRetriesConfig(args.ExecuterRetriesConfig)
    , TasksGraph(args.TasksGraph)
    , UseDataQueryPool(args.UseDataQueryPool)
    , LocalComputeTasks(args.LocalComputeTasks)
    , MkqlMemoryLimit(args.MkqlMemoryLimit)
    , AsyncIoFactory(args.AsyncIoFactory)
    , AllowSinglePartitionOpt(args.AllowSinglePartitionOpt)
    , UserRequestContext(args.UserRequestContext)
    , FederatedQuerySetup(args.FederatedQuerySetup)
    , OutputChunkMaxSize(args.OutputChunkMaxSize)
    , GUCSettings(std::move(args.GUCSettings))
    , MayRunTasksLocally(args.MayRunTasksLocally)
    , ResourceManager_(args.ResourceManager_)
    , CaFactory_(args.CaFactory_)
{
    if (GUCSettings) {
        SerializedGUCSettings = GUCSettings->SerializeToString();
    }

    if (!Database) {
        // a piece of magic for tests
        if (const auto& domain = AppData()->DomainsInfo->Domain) {
            Database = TStringBuilder() << '/' << domain->Name;
            LOG_E("Database not set, use " << Database);
        }
    }

    if (LimitCPU(UserRequestContext)) {
        AllowSinglePartitionOpt = false;
    }
}

// ResourcesSnapshot, ResourceEstimations

void TKqpPlanner::LogMemoryStatistics(const TLogFunc& logFunc) {
    uint64_t totalMemory = 0;
    uint32_t totalComputeActors = 0;
    for (auto& node : ResourcesSnapshot) {
        logFunc(TStringBuilder() << "[AvailableResources] node #" << node.GetNodeId()
            << " memory: " << (node.GetTotalMemory() - node.GetUsedMemory())
            << ", ca: " << node.GetAvailableComputeActors());
        totalMemory += (node.GetTotalMemory() - node.GetUsedMemory());
        totalComputeActors += node.GetAvailableComputeActors();
    }
    logFunc(TStringBuilder() << "Total nodes: " << ResourcesSnapshot.size() << ", total memory: " << totalMemory << ", total CA:" << totalComputeActors);

    totalMemory = 0;
    for (const auto& task : ResourceEstimations) {
        logFunc(TStringBuilder() << "[TaskResources] task: " << task.TaskId << ", memory: " << task.TotalMemoryLimit);
        totalMemory += task.TotalMemoryLimit;
    }
    logFunc(TStringBuilder() << "Total tasks: " << ResourceEstimations.size() << ", total memory: " << totalMemory);
}

bool TKqpPlanner::SendStartKqpTasksRequest(ui32 requestId, const TActorId& target) {
    YQL_ENSURE(requestId < Requests.size());

    auto& requestData = Requests[requestId];

    if (requestData.RetryNumber == ExecuterRetriesConfig.GetMaxRetryNumber() + 1) {
        return false;
    }

    std::unique_ptr<TEvKqpNode::TEvStartKqpTasksRequest> ev;
    if (Y_LIKELY(requestData.SerializedRequest)) {
        ev.reset(requestData.SerializedRequest.release());
    } else {
        ev = SerializeRequest(requestData);
    }

    if (requestData.RetryNumber == ExecuterRetriesConfig.GetMaxRetryNumber()) {
        LOG_E("Retry failed by retries limit, requestId: " << requestId);
        TMaybe<ui32> targetNode;
        for (size_t i = 0; i < ResourcesSnapshot.size(); ++i) {
            if (!TrackingNodes.contains(ResourcesSnapshot[i].nodeid())) {
                targetNode = ResourcesSnapshot[i].nodeid();
                break;
            }
        }
        if (targetNode) {
            LOG_D("Try to retry to another node, nodeId: " << *targetNode << ", requestId: " << requestId);
            auto anotherTarget = MakeKqpNodeServiceID(*targetNode);
            TlsActivationContext->Send(std::make_unique<NActors::IEventHandle>(anotherTarget, ExecuterId, ev.release(),
                CalcSendMessageFlagsForNode(*targetNode), requestId,  nullptr, ExecuterSpan.GetTraceId()));
            requestData.RetryNumber++;
            return true;
        }
        LOG_E("Retry failed because all nodes are busy, requestId: " << requestId);
        return false;
    }

    if (requestData.RetryNumber >= 1) {
        LOG_D("Try to retry by ActorUnknown reason, nodeId: " << target.NodeId() << ", requestId: " << requestId);
    }

    requestData.RetryNumber++;

    TlsActivationContext->Send(std::make_unique<NActors::IEventHandle>(target, ExecuterId, ev.release(),
        requestData.Flag, requestId,  nullptr, ExecuterSpan.GetTraceId()));
    return true;
}

std::unique_ptr<TEvKqpNode::TEvStartKqpTasksRequest> TKqpPlanner::SerializeRequest(const TRequestData& requestData) {
    auto result = std::make_unique<TEvKqpNode::TEvStartKqpTasksRequest>(TasksGraph.GetMeta().GetArenaIntrusivePtr());
    auto& request = result->Record;
    request.SetTxId(TxId);
    request.SetLockTxId(LockTxId);
    request.SetLockNodeId(LockNodeId);
    ActorIdToProto(ExecuterId, request.MutableExecuterActorId());

    if (Deadline) {
        TDuration timeout = Deadline - TAppData::TimeProvider->Now();
        request.MutableRuntimeSettings()->SetTimeoutMs(timeout.MilliSeconds());
    }

    for (ui64 taskId : requestData.TaskIds) {
        const auto& task = TasksGraph.GetTask(taskId);
        NYql::NDqProto::TDqTask* serializedTask = ArenaSerializeTaskToProto(TasksGraph, task, /* serializeAsyncIoSettings = */ true);
        request.AddTasks()->Swap(serializedTask);
    }

    request.MutableRuntimeSettings()->SetStatsMode(GetDqStatsMode(StatsMode));
    request.SetStartAllOrFail(true);
    if (UseDataQueryPool) {
        request.MutableRuntimeSettings()->SetExecType(NYql::NDqProto::TComputeRuntimeSettings::DATA);
        request.MutableRuntimeSettings()->SetUseSpilling(WithSpilling);
    } else {
        request.MutableRuntimeSettings()->SetExecType(NYql::NDqProto::TComputeRuntimeSettings::SCAN);
        request.MutableRuntimeSettings()->SetUseSpilling(WithSpilling);
    }

    if (RlPath) {
        auto rlPath = request.MutableRuntimeSettings()->MutableRlPath();
        rlPath->SetCoordinationNode(RlPath->GetCoordinationNode());
        rlPath->SetResourcePath(RlPath->GetResourcePath());
        rlPath->SetDatabase(Database);
        if (UserToken)
            rlPath->SetToken(UserToken->GetSerializedToken());
    }

    if (Snapshot.IsValid()) {
        request.MutableSnapshot()->SetTxId(Snapshot.TxId);
        request.MutableSnapshot()->SetStep(Snapshot.Step);
    }

    if (OutputChunkMaxSize) {
        request.SetOutputChunkMaxSize(OutputChunkMaxSize);
    }

    if (SerializedGUCSettings) {
        request.SetSerializedGUCSettings(SerializedGUCSettings);
    }

    request.SetSchedulerGroup(UserRequestContext->PoolId);
    request.SetDatabase(Database);
    if (UserRequestContext->PoolConfig.has_value()) {
        request.SetMemoryPoolPercent(UserRequestContext->PoolConfig->QueryMemoryLimitPercentPerNode);
        request.SetMaxCpuShare(UserRequestContext->PoolConfig->TotalCpuLimitPercentPerNode / 100.0);
    }

    return result;
}

void TKqpPlanner::Submit() {
    for (size_t reqId = 0; reqId < Requests.size(); ++reqId) {
        ui64 nodeId = Requests[reqId].NodeId;
        auto target = MakeKqpNodeServiceID(nodeId);
        SendStartKqpTasksRequest(reqId, target);
    }

    if (ExecuterSpan) {
        ExecuterSpan.Attribute("requestsCnt", static_cast<long>(Requests.size()));
    }
}

ui32 TKqpPlanner::GetCurrentRetryDelay(ui32 requestId) {
    auto& requestData = Requests[requestId];
    if (requestData.CurrentDelay == 0) {
        requestData.CurrentDelay = ExecuterRetriesConfig.GetMinDelayToRetryMs();
        return requestData.CurrentDelay;
    }
    requestData.CurrentDelay *= 2;
    requestData.CurrentDelay = Min(requestData.CurrentDelay, ExecuterRetriesConfig.GetMaxDelayToRetryMs());
    requestData.CurrentDelay = requestData.CurrentDelay * AppData()->RandomProvider->Uniform(100, 120) / 100;
    return requestData.CurrentDelay;
}

std::unique_ptr<IEventHandle> TKqpPlanner::AssignTasksToNodes() {
    if (ComputeTasks.empty())
        return nullptr;

    PrepareToProcess();

    auto localResources = ResourceManager_->GetLocalResources();
    Y_UNUSED(MEMORY_ESTIMATION_OVERFLOW);

    auto placingOptions = ResourceManager_->GetPlacingOptions();

    bool singleNodeExecutionMakeSence = (
        ResourceEstimations.size() <= placingOptions.MaxNonParallelTasksExecutionLimit ||
        // all readers are located on the one node.
        TasksPerNode.size() == 1
    );

    if (LocalRunMemoryEst * MEMORY_ESTIMATION_OVERFLOW <= localResources.Memory[NRm::EKqpMemoryPool::ScanQuery] &&
        ResourceEstimations.size() <= localResources.ExecutionUnits &&
        singleNodeExecutionMakeSence)
    {
        ui64 selfNodeId = ExecuterId.NodeId();
        for(ui64 taskId: ComputeTasks) {
            TasksPerNode[selfNodeId].push_back(taskId);
        }

        return nullptr;
    }

    if (ResourcesSnapshot.empty()) {
        ResourcesSnapshot = std::move(ResourceManager_->GetClusterResources());
    }

    if (ResourcesSnapshot.empty() || (ResourcesSnapshot.size() == 1 && ResourcesSnapshot[0].GetNodeId() == ExecuterId.NodeId())) {
        // try to run without memory overflow settings
        if (LocalRunMemoryEst <= localResources.Memory[NRm::EKqpMemoryPool::ScanQuery] &&
            ResourceEstimations.size() <= localResources.ExecutionUnits)
        {
            ui64 localNodeId = ExecuterId.NodeId();
            for(ui64 taskId: ComputeTasks) {
                TasksPerNode[localNodeId].push_back(taskId);
            }

            return nullptr;
        }

        LOG_E("Not enough resources to execute query locally and no information about other nodes");
        auto ev = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
            "Not enough resources to execute query locally and no information about other nodes (estimation: "
            + ToString(LocalRunMemoryEst) + ";" + GetEstimationsInfo() + ")");

        return std::make_unique<IEventHandle>(ExecuterId, ExecuterId, ev.Release());
    }

    std::vector<ui64> deepestTasks;
    ui64 maxLevel = 0;
    for(auto& task: TasksGraph.GetTasks()) {
        // const auto& task = TasksGraph.GetTask(taskId);
        const auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);
        const NKqpProto::TKqpPhyStage& stage = stageInfo.Meta.GetStage(stageInfo.Id);
        const ui64 stageLevel = stage.GetProgram().GetSettings().GetStageLevel();

        if (stageLevel > maxLevel) {
            maxLevel = stageLevel;
            deepestTasks.clear();
        }

        if (stageLevel == maxLevel) {
            deepestTasks.push_back(task.Id);
        }
    }

    THashMap<ui64, ui64> alreadyAssigned;
    for(auto& [nodeId, tasks] : TasksPerNode) {
        for(ui64 taskId: tasks) {
            alreadyAssigned.emplace(taskId, nodeId);
        }
    }

    if (deepestTasks.size() <= placingOptions.MaxNonParallelTopStageExecutionLimit) {
        // looks like the merge / union all connection
        for(ui64 taskId: deepestTasks) {
            auto [it, success] = alreadyAssigned.emplace(taskId, ExecuterId.NodeId());
            if (success) {
                TasksPerNode[ExecuterId.NodeId()].push_back(taskId);
            }
        }
    }

    auto planner = (UseMockEmptyPlanner ? CreateKqpMockEmptyPlanner() : CreateKqpGreedyPlanner());  // KqpMockEmptyPlanner is a mock planner for tests

    auto ctx = TlsActivationContext->AsActorContext();
    if (ctx.LoggerSettings() && ctx.LoggerSettings()->Satisfies(NActors::NLog::PRI_DEBUG, NKikimrServices::KQP_EXECUTER)) {
        planner->SetLogFunc([TxId = TxId, &UserRequestContext = UserRequestContext](TStringBuf msg) { LOG_D(msg); });
    }

    LogMemoryStatistics([TxId = TxId, &UserRequestContext = UserRequestContext](TStringBuf msg) { LOG_D(msg); });

    ui64 selfNodeId = ExecuterId.NodeId();
    TString selfNodeDC;

    TVector<const NKikimrKqp::TKqpNodeResources*> allNodes;
    TVector<const NKikimrKqp::TKqpNodeResources*> executerDcNodes;
    allNodes.reserve(ResourcesSnapshot.size());

    for(auto& snapNode: ResourcesSnapshot) {
        const TString& dc = snapNode.GetKqpProxyNodeResources().GetDataCenterId();
        if (snapNode.GetNodeId() == selfNodeId) {
            selfNodeDC = dc;
            break;
        }
    }

    for(auto& snapNode: ResourcesSnapshot) {
        allNodes.push_back(&snapNode);
        if (selfNodeDC == snapNode.GetKqpProxyNodeResources().GetDataCenterId()) {
            executerDcNodes.push_back(&snapNode);
        }
    }

    TVector<IKqpPlannerStrategy::TResult> plan;

    if (!executerDcNodes.empty()) {
        plan = planner->Plan(executerDcNodes, ResourceEstimations);
    }

    if (plan.empty()) {
        plan = planner->Plan(allNodes, ResourceEstimations);
    }

    if (plan.empty()) {
        LogMemoryStatistics([TxId = TxId, &UserRequestContext = UserRequestContext](TStringBuf msg) { LOG_E(msg); });

        auto ev = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
            TStringBuilder() << "Not enough resources to execute query. " << "TraceId: " << UserRequestContext->TraceId);
        return std::make_unique<IEventHandle>(ExecuterId, ExecuterId, ev.Release());
    }

    for (auto& group : plan) {
        for(ui64 taskId: group.TaskIds) {
            auto [it, success] = alreadyAssigned.emplace(taskId, group.NodeId);
            if (success) {
                TasksPerNode[group.NodeId].push_back(taskId);
            }
        }
    }

    return nullptr;
}

const IKqpGateway::TKqpSnapshot& TKqpPlanner::GetSnapshot() const {
    return TasksGraph.GetMeta().Snapshot;
}

// optimizeProtoForLocalExecution - if we want to execute compute actor locally and don't want to serialize & then deserialize proto message
// instead we just give ptr to proto message and after that we swap/copy it
TString TKqpPlanner::ExecuteDataComputeTask(ui64 taskId, ui32 computeTasksSize) {
    auto& task = TasksGraph.GetTask(taskId);
    NYql::NDqProto::TDqTask* taskDesc = ArenaSerializeTaskToProto(TasksGraph, task, true);
    NYql::NDq::TComputeRuntimeSettings settings;
    if (!TxInfo) {
        double memoryPoolPercent = 100;
        if (UserRequestContext->PoolConfig.has_value()) {
            memoryPoolPercent = UserRequestContext->PoolConfig->QueryMemoryLimitPercentPerNode;
        }

        TxInfo = MakeIntrusive<NRm::TTxState>(
            TxId, TInstant::Now(), ResourceManager_->GetCounters(),
            UserRequestContext->PoolId, memoryPoolPercent, Database);
    }

    auto startResult = CaFactory_->CreateKqpComputeActor({
        .ExecuterId = ExecuterId,
        .TxId = TxId,
        .LockTxId = LockTxId,
        .LockNodeId = LockNodeId,
        .Task = taskDesc,
        .TxInfo = TxInfo,
        .RuntimeSettings = settings,
        .TraceId = NWilson::TTraceId(ExecuterSpan.GetTraceId()),
        .Arena = TasksGraph.GetMeta().GetArenaIntrusivePtr(),
        .SerializedGUCSettings = SerializedGUCSettings,
        .NumberOfTasks = computeTasksSize,
        .OutputChunkMaxSize = OutputChunkMaxSize,
        .MemoryPool = NRm::EKqpMemoryPool::DataQuery,
        .WithSpilling = WithSpilling,
        .StatsMode = GetDqStatsMode(StatsMode),
        .Deadline = Deadline,
        .ShareMailbox = (computeTasksSize <= 1),
        .RlPath = Nothing(),
    });

    if (const auto* rmResult = std::get_if<NRm::TKqpRMAllocateResult>(&startResult)) {
        return rmResult->GetFailReason();
    }

    TActorId* actorId = std::get_if<TActorId>(&startResult);
    Y_ABORT_UNLESS(actorId);
    AcknowledgeCA(taskId, *actorId, nullptr);
    return TString();
}

ui32 TKqpPlanner::GetnScanTasks() {
    return nScanTasks;
}

ui32 TKqpPlanner::GetnComputeTasks() {
    return nComputeTasks;
}

std::unique_ptr<IEventHandle> TKqpPlanner::PlanExecution() {
    nScanTasks = 0;

    for (auto& task : TasksGraph.GetTasks()) {
        switch (task.Meta.Type) {
            case TTaskMeta::TTaskType::Compute:
                ComputeTasks.emplace_back(task.Id);
                break;
            case TTaskMeta::TTaskType::Scan:
                TasksPerNode[task.Meta.NodeId].emplace_back(task.Id);
                nScanTasks++;
                break;
        }
    }

    LOG_D("Total tasks: " << nScanTasks + nComputeTasks << ", readonly: true"  // TODO ???
        << ", " << nScanTasks << " scan tasks on " << TasksPerNode.size() << " nodes"
        << ", pool: " << (UseDataQueryPool ? "Data" : "Scan")
        << ", localComputeTasks: " << LocalComputeTasks
        << ", snapshot: {" << GetSnapshot().TxId << ", " << GetSnapshot().Step << "}");

    nComputeTasks = ComputeTasks.size();

    // explicit requirement to execute task on the same node because it has dependencies
    // on datashard tx.
    if (LocalComputeTasks) {
        for (ui64 taskId : ComputeTasks) {
            auto result = ExecuteDataComputeTask(taskId, ComputeTasks.size());
            if (!result.empty()) {
                return MakeActorStartFailureError(ExecuterId, result);
            }
        }
        ComputeTasks.clear();
    }

    if (nComputeTasks == 0 && TasksPerNode.size() == 1 && (AsyncIoFactory != nullptr) && AllowSinglePartitionOpt) {
        // query affects a single key or shard, so it might be more effective
        // to execute this task locally so we can avoid useless overhead for remote task launching.
        for (auto& [shardId, tasks]: TasksPerNode) {
            for (ui64 taskId: tasks) {
                auto result = ExecuteDataComputeTask(taskId, tasks.size());
                if (!result.empty()) {
                    return MakeActorStartFailureError(ExecuterId, result);
                }
            }
        }

    } else {
        for (ui64 taskId : ComputeTasks) {
            PendingComputeTasks.insert(taskId);
        }

        for (auto& [nodeId, tasks] : TasksPerNode) {
            for (ui64 taskId : tasks) {
                PendingComputeTasks.insert(taskId);
            }
        }

        auto err = AssignTasksToNodes();
        if (err) {
            return err;
        }

        if (MayRunTasksLocally) {
            // temporary flag until common ca factory is implemented.
            auto tasksOnNodeIt = TasksPerNode.find(ExecuterId.NodeId());
            if (tasksOnNodeIt != TasksPerNode.end()) {
                auto& tasks = tasksOnNodeIt->second;
                for (ui64 taskId: tasks) {
                    auto result = ExecuteDataComputeTask(taskId, tasks.size());
                    if (!result.empty()) {
                        return MakeActorStartFailureError(ExecuterId, result);
                    }
                }
            }
        }

        for(auto& [nodeId, tasks] : TasksPerNode) {
            if (MayRunTasksLocally && ExecuterId.NodeId() == nodeId)
                continue;

            SortUnique(tasks);
            auto& request = Requests.emplace_back(std::move(tasks), CalcSendMessageFlagsForNode(nodeId), nodeId);
            request.SerializedRequest = SerializeRequest(request);
            auto ev = CheckTaskSize(TxId, UserRequestContext, request.SerializedRequest->Record.GetTasks());
            if (ev != nullptr) {
                return std::make_unique<IEventHandle>(ExecuterId, ExecuterId, ev.release());
            }
        }

    }


    return nullptr;
}

TString TKqpPlanner::GetEstimationsInfo() const {
    TStringStream ss;
    ss << "ComputeTasks:" << nComputeTasks << ";NodeTasks:";
    if (auto it = TasksPerNode.find(ExecuterId.NodeId()); it != TasksPerNode.end()) {
        ss << it->second.size() << ";";
    } else {
        ss << "0;";
    }
    return ss.Str();
}

void TKqpPlanner::Unsubscribe() {
    for (ui64 nodeId: TrackingNodes) {
        TlsActivationContext->Send(std::make_unique<NActors::IEventHandle>(
            TActivationContext::InterconnectProxy(nodeId), ExecuterId, new TEvents::TEvUnsubscribe()));
    }
}

bool TKqpPlanner::AcknowledgeCA(ui64 taskId, TActorId computeActor, const NYql::NDqProto::TEvComputeActorState* state) {
    auto& task = TasksGraph.GetTask(taskId);
    if (!task.ComputeActorId) {
        task.ComputeActorId = computeActor;
        PendingComputeTasks.erase(taskId);
        auto [it, success] = PendingComputeActors.try_emplace(computeActor);
        YQL_ENSURE(success);
        if (state && state->HasStats()) {
            it->second.Set(state->GetStats());
        }

        return true;
    }

    YQL_ENSURE(task.ComputeActorId == computeActor);
    auto it = PendingComputeActors.find(computeActor);
    if (!task.Meta.Completed) {
        YQL_ENSURE(it != PendingComputeActors.end());
    }

    if (it != PendingComputeActors.end() && state && state->HasStats()) {
        it->second.Set(state->GetStats());
    }

    return false;
}

void TKqpPlanner::CompletedCA(ui64 taskId, TActorId computeActor) {
    auto& task = TasksGraph.GetTask(taskId);
    if (task.Meta.Completed) {
        YQL_ENSURE(!PendingComputeActors.contains(computeActor));
        return;
    }

    task.Meta.Completed = true;
    auto it = PendingComputeActors.find(computeActor);
    YQL_ENSURE(it != PendingComputeActors.end());
    LastStats.emplace_back(std::move(it->second));
    PendingComputeActors.erase(it);

    LOG_I("Compute actor has finished execution: " << computeActor.ToString());
}

void TKqpPlanner::TaskNotStarted(ui64 taskId) {
    // NOTE: should be invoked only while shutting down - when node is disconnected.

    auto& task = TasksGraph.GetTask(taskId);

    YQL_ENSURE(!task.ComputeActorId);
    YQL_ENSURE(!task.Meta.Completed);

    PendingComputeTasks.erase(taskId);
}

TProgressStat::TEntry TKqpPlanner::CalculateConsumptionUpdate() {
    TProgressStat::TEntry consumption;

    for (const auto& p : PendingComputeActors) {
        const auto& t = p.second.GetLastUsage();
        consumption += t;
    }

    for (const auto& p : LastStats) {
        const auto& t = p.GetLastUsage();
        consumption += t;
    }

    return consumption;
}

void TKqpPlanner::ShiftConsumption() {
    for (auto& p : PendingComputeActors) {
        p.second.Update();
    }

    for (auto& p : LastStats) {
        p.Update();
    }
}

const THashMap<TActorId, TProgressStat>& TKqpPlanner::GetPendingComputeActors() {
    return PendingComputeActors;
}

const THashSet<ui64>& TKqpPlanner::GetPendingComputeTasks() {
    return PendingComputeTasks;
}

void TKqpPlanner::PrepareToProcess() {
    ui32 tasksCount = ComputeTasks.size();
    for (auto& [shardId, tasks] : TasksPerNode) {
        tasksCount += tasks.size();
    }

    ResourceEstimations.resize(tasksCount);
    LocalRunMemoryEst = 0;

    for (size_t i = 0; i < ComputeTasks.size(); ++i) {
        BuildInitialTaskResources(TasksGraph, ComputeTasks[i], ResourceEstimations[i]);
        ResourceManager_->EstimateTaskResources(ResourceEstimations[i], ComputeTasks.size());
        LocalRunMemoryEst += ResourceEstimations[i].TotalMemoryLimit;
    }

    ui32 currentEst = ComputeTasks.size();
    for(auto& [nodeId, tasks] : TasksPerNode) {
        for (ui64 taskId: tasks) {
            BuildInitialTaskResources(TasksGraph, taskId, ResourceEstimations[currentEst]);
            ResourceManager_->EstimateTaskResources(ResourceEstimations[currentEst], tasks.size());
            LocalRunMemoryEst += ResourceEstimations[currentEst].TotalMemoryLimit;
            ++currentEst;
        }
    }
    Sort(ResourceEstimations, [](const auto& l, const auto& r) { return l.TotalMemoryLimit > r.TotalMemoryLimit; });
}

ui32 TKqpPlanner::CalcSendMessageFlagsForNode(ui32 nodeId) {
    ui32 flags = IEventHandle::FlagTrackDelivery;
    if (TrackingNodes.insert(nodeId).second) {
        flags |= IEventHandle::FlagSubscribeOnSession;
    }
    return flags;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
std::unique_ptr<TKqpPlanner> CreateKqpPlanner(TKqpPlanner::TArgs args) {
    return std::make_unique<TKqpPlanner>(std::move(args));
}

} // namespace NKikimr::NKqp
