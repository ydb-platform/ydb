#include "kqp_executer_stats.h"
#include "kqp_planner.h"
#include "kqp_planner_strategy.h"
#include "kqp_shards_resolver.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <util/generic/set.h>

using namespace NActors;

namespace NKikimr::NKqp {

#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)

using namespace NYql;

namespace {

const ui64 MaxTaskSize = 48_MB;

template <class TCollection>
std::unique_ptr<TEvKqp::TEvAbortExecution> CheckTaskSize(ui64 TxId, const TCollection& tasks) {
    for (const auto& task : tasks) {
        if (ui32 size = task.ByteSize(); size > MaxTaskSize) {
            LOG_E("Abort execution. Task #" << task.GetId() << " size is too big: " << size << " > " << MaxTaskSize);
            return std::make_unique<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::ABORTED,
                TStringBuilder() << "Datashard program size limit exceeded (" << size << " > " << MaxTaskSize << ")");
        }
    }
    return nullptr;
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

}

// Task can allocate extra memory during execution.
// So, we estimate total memory amount required for task as apriori task size multiplied by this constant.
constexpr ui32 MEMORY_ESTIMATION_OVERFLOW = 2;
constexpr ui32 MAX_NON_PARALLEL_TASKS_EXECUTION_LIMIT = 4;

TKqpPlanner::TKqpPlanner(TKqpTasksGraph& graph, ui64 txId, const TActorId& executer, const IKqpGateway::TKqpSnapshot& snapshot,
    const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TInstant deadline,
    const Ydb::Table::QueryStatsCollection::Mode& statsMode,
    bool withSpilling, const TMaybe<NKikimrKqp::TRlPath>& rlPath, NWilson::TSpan& executerSpan,
    TVector<NKikimrKqp::TKqpNodeResources>&& resourcesSnapshot,
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig,
    bool isDataQuery, ui64 mkqlMemoryLimit, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, bool doOptimization,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext)
    : TxId(txId)
    , ExecuterId(executer)
    , Snapshot(snapshot)
    , Database(database)
    , UserToken(userToken)
    , Deadline(deadline)
    , StatsMode(statsMode)
    , WithSpilling(withSpilling)
    , RlPath(rlPath)
    , ResourcesSnapshot(std::move(resourcesSnapshot))
    , ExecuterSpan(executerSpan)
    , ExecuterRetriesConfig(executerRetriesConfig)
    , TasksGraph(graph)
    , IsDataQuery(isDataQuery)
    , MkqlMemoryLimit(mkqlMemoryLimit)
    , AsyncIoFactory(asyncIoFactory)
    , DoOptimization(doOptimization)
    , UserRequestContext(userRequestContext)
{
    if (!Database) {
        // a piece of magic for tests
        for (auto& x : AppData()->DomainsInfo->DomainByName) {
            Database = TStringBuilder() << '/' << x.first;
            LOG_E("Database not set, use " << Database);
        }
    }
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
    ActorIdToProto(ExecuterId, request.MutableExecuterActorId());

    if (Deadline) {
        TDuration timeout = Deadline - TAppData::TimeProvider->Now();
        request.MutableRuntimeSettings()->SetTimeoutMs(timeout.MilliSeconds());
    }

    for (ui64 taskId : requestData.TaskIds) {
        const auto& task = TasksGraph.GetTask(taskId);
        NYql::NDqProto::TDqTask* serializedTask = ArenaSerializeTaskToProto(TasksGraph, task);
        request.AddTasks()->Swap(serializedTask);
    }

    request.MutableRuntimeSettings()->SetStatsMode(GetDqStatsMode(StatsMode));
    request.SetStartAllOrFail(true);
    if (IsDataQuery) {
        request.MutableRuntimeSettings()->SetExecType(NYql::NDqProto::TComputeRuntimeSettings::DATA);
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

    auto localResources = GetKqpResourceManager()->GetLocalResources();
    Y_UNUSED(MEMORY_ESTIMATION_OVERFLOW);
    if (LocalRunMemoryEst * MEMORY_ESTIMATION_OVERFLOW <= localResources.Memory[NRm::EKqpMemoryPool::ScanQuery] &&
        ResourceEstimations.size() <= localResources.ExecutionUnits &&
        ResourceEstimations.size() <= MAX_NON_PARALLEL_TASKS_EXECUTION_LIMIT)
    {
        ui64 selfNodeId = ExecuterId.NodeId();
        for(ui64 taskId: ComputeTasks) {
            TasksPerNode[selfNodeId].push_back(taskId);
        }

        return nullptr;
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

    auto planner = CreateKqpGreedyPlanner();

    auto ctx = TlsActivationContext->AsActorContext();
    if (ctx.LoggerSettings() && ctx.LoggerSettings()->Satisfies(NActors::NLog::PRI_DEBUG, NKikimrServices::KQP_EXECUTER)) {
        planner->SetLogFunc([TxId = TxId](TStringBuf msg) { LOG_D(msg); });
    }

    THashMap<ui64, size_t> nodeIdtoIdx;
    for (size_t idx = 0; idx < ResourcesSnapshot.size(); ++idx) {
        nodeIdtoIdx[ResourcesSnapshot[idx].nodeid()] = idx;
    }

    auto plan = planner->Plan(ResourcesSnapshot, ResourceEstimations);

    THashMap<ui64, ui64> alreadyAssigned;
    for(auto& [nodeId, tasks] : TasksPerNode) { 
        for(ui64 taskId: tasks) {
            alreadyAssigned.emplace(taskId, nodeId);
        }
    }

    if (!plan.empty()) {
        for (auto& group : plan) {
            for(ui64 taskId: group.TaskIds) {
                auto [it, success] = alreadyAssigned.emplace(taskId, group.NodeId);
                if (success) {
                    TasksPerNode[group.NodeId].push_back(taskId);
                }
            }
        }

        return nullptr;
    }  else {
        // what is here?
        Cerr << (*UserRequestContext);
        Y_FAIL_S("Fail point 123#!");
        // UserRequestContext->Out(Cerr);
        auto ev = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
            "Not enough resources to execute query");
        return std::make_unique<IEventHandle>(ExecuterId, ExecuterId, ev.Release());
    }
}

const IKqpGateway::TKqpSnapshot& TKqpPlanner::GetSnapshot() const {
    return TasksGraph.GetMeta().Snapshot;
}

void TKqpPlanner::ExecuteDataComputeTask(ui64 taskId, bool shareMailbox) {

    auto& task = TasksGraph.GetTask(taskId);
    NYql::NDqProto::TDqTask* taskDesc = ArenaSerializeTaskToProto(TasksGraph, task);

    NYql::NDq::TComputeRuntimeSettings settings;
    if (Deadline) {
        settings.Timeout = Deadline - TAppData::TimeProvider->Now();
    }
    //settings.ExtraMemoryAllocationPool = NRm::EKqpMemoryPool::DataQuery;
    settings.ExtraMemoryAllocationPool = NRm::EKqpMemoryPool::Unspecified;
    settings.FailOnUndelivery = true;
    settings.StatsMode = GetDqStatsMode(StatsMode);
    settings.UseSpilling = false;

    NYql::NDq::TComputeMemoryLimits limits;
    limits.ChannelBufferSize = 50_MB;
    limits.MkqlLightProgramMemoryLimit = MkqlMemoryLimit > 0 ? std::min(500_MB, MkqlMemoryLimit) : 500_MB;
    limits.MkqlHeavyProgramMemoryLimit = MkqlMemoryLimit > 0 ? std::min(2_GB, MkqlMemoryLimit) : 2_GB;

    auto& taskOpts = taskDesc->GetProgram().GetSettings();
    auto limit = taskOpts.GetHasMapJoin() /* || opts.GetHasSort()*/
        ? limits.MkqlHeavyProgramMemoryLimit
        : limits.MkqlLightProgramMemoryLimit;

    limits.MemoryQuotaManager = std::make_shared<NYql::NDq::TGuaranteeQuotaManager>(limit * 2, limit);

    auto computeActor = NKikimr::NKqp::CreateKqpComputeActor(ExecuterId, TxId, taskDesc, AsyncIoFactory,
        AppData()->FunctionRegistry, settings, limits, NWilson::TTraceId(), TasksGraph.GetMeta().GetArenaIntrusivePtr());

    auto computeActorId = shareMailbox ? TlsActivationContext->AsActorContext().RegisterWithSameMailbox(computeActor) : TlsActivationContext->AsActorContext().Register(computeActor);
    task.ComputeActorId = computeActorId;

    LOG_D("Executing task: " << taskId << " on compute actor: " << task.ComputeActorId);
    
    auto result = PendingComputeActors.emplace(task.ComputeActorId, TProgressStat());
    YQL_ENSURE(result.second);
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
        << ", execType: " << (IsDataQuery ? "Data" : "Scan")
        << ", snapshot: {" << GetSnapshot().TxId << ", " << GetSnapshot().Step << "}");

    nComputeTasks = ComputeTasks.size();

    if (IsDataQuery) {
        bool shareMailbox = (ComputeTasks.size() <= 1);
        for (ui64 taskId : ComputeTasks) {
            ExecuteDataComputeTask(taskId, shareMailbox);
        }
        ComputeTasks.clear();
    }
    
    if (nComputeTasks == 0 && TasksPerNode.size() == 1 && (AsyncIoFactory != nullptr) && DoOptimization && IsDataQuery) {
        // query affects a single key or shard, so it might be more effective
        // to execute this task locally so we can avoid useless overhead for remote task launching.
        for(auto& [shardId, tasks]: TasksPerNode) {
            for(ui64 taskId: tasks) {
                ExecuteDataComputeTask(taskId, true);
            }
        }

    } else {
        for (ui64 taskId : ComputeTasks) {
            PendingComputeTasks.insert(taskId);
        }

        for (auto& [shardId, tasks] : TasksPerNode) {
            for (ui64 taskId : tasks) {
                PendingComputeTasks.insert(taskId);
            }
        }

        auto err = AssignTasksToNodes();
        if (err) {
            return err;
        }

        for(auto& [nodeId, tasks] : TasksPerNode) {
            SortUnique(tasks);
            auto& request = Requests.emplace_back(std::move(tasks), CalcSendMessageFlagsForNode(nodeId), nodeId);
            request.SerializedRequest = SerializeRequest(request);
            auto ev = CheckTaskSize(TxId, request.SerializedRequest->Record.GetTasks());
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

THashMap<TActorId, TProgressStat>& TKqpPlanner::GetPendingComputeActors() {
    return PendingComputeActors;
}

THashSet<ui64>& TKqpPlanner::GetPendingComputeTasks() {
    return PendingComputeTasks;
}

void TKqpPlanner::PrepareToProcess() {
    auto rmConfig = GetKqpResourceManager()->GetConfig();

    ui32 tasksCount = ComputeTasks.size();
    for (auto& [shardId, tasks] : TasksPerNode) {
        tasksCount += tasks.size();
    }

    ResourceEstimations.resize(tasksCount);
    LocalRunMemoryEst = 0;

    for (size_t i = 0; i < ComputeTasks.size(); ++i) {
        BuildInitialTaskResources(TasksGraph, ComputeTasks[i], ResourceEstimations[i]);
        EstimateTaskResources(rmConfig, ResourceEstimations[i], ComputeTasks.size());
        LocalRunMemoryEst += ResourceEstimations[i].TotalMemoryLimit;
    }

    ui32 currentEst = ComputeTasks.size();
    for(auto& [nodeId, tasks] : TasksPerNode) {
        for (ui64 taskId: tasks) {
            BuildInitialTaskResources(TasksGraph, taskId, ResourceEstimations[currentEst]);
            EstimateTaskResources(rmConfig, ResourceEstimations[currentEst], tasks.size());
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
std::unique_ptr<TKqpPlanner> CreateKqpPlanner(TKqpTasksGraph& tasksGraph, ui64 txId, const TActorId& executer,
    const IKqpGateway::TKqpSnapshot& snapshot,
    const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TInstant deadline,
    const Ydb::Table::QueryStatsCollection::Mode& statsMode,
    bool withSpilling, const TMaybe<NKikimrKqp::TRlPath>& rlPath, NWilson::TSpan& executerSpan,
    TVector<NKikimrKqp::TKqpNodeResources>&& resourcesSnapshot, const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig,
    bool isDataQuery, ui64 mkqlMemoryLimit, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, bool doOptimization,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext)
{
    return std::make_unique<TKqpPlanner>(tasksGraph, txId, executer, snapshot,
        database, userToken, deadline, statsMode, withSpilling, rlPath, executerSpan,
        std::move(resourcesSnapshot), executerRetriesConfig, isDataQuery, mkqlMemoryLimit, asyncIoFactory, doOptimization, userRequestContext);
}

} // namespace NKikimr::NKqp
