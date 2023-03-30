#include "kqp_executer_stats.h"
#include "kqp_planner.h"
#include "kqp_planner_strategy.h"
#include "kqp_shards_resolver.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/wilson.h>

#include <util/generic/set.h>

namespace NKikimr::NKqp {

#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)

using namespace NYql;

// Task can allocate extra memory during execution.
// So, we estimate total memory amount required for task as apriori task size multiplied by this constant.
constexpr ui32 MEMORY_ESTIMATION_OVERFLOW = 2;
constexpr ui32 MAX_NON_PARALLEL_TASKS_EXECUTION_LIMIT = 4;

TKqpPlanner::TKqpPlanner(ui64 txId, const TActorId& executer, TVector<NDqProto::TDqTask>&& computeTasks,
    THashMap<ui64, TVector<NDqProto::TDqTask>>&& mainTasksPerNode, const IKqpGateway::TKqpSnapshot& snapshot,
    const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TInstant deadline,
    const Ydb::Table::QueryStatsCollection::Mode& statsMode, bool disableLlvmForUdfStages, bool enableLlvm,
    bool withSpilling, const TMaybe<NKikimrKqp::TRlPath>& rlPath, NWilson::TSpan& executerSpan,
    TVector<NKikimrKqp::TKqpNodeResources>&& resourcesSnapshot,
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig)
    : TxId(txId)
    , ExecuterId(executer)
    , ComputeTasks(std::move(computeTasks))
    , MainTasksPerNode(std::move(mainTasksPerNode))
    , Snapshot(snapshot)
    , Database(database)
    , UserToken(userToken)
    , Deadline(deadline)
    , StatsMode(statsMode)
    , DisableLlvmForUdfStages(disableLlvmForUdfStages)
    , EnableLlvm(enableLlvm)
    , WithSpilling(withSpilling)
    , RlPath(rlPath)
    , ResourcesSnapshot(std::move(resourcesSnapshot))
    , ExecuterSpan(executerSpan)
    , ExecuterRetriesConfig(executerRetriesConfig)
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
    auto& requestData = Requests[requestId];

    if (requestData.RetryNumber == ExecuterRetriesConfig.GetMaxRetryNumber() + 1) {
        return false;
    }

    auto ev = MakeHolder<TEvKqpNode::TEvStartKqpTasksRequest>();
    ev->Record = requestData.request;

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
            TlsActivationContext->Send(std::make_unique<NActors::IEventHandle>(anotherTarget, ExecuterId, ev.Release(),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, requestId,  nullptr, ExecuterSpan.GetTraceId()));
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

    TlsActivationContext->Send(std::make_unique<NActors::IEventHandle>(target, ExecuterId, ev.Release(),
        requestData.flag, requestId,  nullptr, ExecuterSpan.GetTraceId()));
    return true;
}

void TKqpPlanner::ProcessTasksForDataExecuter() {

    long requestsCnt = 0;

    for (auto& [nodeId, tasks] : MainTasksPerNode) {

        auto& requestData = Requests.emplace_back();

        requestData.request.SetTxId(TxId);
        ActorIdToProto(ExecuterId, requestData.request.MutableExecuterActorId());

        if (Deadline) {
            TDuration timeout = Deadline - TAppData::TimeProvider->Now();
            requestData.request.MutableRuntimeSettings()->SetTimeoutMs(timeout.MilliSeconds());
        }

        requestData.request.MutableRuntimeSettings()->SetExecType(NDqProto::TComputeRuntimeSettings::DATA);
        requestData.request.MutableRuntimeSettings()->SetStatsMode(GetDqStatsMode(StatsMode));
        requestData.request.MutableRuntimeSettings()->SetUseLLVM(false);
        requestData.request.SetStartAllOrFail(true);

        for (auto&& task : tasks) {
            requestData.request.AddTasks()->Swap(&task);
        }

        auto target = MakeKqpNodeServiceID(nodeId);

        requestData.flag = CalcSendMessageFlagsForNode(nodeId);
        requestsCnt++;

        SendStartKqpTasksRequest(Requests.size() - 1, target);
    }

    if (ExecuterSpan) {
        ExecuterSpan.Attribute("requestsCnt", requestsCnt);
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

void TKqpPlanner::ProcessTasksForScanExecuter() {
    PrepareToProcess();

    auto localResources = GetKqpResourceManager()->GetLocalResources();
    Y_UNUSED(MEMORY_ESTIMATION_OVERFLOW);
    if (LocalRunMemoryEst * MEMORY_ESTIMATION_OVERFLOW <= localResources.Memory[NRm::EKqpMemoryPool::ScanQuery] &&
        ResourceEstimations.size() <= localResources.ExecutionUnits &&
        ResourceEstimations.size() <= MAX_NON_PARALLEL_TASKS_EXECUTION_LIMIT)
    {
        RunLocal(ResourcesSnapshot);
        return;
    }

    if (ResourcesSnapshot.empty() || (ResourcesSnapshot.size() == 1 && ResourcesSnapshot[0].GetNodeId() == ExecuterId.NodeId())) {
        // try to run without memory overflow settings
        if (LocalRunMemoryEst <= localResources.Memory[NRm::EKqpMemoryPool::ScanQuery] &&
            ResourceEstimations.size() <= localResources.ExecutionUnits)
        {
            RunLocal(ResourcesSnapshot);
            return;
        }

        LOG_E("Not enough resources to execute query locally and no information about other nodes");
        auto ev = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
            "Not enough resources to execute query locally and no information about other nodes (estimation: "
            + ToString(LocalRunMemoryEst) + ";" + GetEstimationsInfo() + ")");

        TlsActivationContext->Send(std::make_unique<IEventHandle>(ExecuterId, ExecuterId, ev.Release()));
        return;
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

    long requestsCnt = 0;

    if (!plan.empty()) {
        for (auto& group : plan) {
            auto& requestData = Requests.emplace_back();
            PrepareKqpNodeRequest(requestData.request, THashSet<ui64>(group.TaskIds.begin(), group.TaskIds.end()));
            AddScansToKqpNodeRequest(requestData.request, group.NodeId);

            auto target = MakeKqpNodeServiceID(group.NodeId);
            requestData.flag = CalcSendMessageFlagsForNode(group.NodeId);

            SendStartKqpTasksRequest(Requests.size() - 1, target);
            ++requestsCnt;
        }

        TVector<ui64> nodes;
        nodes.reserve(MainTasksPerNode.size());
        for (auto& [nodeId, _]: MainTasksPerNode) {
            nodes.push_back(nodeId);
        }

        for (ui64 nodeId: nodes) {
            auto& requestData = Requests.emplace_back();
            PrepareKqpNodeRequest(requestData.request, {});
            AddScansToKqpNodeRequest(requestData.request, nodeId);

            auto target = MakeKqpNodeServiceID(nodeId);
            requestData.flag = CalcSendMessageFlagsForNode(nodeId);
            LOG_D("Send request to kqpnode: " << target << ", node_id: " << ExecuterId.NodeId() << ", TxId: " << TxId);
            SendStartKqpTasksRequest(Requests.size() - 1, target);
            ++requestsCnt;
        }
        Y_VERIFY(MainTasksPerNode.empty());
    } else {
        auto ev = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
            "Not enough resources to execute query");

        TlsActivationContext->Send(std::make_unique<IEventHandle>(ExecuterId, ExecuterId, ev.Release()));
    }

    if (ExecuterSpan) {
        ExecuterSpan.Attribute("requestsCnt", requestsCnt);
    }
}

TString TKqpPlanner::GetEstimationsInfo() const {
    TStringStream ss;
    ss << "ComputeTasks:" << ComputeTasks.size() << ";NodeTasks:";
    if (auto it = MainTasksPerNode.find(ExecuterId.NodeId()); it != MainTasksPerNode.end()) {
        ss << it->second.size() << ";";
    } else {
        ss << "0;";
    }
    return ss.Str();
}

void TKqpPlanner::PrepareToProcess() {
    auto rmConfig = GetKqpResourceManager()->GetConfig();

    ui32 tasksCount = ComputeTasks.size();
    for (auto& [shardId, tasks] : MainTasksPerNode) {
        tasksCount += tasks.size();
    }

    ResourceEstimations.resize(tasksCount);
    LocalRunMemoryEst = 0;

    for (size_t i = 0; i < ComputeTasks.size(); ++i) {
        EstimateTaskResources(ComputeTasks[i], rmConfig, ResourceEstimations[i], ComputeTasks.size());
        LocalRunMemoryEst += ResourceEstimations[i].TotalMemoryLimit;
    }
    if (auto it = MainTasksPerNode.find(ExecuterId.NodeId()); it != MainTasksPerNode.end()) {
        for (size_t i = 0; i < it->second.size(); ++i) {
            EstimateTaskResources(it->second[i], rmConfig, ResourceEstimations[i + ComputeTasks.size()], it->second.size());
            LocalRunMemoryEst += ResourceEstimations[i + ComputeTasks.size()].TotalMemoryLimit;
        }
    }
    Sort(ResourceEstimations, [](const auto& l, const auto& r) { return l.TotalMemoryLimit > r.TotalMemoryLimit; });
}

ui64 TKqpPlanner::GetComputeTasksNumber() const {
    return ComputeTasks.size();
}

ui64 TKqpPlanner::GetMainTasksNumber() const {
    return MainTasksPerNode.size();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/// Local Execution
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void TKqpPlanner::RunLocal(const TVector<NKikimrKqp::TKqpNodeResources>& snapshot) {
    LOG_D("Execute query locally");

    auto& requestData = Requests.emplace_back();
    PrepareKqpNodeRequest(requestData.request, {});
    AddScansToKqpNodeRequest(requestData.request, ExecuterId.NodeId());

    auto target = MakeKqpNodeServiceID(ExecuterId.NodeId());
    requestData.flag = CalcSendMessageFlagsForNode(ExecuterId.NodeId());
    LOG_D("Send request to kqpnode: " << target << ", node_id: " << ExecuterId.NodeId() << ", TxId: " << TxId);
    SendStartKqpTasksRequest(Requests.size() - 1, target);

    long requestsCnt = 1;

    TVector<ui64> nodes;
    for (const auto& pair: MainTasksPerNode) {
        nodes.push_back(pair.first);
        YQL_ENSURE(pair.first != ExecuterId.NodeId());
    }

    THashMap<ui64, size_t> nodeIdToIdx;
    for (size_t idx = 0; idx < snapshot.size(); ++idx) {
        nodeIdToIdx[snapshot[idx].nodeid()] = idx;
        LOG_D("snapshot #" << idx << ": " << snapshot[idx].ShortDebugString());
    }

    for (auto nodeId: nodes) {
        auto& requestData = Requests.emplace_back();
        PrepareKqpNodeRequest(requestData.request, {});
        AddScansToKqpNodeRequest(requestData.request, nodeId);

        auto target = MakeKqpNodeServiceID(nodeId);
        requestData.flag = CalcSendMessageFlagsForNode(target.NodeId());
        SendStartKqpTasksRequest(Requests.size() - 1, target);

        requestsCnt++;
    }
    Y_VERIFY(MainTasksPerNode.size() == 0);

    if (ExecuterSpan) {
        ExecuterSpan.Attribute("requestsCnt", requestsCnt);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void TKqpPlanner::PrepareKqpNodeRequest(NKikimrKqp::TEvStartKqpTasksRequest& request, THashSet<ui64> taskIds) {
    request.SetTxId(TxId);
    ActorIdToProto(ExecuterId, request.MutableExecuterActorId());

    bool withLLVM = EnableLlvm;

    if (taskIds.empty()) {
        for (auto& taskDesc : ComputeTasks) {
            if (taskDesc.GetId()) {
                if (DisableLlvmForUdfStages && taskDesc.GetProgram().GetSettings().GetHasUdf()) {
                    withLLVM = false;
                }
                AddSnapshotInfoToTaskInputs(taskDesc);
                request.AddTasks()->Swap(&taskDesc);
            }
        }
    } else {
        for (auto& taskDesc : ComputeTasks) {
            if (taskDesc.GetId() && Find(taskIds, taskDesc.GetId()) != taskIds.end()) {
                if (DisableLlvmForUdfStages && taskDesc.GetProgram().GetSettings().GetHasUdf()) {
                    withLLVM = false;
                }
                AddSnapshotInfoToTaskInputs(taskDesc);
                request.AddTasks()->Swap(&taskDesc);
            }
        }
    }

    if (Deadline) {
        TDuration timeout = Deadline - TAppData::TimeProvider->Now();
        request.MutableRuntimeSettings()->SetTimeoutMs(timeout.MilliSeconds());
    }

    request.MutableRuntimeSettings()->SetExecType(NDqProto::TComputeRuntimeSettings::SCAN);
    request.MutableRuntimeSettings()->SetStatsMode(GetDqStatsMode(StatsMode));
    request.MutableRuntimeSettings()->SetUseLLVM(withLLVM);
    request.MutableRuntimeSettings()->SetUseSpilling(WithSpilling);

    if (RlPath) {
        auto rlPath = request.MutableRuntimeSettings()->MutableRlPath();
        rlPath->SetCoordinationNode(RlPath->GetCoordinationNode());
        rlPath->SetResourcePath(RlPath->GetResourcePath());
        rlPath->SetDatabase(Database);
        if (UserToken)
            rlPath->SetToken(UserToken->GetSerializedToken());
    }

    request.SetStartAllOrFail(true);
}

void TKqpPlanner::AddScansToKqpNodeRequest(NKikimrKqp::TEvStartKqpTasksRequest& request, ui64 nodeId) {
    if (!Snapshot.IsValid()) {
        Y_ASSERT(MainTasksPerNode.size() == 0);
        return;
    }

    bool withLLVM = true;
    if (auto nodeTasks = MainTasksPerNode.FindPtr(nodeId)) {
        LOG_D("Adding " << nodeTasks->size() << " scans to KqpNode request");

        request.MutableSnapshot()->SetTxId(Snapshot.TxId);
        request.MutableSnapshot()->SetStep(Snapshot.Step);

        for (auto& task: *nodeTasks) {
            if (DisableLlvmForUdfStages && task.GetProgram().GetSettings().GetHasUdf()) {
                withLLVM = false;
            }
            AddSnapshotInfoToTaskInputs(task);
            request.AddTasks()->Swap(&task);
        }
        MainTasksPerNode.erase(nodeId);
    }

    if (request.GetRuntimeSettings().GetUseLLVM()) {
        request.MutableRuntimeSettings()->SetUseLLVM(withLLVM);
    }
}

ui32 TKqpPlanner::CalcSendMessageFlagsForNode(ui32 nodeId) {
    ui32 flags = IEventHandle::FlagTrackDelivery;
    if (TrackingNodes.insert(nodeId).second) {
        flags |= IEventHandle::FlagSubscribeOnSession;
    }
    return flags;
}

void TKqpPlanner::AddSnapshotInfoToTaskInputs(NYql::NDqProto::TDqTask& task) {
    YQL_ENSURE(Snapshot.IsValid());

    for (auto& input : *task.MutableInputs()) {
        if (input.HasTransform()) {
            auto transform = input.MutableTransform();
            YQL_ENSURE(transform->GetType() == "StreamLookupInputTransformer",
                "Unexpected input transform type: " << transform->GetType());

            const google::protobuf::Any& settingsAny = transform->GetSettings();
            YQL_ENSURE(settingsAny.Is<NKikimrKqp::TKqpStreamLookupSettings>(), "Expected settings type: "
                << NKikimrKqp::TKqpStreamLookupSettings::descriptor()->full_name()
                << " , but got: " << settingsAny.type_url());

            NKikimrKqp::TKqpStreamLookupSettings settings;
            YQL_ENSURE(settingsAny.UnpackTo(&settings), "Failed to unpack settings");

            settings.MutableSnapshot()->SetStep(Snapshot.Step);
            settings.MutableSnapshot()->SetTxId(Snapshot.TxId);

            transform->MutableSettings()->PackFrom(settings);
        }
        if (input.HasSource() && input.GetSource().GetType() == NYql::KqpReadRangesSourceName) {
            auto source = input.MutableSource();
            const google::protobuf::Any& settingsAny = source->GetSettings();

            YQL_ENSURE(settingsAny.Is<NKikimrTxDataShard::TKqpReadRangesSourceSettings>(), "Expected settings type: "
                << NKikimrTxDataShard::TKqpReadRangesSourceSettings::descriptor()->full_name()
                << " , but got: " << settingsAny.type_url());

            NKikimrTxDataShard::TKqpReadRangesSourceSettings settings;
            YQL_ENSURE(settingsAny.UnpackTo(&settings), "Failed to unpack settings");

            if (Snapshot.IsValid()) {
                settings.MutableSnapshot()->SetStep(Snapshot.Step);
                settings.MutableSnapshot()->SetTxId(Snapshot.TxId);
            }

            source->MutableSettings()->PackFrom(settings);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
std::unique_ptr<TKqpPlanner> CreateKqpPlanner(ui64 txId, const TActorId& executer, TVector<NYql::NDqProto::TDqTask>&& tasks,
    THashMap<ui64, TVector<NYql::NDqProto::TDqTask>>&& mainTasksPerNode, const IKqpGateway::TKqpSnapshot& snapshot,
    const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TInstant deadline,
    const Ydb::Table::QueryStatsCollection::Mode& statsMode, bool disableLlvmForUdfStages, bool enableLlvm,
    bool withSpilling, const TMaybe<NKikimrKqp::TRlPath>& rlPath, NWilson::TSpan& executerSpan,
    TVector<NKikimrKqp::TKqpNodeResources>&& resourcesSnapshot, const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig)
{
    return std::make_unique<TKqpPlanner>(txId, executer, std::move(tasks), std::move(mainTasksPerNode), snapshot,
        database, userToken, deadline, statsMode, disableLlvmForUdfStages, enableLlvm, withSpilling, rlPath, executerSpan, 
        std::move(resourcesSnapshot), executerRetriesConfig);
}

} // namespace NKikimr::NKqp
