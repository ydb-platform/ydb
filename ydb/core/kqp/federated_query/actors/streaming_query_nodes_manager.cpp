#include "streaming_query_nodes_manager.h"

#include <ydb/core/fq/libs/checkpointing/events/events.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/mind/tenant_node_enumeration.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/providers/pq/common/pq_partitions.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/system/env.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_EXECUTER

#define LOG_D(msg, ...) YDB_LOG_DEBUG(msg, {"queryId", QueryId}, ##__VA_ARGS__)
#define LOG_I(msg, ...) YDB_LOG_INFO(msg, {"queryId", QueryId}, ##__VA_ARGS__)
#define LOG_W(msg, ...) YDB_LOG_WARN(msg, {"queryId", QueryId}, ##__VA_ARGS__)

namespace NKikimr::NKqp {

using namespace NActors;

namespace {

// Tag for periodic wakeup timer.
constexpr ui64 WakeupTag = 1;

TDuration GetDurationFromEnv(const char* name, TDuration defaultValue) {
    ui64 ms = 0;
    if (TryFromString<ui64>(GetEnv(name), ms) && ms) {
        return TDuration::MilliSeconds(ms);
    }
    return defaultValue;
}

bool IsTopicSourceTask(const NYql::NDqProto::TDqTask& task) {
    for (const auto& input : task.GetInputs()) {
        if (input.GetTypeCase() == NYql::NDqProto::TTaskInput::kSource
            && input.GetSource().GetType() == NYql::NDq::PqSource)
        {
            return true;
        }
    }
    return false;
}

ui64 GetTopicPartitionsCount(const NYql::NDqProto::TDqTask& task) {
    if (task.ReadRangesSize() == 0) {
        return 0;
    }

    NYql::NPq::NProto::TDqPqTopicSource topicSource;
    bool topicSourceFound = false;

    if (const auto it = task.GetTaskParams().find("pq_topic_source"); it != task.GetTaskParams().end()) {
        topicSourceFound = topicSource.ParseFromString(it->second);
    }

    if (!topicSourceFound) {
        for (const auto& input : task.GetInputs()) {
            if (input.GetTypeCase() == NYql::NDqProto::TTaskInput::kSource
                && input.GetSource().GetType() == NYql::NDq::PqSource
                && input.GetSource().GetSettings().UnpackTo(&topicSource))
            {
                topicSourceFound = true;
                break;
            }
        }
    }

    std::vector<NYql::NDq::TPartitionKey> federatedClusters;
    if (topicSourceFound) {
        for (const auto& cluster : topicSource.GetFederatedClusters()) {
            federatedClusters.push_back({
                .Cluster = cluster.GetName(),
                .PartitionId = cluster.GetPartitionsCount(),
            });
        }
    }

    TVector<TString> readRanges;
    readRanges.reserve(task.ReadRangesSize());
    for (const auto& readRange : task.GetReadRanges()) {
        readRanges.push_back(readRange);
    }

    THashMap<TString, TString> taskParams;
    for (const auto& [key, value] : task.GetTaskParams()) {
        taskParams.emplace(key, value);
    }

    const auto readTaskParams = NYql::NDq::ExtractReadTaskParams(taskParams, readRanges);
    return NYql::NDq::GetPartitionsToRead(readTaskParams, federatedClusters).size();
}

class TStreamingQueryNodesManager
    : public TActorBootstrapped<TStreamingQueryNodesManager>
{
public:
    TStreamingQueryNodesManager(
        TActorId runActorId,
        TString tenantName,
        TString queryId,
        const google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& tasks,
        TDuration checkPeriod,
        TDuration startDelay,
        ui64 maxTasksPerStage)
        : RunActorId(runActorId)
        , TenantName(std::move(tenantName))
        , QueryId(std::move(queryId))
        , CheckPeriod(GetDurationFromEnv("YDB_TEST_NODES_MANAGER_CHECK_PERIOD_MS", checkPeriod))
        , StartDelay(GetDurationFromEnv("YDB_TEST_NODES_MANAGER_START_DELAY_MS", startDelay))
        , MaxTasksPerStage(maxTasksPerStage)
    {
        for (const auto& task : tasks) {
            if (IsTopicSourceTask(task)) {
                TopicSourceTaskNodes.emplace(task.GetId(), Nothing());
                TopicPartitionsCount += GetTopicPartitionsCount(task);
            }
        }
    }

    static constexpr char ActorName[] = "STREAMING_QUERY_NODES_MANAGER";

    void Bootstrap() {
        LOG_I("StreamingQueryNodesManager started",
            {"tenant", TenantName},
            {"taskCount", TopicSourceTaskNodes.size()},
            {"partitionCount", TopicPartitionsCount},
            {"checkPeriod", CheckPeriod},
            {"startDelay", StartDelay});

        if (TopicSourceTaskNodes.empty()) {
            PassAway();
            return;
        }
        Become(&TThis::StateWork);
    }

    STRICT_STFUNC(StateWork,
        hFunc(NFq::TEvCheckpointCoordinator::TEvReadyState, Handle);
        hFunc(NKikimr::TEvTenantNodeEnumerator::TEvLookupResult, Handle);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        hFunc(TEvents::TEvWakeup, Handle);
    )

private:
    // -------------------------------------------------------------------------
    // Handlers
    // -------------------------------------------------------------------------

    void Handle(NFq::TEvCheckpointCoordinator::TEvReadyState::TPtr& ev) {
        if (Ready) {
            return;
        }
        for (const auto& task : ev->Get()->Tasks) {
            auto topicSourceTask = TopicSourceTaskNodes.find(task.Id);
            if (topicSourceTask == TopicSourceTaskNodes.end()) {
                continue;
            }
            const ui32 nodeId = task.ActorId.NodeId();
            topicSourceTask->second = nodeId;
            QueryNodes.insert(nodeId);
            LOG_D("Task node updated",
                {"taskId", task.Id},
                {"nodeId", nodeId});
        }
        Ready = true;
        Schedule(StartDelay, new TEvents::TEvWakeup(WakeupTag));
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (!Ready || ev->Get()->Tag != WakeupTag) {
            return;
        }
        ScheduleWakeup();

        if (LookupInFlight) {
            return;
        }
        LookupInFlight = true;
        Register(NKikimr::CreateTenantNodeEnumerationLookup(SelfId(), TenantName));
    }

    void Handle(NKikimr::TEvTenantNodeEnumerator::TEvLookupResult::TPtr& ev) {
        LookupInFlight = false;

        if (!ev->Get()->Success) {
            LOG_W("TenantNodeEnumerationLookup failed, will retry on next wakeup");
            return;
        }

        CheckNodes(ev->Get()->AssignedNodes);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    void CheckNodes(const TVector<ui32>& nodes) {
        if (!Ready) {
            return;
        }
        const ui64 totalNodes = nodes.size();

        LOG_D("Received tenant node list",
            {"totalTenantNodes", totalNodes},
            {"topicSourceTasks", TopicSourceTaskNodes.size()},
            {"topicPartitions", TopicPartitionsCount});

        if (totalNodes == 0) {
            LOG_W("Tenant has no nodes, skipping check");
            return;
        }

        if (AlreadyAborted) {
            return;
        }

        const ui64 nodesWithQuery = QueryNodes.size();
        const ui64 expectedTasks = NYql::NDq::GetExpectedTopicReadTasks(
            TopicPartitionsCount, MaxTasksPerStage, !MaxTasksPerStage);
        const ui64 expectedNodesWithQuery = Min(totalNodes, expectedTasks);

        if (nodesWithQuery < expectedNodesWithQuery) {
            const TString reason = TStringBuilder()
                << "StreamingQuery health check failed: "
                << "nodes with topic reader tasks (" << nodesWithQuery << " [" << JoinSeq(", ", QueryNodes) << "]) "
                << "is less than expected (" << expectedNodesWithQuery << "). "
                << "Total tenant nodes: " << totalNodes << ". "
                << "Expected topic reader tasks: " << expectedTasks << ". "
                << "Query will be aborted.";
            LOG_W(reason);
            Abort(reason);
            return;
        }

        LOG_D("Health check passed",
            {"nodesWithQuery", nodesWithQuery},
            {"expectedNodesWithQuery", expectedNodesWithQuery},
            {"totalNodes", totalNodes},
            {"expectedTasks", expectedTasks});
    }

    void ScheduleWakeup() {
        Schedule(CheckPeriod, new TEvents::TEvWakeup(WakeupTag));
    }

    void Abort(const TString& reason) {
        AlreadyAborted = true;
        Send(RunActorId, new TEvKqp::TEvAbortExecution(NYql::NDqProto::StatusIds::ABORTED, reason));
    }

    // -------------------------------------------------------------------------
    // Members
    // -------------------------------------------------------------------------

    const TActorId RunActorId;
    const TString TenantName;
    const TString QueryId;
    const TDuration CheckPeriod;
    const TDuration StartDelay;
    const ui64 MaxTasksPerStage;

    // Contains topic-source tasks and their node from the ready-state snapshot.
    THashMap<ui64, TMaybe<ui32>> TopicSourceTaskNodes;
    // Nodes hosting topic-source tasks in the ready-state snapshot.
    THashSet<ui32> QueryNodes;
    ui64 TopicPartitionsCount = 0;

    bool Ready = false;
    bool LookupInFlight = false;
    bool AlreadyAborted = false;
};

} // anonymous namespace

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

IActor* CreateStreamingQueryNodesManager(
    TActorId runActorId,
    TString tenantName,
    TString queryId,
    const google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& tasks,
    TDuration checkPeriod,
    TDuration startDelay,
    ui64 maxTasksPerStage)
{
    return new TStreamingQueryNodesManager(
        runActorId,
        std::move(tenantName),
        std::move(queryId),
        tasks,
        checkPeriod,
        startDelay,
        maxTasksPerStage);
}

} // namespace NKikimr::NKqp
