#include "streaming_query_nodes_manager.h"

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

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FQ_RUN_ACTOR

#define LOG_T(msg, ...) YDB_LOG_TRACE(msg, {"queryId", QueryId}, ##__VA_ARGS__)
#define LOG_D(msg, ...) YDB_LOG_DEBUG(msg, {"queryId", QueryId}, ##__VA_ARGS__)
#define LOG_W(msg, ...) YDB_LOG_WARN(msg, {"queryId", QueryId}, ##__VA_ARGS__)
#define LOG_E(msg, ...) YDB_LOG_ERROR(msg, {"queryId", QueryId}, ##__VA_ARGS__)

namespace NFq {

using namespace NActors;

namespace {

// Tag for periodic wakeup timer.
constexpr ui64 WakeupTag = 1;

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
        const NProto::TGraphParams& graphParams,
        TDuration checkPeriod,
        TDuration startDelay)
        : RunActorId(runActorId)
        , TenantName(std::move(tenantName))
        , QueryId(std::move(queryId))
        , GraphParams(graphParams)
        , CheckPeriod(checkPeriod)
        , StartDelay(startDelay)
    {
        for (const auto& task : GraphParams.GetTasks()) {
            if (IsTopicSourceTask(task)) {
                TopicSourceTaskNodes.emplace(task.GetId(), Nothing());
                TopicPartitionsCount += GetTopicPartitionsCount(task);
            }
        }
    }

    static constexpr char ActorName[] = "STREAMING_QUERY_NODES_MANAGER";

    void Bootstrap() {
        LOG_D("StreamingQueryNodesManager started",
            {"tenant", TenantName},
            {"taskCount", TopicSourceTaskNodes.size()},
            {"partitionCount", TopicPartitionsCount},
            {"checkPeriod", CheckPeriod},
            {"startDelay", StartDelay});

        if (TopicSourceTaskNodes.empty()) {
            PassAway();
            return;
        }
        // Give all compute actors time to report their initial state first.
        Schedule(StartDelay, new TEvents::TEvWakeup(WakeupTag));
        Become(&TThis::StateWork);
    }

    STRICT_STFUNC(StateWork,
        hFunc(NYql::NDq::TEvDqCompute::TEvState, Handle);
        hFunc(NKikimr::TEvTenantNodeEnumerator::TEvLookupResult, Handle);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        hFunc(TEvents::TEvWakeup, Handle);
    )

private:
    // -------------------------------------------------------------------------
    // Handlers
    // -------------------------------------------------------------------------

    void Handle(NYql::NDq::TEvDqCompute::TEvState::TPtr& ev) {
        const ui64 taskId = ev->Get()->Record.GetTaskId();
        auto topicSourceTask = TopicSourceTaskNodes.find(taskId);
        if (topicSourceTask == TopicSourceTaskNodes.end()) {
            return;
        }
        topicSourceTask->second = ev->Sender.NodeId();
        LOG_D("Task node updated",
            {"taskId", taskId},
            {"nodeId", ev->Sender.NodeId()});
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag != WakeupTag) {
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
        if (TopicSourceTaskNodes.empty()) {
            return;
        }

        const ui64 totalNodes = nodes.size();

        LOG_D("Received tenant node list",
            {"totalNodes", totalNodes},
            {"topicSourceTasks", TopicSourceTaskNodes.size()},
            {"topicPartitions", TopicPartitionsCount});

        if (totalNodes == 0) {
            LOG_W("Tenant has no nodes, skipping check");
            return;
        }

        if (AlreadyAborted) {
            return;
        }

        THashSet<ui32> queryNodes;
        for (const auto& [_, nodeId] : TopicSourceTaskNodes) {
            if (nodeId) {
                queryNodes.insert(*nodeId);
            }
        }
        const ui64 nodesWithQuery = queryNodes.size();

        // Restart only when topic readers cover less than half of tenant nodes
        // and the query reads more than one partition per five tenant nodes.
        // nodesWithQuery / totalNodes < 0.5  ⟺  nodesWithQuery * 2 < totalNodes
        if (nodesWithQuery * 2 < totalNodes && TopicPartitionsCount > totalNodes / 5) {
            const TString reason = TStringBuilder()
                << "StreamingQuery health check failed: "
                << "nodes with topic reader tasks (" << nodesWithQuery << ") "
                << "is less than half of total tenant nodes (" << totalNodes << "). "
                << "Topic partition count (" << TopicPartitionsCount << ") "
                << "is greater than one fifth of total tenant nodes. "
                << "Query will be aborted.";
            LOG_W(reason);
            Abort(reason);
            return;
        }

        if (nodesWithQuery * 2 < totalNodes) {
            LOG_D("Health check passed: too few topic partitions to restart query",
                {"nodesWithQuery", nodesWithQuery},
                {"totalNodes", totalNodes},
                {"topicPartitions", TopicPartitionsCount});
            return;
        }

        // Check 2: if taskCount <= 2 * nodesWithQuery – do nothing extra.
        // This is already the healthy case; we just log for visibility.
        if (TopicSourceTaskNodes.size() <= 2 * nodesWithQuery) {
            LOG_D("Health check passed",
                {"nodesWithQuery", nodesWithQuery},
                {"totalNodes", totalNodes},
                {"taskCount", TopicSourceTaskNodes.size()});
        } else {
            // Tasks are piling up on fewer nodes than expected – log a warning
            // but do NOT abort here per the spec.
            LOG_W("Task concentration warning: taskCount > 2 * nodesWithQuery",
                {"taskCount", TopicSourceTaskNodes.size()},
                {"nodesWithQuery", nodesWithQuery});
        }
    }

    void ScheduleWakeup() {
        Schedule(CheckPeriod, new TEvents::TEvWakeup(WakeupTag));
    }

    void Abort(const TString& reason) {
        AlreadyAborted = true;
        Send(RunActorId, new TEvStreamingQueryNodesManager::TEvAbortQuery(reason));
    }

    // -------------------------------------------------------------------------
    // Members
    // -------------------------------------------------------------------------

    const TActorId RunActorId;
    const TString TenantName;
    const TString QueryId;
    const NProto::TGraphParams GraphParams;
    const TDuration CheckPeriod;
    const TDuration StartDelay;

    // Contains topic-source tasks and their latest known node, when reported.
    THashMap<ui64, TMaybe<ui32>> TopicSourceTaskNodes;
    ui64 TopicPartitionsCount = 0;

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
    const NProto::TGraphParams& graphParams,
    TDuration checkPeriod,
    TDuration startDelay)
{
    return new TStreamingQueryNodesManager(
        runActorId,
        std::move(tenantName),
        std::move(queryId),
        graphParams,
        checkPeriod,
        startDelay);
}

} // namespace NFq
