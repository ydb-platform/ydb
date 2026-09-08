#include "streaming_query_nodes_manager.h"

#include <ydb/core/mind/tenant_node_enumeration.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

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

class TStreamingQueryNodesManager
    : public TActorBootstrapped<TStreamingQueryNodesManager>
{
public:
    TStreamingQueryNodesManager(
        TActorId runActorId,
        TString tenantName,
        ui64 taskCount,
        TString queryId,
        const NProto::TGraphParams& graphParams,
        TDuration checkPeriod,
        TDuration startDelay)
        : RunActorId(runActorId)
        , TenantName(std::move(tenantName))
        , TaskCount(taskCount)
        , QueryId(std::move(queryId))
        , GraphParams(graphParams)
        , CheckPeriod(checkPeriod)
        , StartDelay(startDelay)
    {}

    static constexpr char ActorName[] = "STREAMING_QUERY_NODES_MANAGER";

    void Bootstrap() {
        LOG_D("StreamingQueryNodesManager started",
            {"tenant", TenantName},
            {"taskCount", TaskCount},
            {"checkPeriod", CheckPeriod},
            {"startDelay", StartDelay});

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
        TaskNodes[ev->Get()->Record.GetTaskId()] = ev->Sender.NodeId();
        LOG_D("Task node updated",
            {"taskId", ev->Get()->Record.GetTaskId()},
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
        const ui64 totalNodes = nodes.size();

        LOG_D("Received tenant node list",
            {"totalNodes", totalNodes},
            {"tasksWithState", TaskNodes.size()});

        if (totalNodes == 0) {
            LOG_W("Tenant has no nodes, skipping check");
            return;
        }

        if (AlreadyAborted) {
            return;
        }

        THashSet<ui32> queryNodes;
        for (const auto& [_, nodeId] : TaskNodes) {
            queryNodes.insert(nodeId);
        }
        const ui64 nodesWithQuery = queryNodes.size();

        // Check 1: fraction of nodes hosting the query must be >= 0.5.
        // nodesWithQuery / totalNodes < 0.5  ⟺  nodesWithQuery * 2 < totalNodes
        if (nodesWithQuery * 2 < totalNodes) {
            const TString reason = TStringBuilder()
                << "StreamingQuery health check failed: "
                << "nodes with query tasks (" << nodesWithQuery << ") "
                << "is less than half of total tenant nodes (" << totalNodes << "). "
                << "Query will be aborted.";
            LOG_W(reason);
            Abort(reason);
            return;
        }

        // Check 2: if taskCount <= 2 * nodesWithQuery – do nothing extra.
        // This is already the healthy case; we just log for visibility.
        if (TaskCount <= 2 * nodesWithQuery) {
            LOG_D("Health check passed",
                {"nodesWithQuery", nodesWithQuery},
                {"totalNodes", totalNodes},
                {"taskCount", TaskCount});
        } else {
            // Tasks are piling up on fewer nodes than expected – log a warning
            // but do NOT abort here per the spec.
            LOG_W("Task concentration warning: taskCount > 2 * nodesWithQuery",
                {"taskCount", TaskCount},
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
    const ui64 TaskCount;
    const TString QueryId;
    const NProto::TGraphParams GraphParams;
    const TDuration CheckPeriod;
    const TDuration StartDelay;

    // Updated from compute actor state events; maps a task to its latest node.
    THashMap<ui64, ui32> TaskNodes;

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
    ui64 taskCount,
    TString queryId,
    const NProto::TGraphParams& graphParams,
    TDuration checkPeriod,
    TDuration startDelay)
{
    return new TStreamingQueryNodesManager(
        runActorId,
        std::move(tenantName),
        taskCount,
        std::move(queryId),
        graphParams,
        checkPeriod,
        startDelay);
}

} // namespace NFq
