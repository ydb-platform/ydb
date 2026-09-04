#include "streaming_query_nodes_manager.h"

#include <ydb/core/mind/tenant_node_enumeration.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/hash_set.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FQ_RUN_ACTOR

#define LOG_D(msg, ...) YDB_LOG_DEBUG(msg, {"queryId", QueryId_}, ##__VA_ARGS__)
#define LOG_W(msg, ...) YDB_LOG_WARN(msg, {"queryId", QueryId_}, ##__VA_ARGS__)
#define LOG_E(msg, ...) YDB_LOG_ERROR(msg, {"queryId", QueryId_}, ##__VA_ARGS__)

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
        TDuration checkPeriod)
        : RunActorId_(runActorId)
        , TenantName_(std::move(tenantName))
        , TaskCount_(taskCount)
        , QueryId_(std::move(queryId))
        , CheckPeriod_(checkPeriod)
    {}

    static constexpr char ActorName[] = "STREAMING_QUERY_NODES_MANAGER";

    void Bootstrap() {
        LOG_D("StreamingQueryNodesManager started",
            {"tenant", TenantName_},
            {"taskCount", TaskCount_},
            {"checkPeriod", CheckPeriod_});

        // Start first check immediately.
        ScheduleWakeup();
        Become(&TThis::StateWork);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvStreamingQueryNodesManager::TEvSetTaskNodes, Handle);
        hFunc(NKikimr::TEvTenantNodeEnumerator::TEvLookupResult, Handle);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        hFunc(TEvents::TEvWakeup, Handle);
    )

private:
    // -------------------------------------------------------------------------
    // Handlers
    // -------------------------------------------------------------------------

    void Handle(TEvStreamingQueryNodesManager::TEvSetTaskNodes::TPtr& ev) {
        THashSet<ui32> nodeSet(ev->Get()->NodeIds.begin(), ev->Get()->NodeIds.end());
        QueryNodeCount_ = nodeSet.size();
        LOG_D("Task nodes updated",
            {"queryNodeCount", QueryNodeCount_});
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag != WakeupTag) {
            return;
        }
        // Kick off a fresh lookup. The result will arrive in Handle(TEvLookupResult).
        // We guard against launching multiple parallel lookups.
        if (!LookupInFlight_) {
            LookupInFlight_ = true;
            Register(NKikimr::CreateTenantNodeEnumerationLookup(SelfId(), TenantName_));
        }
        ScheduleWakeup();
    }

    void Handle(NKikimr::TEvTenantNodeEnumerator::TEvLookupResult::TPtr& ev) {
        LookupInFlight_ = false;

        if (!ev->Get()->Success) {
            LOG_W("TenantNodeEnumerationLookup failed, will retry on next wakeup");
            return;
        }

        const auto& nodes = ev->Get()->AssignedNodes;
        const ui64 totalNodes = nodes.size();

        LOG_D("Received tenant node list",
            {"totalNodes", totalNodes},
            {"queryNodeCount", QueryNodeCount_});

        if (totalNodes == 0) {
            LOG_W("Tenant has no nodes, skipping check");
            return;
        }

        if (AlreadyAborted_) {
            return;
        }

        // Use the more accurate QueryNodeCount_ if it has been populated via
        // TEvSetTaskNodes; otherwise fall back to min(taskCount, totalNodes).
        const ui64 nodesWithQuery = QueryNodeCount_.Defined()
            ? *QueryNodeCount_
            : Min(TaskCount_, totalNodes);

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
        if (TaskCount_ <= 2 * nodesWithQuery) {
            LOG_D("Health check passed",
                {"nodesWithQuery", nodesWithQuery},
                {"totalNodes", totalNodes},
                {"taskCount", TaskCount_});
        } else {
            // Tasks are piling up on fewer nodes than expected – log a warning
            // but do NOT abort here per the spec.
            LOG_W("Task concentration warning: taskCount > 2 * nodesWithQuery",
                {"taskCount", TaskCount_},
                {"nodesWithQuery", nodesWithQuery});
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    void ScheduleWakeup() {
        Schedule(CheckPeriod_, new TEvents::TEvWakeup(WakeupTag));
    }

    void Abort(const TString& reason) {
        AlreadyAborted_ = true;
        Send(RunActorId_, new TEvStreamingQueryNodesManager::TEvAbortQuery(reason));
    }

    // -------------------------------------------------------------------------
    // Members
    // -------------------------------------------------------------------------

    const TActorId RunActorId_;
    const TString TenantName_;
    const ui64 TaskCount_;
    const TString QueryId_;
    const TDuration CheckPeriod_;

    // Populated via TEvSetTaskNodes once compute actors are placed.
    TMaybe<ui64> QueryNodeCount_;

    bool LookupInFlight_ = false;
    bool AlreadyAborted_ = false;
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
    TDuration checkPeriod)
{
    return new TStreamingQueryNodesManager(
        runActorId,
        std::move(tenantName),
        taskCount,
        std::move(queryId),
        checkPeriod);
}

} // namespace NFq
