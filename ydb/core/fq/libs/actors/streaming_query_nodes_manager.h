#pragma once

#include <ydb/core/fq/libs/events/event_subspace.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>

namespace NFq {

// Events for StreamingQueryNodesManager
struct TEvStreamingQueryNodesManager {
    enum EEv : ui32 {
        EvSetTaskNodes = YqEventSubspaceBegin(TYqEventSubspace::StreamingQueryNodesManager),
        EvAbortQuery,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(TYqEventSubspace::StreamingQueryNodesManager),
        "All events must be in their subspace");

    // Sent by run_actor once compute actors are placed on nodes.
    // Manager uses this information to determine how many distinct nodes host query tasks.
    struct TEvSetTaskNodes : public NActors::TEventLocal<TEvSetTaskNodes, EvSetTaskNodes> {
        TVector<ui32> NodeIds; // one entry per task (may have duplicates)

        explicit TEvSetTaskNodes(TVector<ui32> nodeIds)
            : NodeIds(std::move(nodeIds))
        {}
    };

    // Sent by manager → run_actor when checks detect an unhealthy node ratio.
    struct TEvAbortQuery : public NActors::TEventLocal<TEvAbortQuery, EvAbortQuery> {
        TString Reason;

        explicit TEvAbortQuery(TString reason)
            : Reason(std::move(reason))
        {}
    };
};

// Creates a StreamingQueryNodesManager actor that:
//   - periodically (every checkPeriod) fetches the list of tenant nodes via
//     CreateTenantNodeEnumerationLookup;
//   - checks:
//       (a) ratio = nodesWithQuery / totalTenantNodes >= 0.5  (else abort)
//       (b) totalTasks <= 2 * nodesWithQuery                  (else no action)
//   - sends TEvStreamingQueryNodesManager::TEvAbortQuery to runActorId when
//     the health check fails.
//
// Parameters:
//   runActorId   – actor that receives TEvAbortQuery
//   tenantName   – tenant path used for TenantNodeEnumeration lookup
//   taskCount    – number of DQ tasks in the current graph
//   queryId      – used for logging
//   checkPeriod  – how often to repeat the check (default 1 minute)
NActors::IActor* CreateStreamingQueryNodesManager(
    NActors::TActorId runActorId,
    TString tenantName,
    ui64 taskCount,
    TString queryId,
    TDuration checkPeriod = TDuration::Minutes(1));

} // namespace NFq
