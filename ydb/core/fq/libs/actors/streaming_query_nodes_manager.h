#pragma once

#include <ydb/core/fq/libs/events/event_subspace.h>
#include <ydb/core/fq/libs/graph_params/proto/graph_params.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

namespace NFq {

// Events for StreamingQueryNodesManager
struct TEvStreamingQueryNodesManager {
    enum EEv : ui32 {
        EvAbortQuery = YqEventSubspaceBegin(TYqEventSubspace::StreamingQueryNodesManager),
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(TYqEventSubspace::StreamingQueryNodesManager),
        "All events must be in their subspace");

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
//   - waits for startDelay to collect TEvDqCompute::TEvState events, which
//     identify the nodes running the query's compute actors;
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
//   graphParams  – serialized DQ task graph snapshot
//   checkPeriod  – how often to repeat the check (default 1 minute)
//   startDelay   – time to wait for initial compute states before first check
NActors::IActor* CreateStreamingQueryNodesManager(
    NActors::TActorId runActorId,
    TString tenantName,
    ui64 taskCount,
    TString queryId,
    const NProto::TGraphParams& graphParams,
    TDuration checkPeriod = TDuration::Minutes(1),
    TDuration startDelay = TDuration::Minutes(1));

} // namespace NFq
