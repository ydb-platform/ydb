#pragma once

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

namespace NKikimr::NKqp {

// Events for StreamingQueryNodesManager
struct TEvStreamingQueryNodesManager {
    enum EEv : ui32 {
        EvAbortQuery = TKqpStreamingQueryNodesManagerEvents::EvAbortQuery,
        EvEnd,
    };

    static_assert(EvEnd <= EventSpaceEnd(TKikimrEvents::ES_KQP),
        "All events must be in their subspace");

    // Sent by manager → run_actor when checks detect an unhealthy node ratio.
    struct TEvAbortQuery : public NActors::TEventLocal<TEvAbortQuery, EvAbortQuery> {
        TString Reason;

        explicit TEvAbortQuery(TString reason)
            : Reason(std::move(reason))
        {}
    };
};

// Parameters:
//   runActorId   – actor that receives TEvAbortQuery
//   tenantName   – tenant path used for TenantNodeEnumeration lookup
//   queryId      – used for logging
//   tasks        – serialized DQ tasks
//   checkPeriod  – how often to repeat the check (default 1 minute)
//   startDelay   – time to wait for initial compute states before first check
//   maxTasksPerStage – resolved KQP MaxTasksPerStage pragma value
NActors::IActor* CreateStreamingQueryNodesManager(
    NActors::TActorId runActorId,
    TString tenantName,
    TString queryId,
    const google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& tasks,
    TDuration checkPeriod,
    TDuration startDelay,
    ui64 maxTasksPerStage);

} // namespace NKikimr::NKqp
