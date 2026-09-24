#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

namespace NKikimr::NKqp {

// Parameters:
//   runActorId   – actor that receives TEvKqp::TEvAbortExecution
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
