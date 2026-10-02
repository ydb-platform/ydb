#pragma once

#include <ydb/core/fq/libs/graph_params/proto/graph_params.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/dq/proto/dq_state_load_plan.pb.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/hash.h>

namespace NFq {

using TStateLoadPlan = THashMap<ui64, NYql::NDqProto::NDqStateLoadPlan::TTaskPlan>;
using TCheckpointTaskStates = THashMap<ui64, NYql::NDq::TComputeActorState>;

/*
    Recover only reading positions from previous graph into new one.

    NOTE: If query have any stateful operators such continue plan is inconsistent.
          Now this function may be used only as force recovery fallback plan, if other not succeeded.
*/
bool MakeContinueFromStreamingOffsetsPlan(
    const google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& src,
    const google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& dst,
    bool force, TStateLoadPlan& plan, NYql::TIssues& issues);

/*
    Acounted all stateful operators with supported replay (now only GROUP BY HoppingWindow),
    read offsets and checkpoint states provide consistent at-least-once state recreation after update if function succeed
*/
bool MakeHistoryReplayPlan(const NProto::TGraphParams& src, const NProto::TGraphParams& dst, const TCheckpointTaskStates& states, TStateLoadPlan& plan, NYql::TIssues& issues);

/*
    Based on stateful operators with replay support provide operators state and read time in order to
    produce consistent data from time point at least outputStartTimeUs. If provided explicit read start time
    recovery plan will just prevent incomplete data reporting before outputStartTimeUs.
*/
bool MakeOutputStartTimeReplayPlan(const NProto::TGraphParams& graph, ui64 outputStartTimeUs, bool useSourceDisposition, TStateLoadPlan& plan, NYql::TIssues& issues);

} // namespace NFq
