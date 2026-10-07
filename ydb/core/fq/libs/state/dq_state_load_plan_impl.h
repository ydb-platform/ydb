#pragma once

#include "dq_stage_state_recovery_info.h"
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/dq/proto/dq_state_load_plan.pb.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NFq {

using TStateLoadPlan = THashMap<ui64, NYql::NDqProto::NDqStateLoadPlan::TTaskPlan>;
using TCheckpointTaskStates = THashMap<ui64, NYql::NDq::TComputeActorState>;
using TSourceRecoverySet = THashSet<std::pair<ui64, ui64>>; // Target task ID, input index.

/*
    Validates operator and output state preservation, maps source offsets and compatible
    tied aggregation checkpoints, checks full key/state types, task counts and compiled
    shuffle routing, and identifies changed consumers requiring source preparation.
    FORCE permits unsupported state loss but never transfers an incompatible program checkpoint.
*/
bool MakeContinueFromStreamingOffsetsPlan(const TGraphStateInfo& src, const TGraphStateInfo& dst,
    bool force, TStateLoadPlan& plan, TSourceRecoverySet& sourcesToPrepare, NYql::TIssues& issues);

/*
    Acounted all stateful operators with supported replay (now only GROUP BY HoppingWindow),
    read offsets and checkpoint states provide consistent at-least-once state recreation after update if function succeed
*/
bool MakeHistoryReplayPlan(const TGraphStateInfo& src, const TGraphStateInfo& dst, const TCheckpointTaskStates& states, TStateLoadPlan& plan, NYql::TIssues& issues);

/*
    Based on stateful operators with replay support provide operators state and read time in order to
    produce consistent data from time point at least outputStartTimeUs. If provided explicit read start time
    recovery plan will just prevent incomplete data reporting before outputStartTimeUs.
*/
bool MakeOutputStartTimeReplayPlan(const TGraphStateInfo& graph, ui64 outputStartTimeUs, bool useSourceDisposition, TStateLoadPlan& plan, NYql::TIssues& issues);

} // namespace NFq
