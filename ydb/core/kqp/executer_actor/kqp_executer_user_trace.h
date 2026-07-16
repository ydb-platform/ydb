#pragma once

#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NYql::NDqProto {
    class TDqExecutionStats;
}

namespace NKikimr::NKqp {

// Emits one user-facing span per execution stage as children of `parent` (the user Execute span),
// reconstructed post-execution from finalized stage stats. Named by the stage's dominant operator
// (Read/Write table, Join, Aggregate, Filter). No-op when the user channel didn't sample the query.
void EmitUserStagePhases(const NWilson::TTraceId& parent, const NYql::NDqProto::TDqExecutionStats& stats);

} // namespace NKikimr::NKqp
