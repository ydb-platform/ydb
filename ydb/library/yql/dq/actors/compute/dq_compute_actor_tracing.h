#pragma once

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

namespace NYql::NDq {

struct TDqTaskRunnerStats;

void FillComputeTraceStats(const TDqTaskRunnerStats& source, NDqProto::TDqTaskStats& target);
void AddComputeTraceAttributes(NWilson::TSpan& span, const NDqProto::TDqComputeActorStats& stats);

}
