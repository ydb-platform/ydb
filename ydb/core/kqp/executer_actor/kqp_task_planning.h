#pragma once

#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/library/yql/dq/tasks/dq_tasks_graph.h>

namespace NKikimr::NKqp {

struct TTaskPlanningConstraints {
    THashMap<NYql::NDq::TStageId, ui32> FixedTaskCountByStage;
};

// Execution-local source descriptors. The compiled query remains immutable.
using TPqSourcePlanningSnapshot = THashMap<NYql::NDq::TStageId, NKqpProto::TKqpExternalSource>;

} // namespace NKikimr::NKqp
