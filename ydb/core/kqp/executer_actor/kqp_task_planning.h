#pragma once

#include <ydb/core/kqp/common/kqp_streaming_query_controller.h>
#include <ydb/library/yql/dq/tasks/dq_tasks_graph.h>

namespace NKikimr::NKqp {

struct TTaskPlanningConstraints {
    THashMap<NYql::NDq::TStageId, ui32> FixedTaskCountByStage;
};

} // namespace NKikimr::NKqp
