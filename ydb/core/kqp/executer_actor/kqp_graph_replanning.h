#pragma once

#include "kqp_task_planning.h"

#include <ydb/core/fq/libs/graph_params/proto/graph_params.pb.h>
#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr::NKqp {

bool CollectReplanningConstraints(const NKikimrKqp::TQueryPhysicalGraph& previous,
    TTaskPlanningConstraints& constraints, TString& fallbackReason);

NFq::NProto::TGraphParams MakeCheckpointGraphParams(const NKikimrKqp::TQueryPhysicalGraph& graph);

bool MaterializedGraphsEqual(const NKikimrKqp::TQueryPhysicalGraph& previous,
    const NKikimrKqp::TQueryPhysicalGraph& candidate);

void RefreshPqSourcePartitions(NKqpProto::TKqpExternalSource& source,
    const THashMap<TString, ui32>& clusterPartitions, ui32 maxPartitions, ui32 maxTasksPerStage);

} // namespace NKikimr::NKqp
