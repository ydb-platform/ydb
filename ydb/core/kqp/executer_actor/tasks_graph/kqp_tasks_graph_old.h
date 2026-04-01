#pragma once

#include "kqp_tasks_graph.h"

namespace NKikimr::NKqp {

class TKqpTasksGraphOld : public TKqpTasksGraph {
public:
    using TKqpTasksGraph::TKqpTasksGraph;

private:
    size_t DoBuildAllTasks(const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot, TQueryExecutionStats* stats) override;

    TMaybe<size_t> BuildScanTasksFromSource(TStageInfo& stageInfo, bool limitTasksPerNode, TQueryExecutionStats* stats);
    void BuildFullTextScanTasksFromSource(TStageInfo& stageInfo, TQueryExecutionStats* stats);
    void BuildSysViewTasksFromSource(TStageInfo& stageInfo);
    void BuildReadTasksFromSource(TStageInfo& stageInfo, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot, ui32 scheduledTaskCount);
    void BuildSysViewScanTasks(TStageInfo& stageInfo);
    bool BuildComputeTasks(TStageInfo& stageInfo, const ui32 nodesCount);
    void BuildScanTasksFromShards(TStageInfo& stageInfo, bool enableShuffleElimination, TQueryExecutionStats* stats);
};

} // namespace NKikimr::NKqp
