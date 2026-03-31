#pragma once

#include "kqp_tasks_graph.h"
#include "max_tasks_graph.h"

namespace NKikimr::NKqp {

class TKqpTasksGraphNew : public TKqpTasksGraph {
public:
    TKqpTasksGraphNew(
        const TString& database,
        const TVector<IKqpGateway::TPhysicalTxData>& transactions,
        const NKikimr::NKqp::TTxAllocatorState::TPtr& txAlloc,
        const NKikimrConfig::TTableServiceConfig::TAggregationConfig& aggregationSettings,
        const NKikimrConfig::TTableServiceConfig::TResourceManager& channelSettings,
        const TKqpRequestCounters::TPtr& counters,
        TActorId bufferActorId,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken);

private:
    size_t DoBuildAllTasks(const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot, TQueryExecutionStats* stats) override;

    void CountScanTasksFromSource(const TStageInfo& stageInfo, bool limitTasksPerNode);
    void CountFullTextScanTasksFromSource(const TStageInfo& stageInfo);
    void CountSysViewTasksFromSource(const TStageInfo& stageInfo);
    void CountReadTasksFromSource(const TStageInfo& stageInfo, size_t resourceSnapshotSize, ui32 scheduledTaskCount);
    void CountSysViewScanTasks(const TStageInfo& stageInfo);
    void CountComputeTasks(const TStageInfo& stageInfo, const ui32 nodesCount);
    void CountScanTasksFromShards(const TStageInfo& stageInfo, bool enableShuffleElimination);

    TMaybe<size_t> BuildScanTasksFromSource(TStageInfo& stageInfo, TQueryExecutionStats* stats);
    void BuildFullTextScanTasksFromSource(TStageInfo& stageInfo, TQueryExecutionStats* stats);
    void BuildSysViewTasksFromSource(TStageInfo& stageInfo);
    void BuildReadTasksFromSource(TStageInfo& stageInfo, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot, ui32 scheduledTaskCount);
    void BuildSysViewScanTasks(TStageInfo& stageInfo);
    void BuildComputeTasks(TStageInfo& stageInfo);
    void BuildScanTasksFromShards(TStageInfo& stageInfo, bool enableShuffleElimination, TQueryExecutionStats* stats);

private:
    TMaxTasksGraph MaxTasksGraph;
};

} // namespace NKikimr::NKqp
