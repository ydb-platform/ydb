#pragma once

#include "../kqp_rbo.h"
#include "kqp_rbo_trace_format.h"

#include <string>
#include <vector>
#include <util/generic/hash.h>

namespace NKikimr {
namespace NKqp {

struct TTraceBuildState {
    struct TOverviewNode {
        const IOperator* Op = nullptr;
        std::string Id;
        std::string Label;
    };

    struct TOverviewEdge {
        std::string Id;
        std::string From;
        std::string To;
        const IOperator* Child = nullptr;
    };

    THashMap<ui32, std::vector<optimizer_trace::Target>> StageTargets;
    THashMap<const IOperator*, std::vector<optimizer_trace::Target>> OperatorTargets;
    THashMap<const IOperator*, std::string> OverviewNodeIds;
    THashMap<std::string, bool> OverviewEdgeIds;
    std::vector<TOverviewNode> OverviewNodes;
    std::vector<TOverviewEdge> OverviewEdges;
    ui64 NextOverviewNodeId = 0;
    ui64 NextOverviewEdgeId = 0;
};

optimizer_trace::Node BuildPlanNodeFromRoot(
    TOpRoot& root,
    TExprContext& ctx,
    ui32 opts,
    TTraceBuildState* state = nullptr);
std::vector<optimizer_trace::Target> GetOperatorTargets(
    const TTraceBuildState& state,
    const IOperator& op);
void AddPlanWidgets(optimizer_trace::Trace::Tile& tile, const TOpRoot& root, const TTraceBuildState& state);
void DefineHtmlTraceFields(optimizer_trace::Trace& trace);

} // namespace NKqp
} // namespace NKikimr
