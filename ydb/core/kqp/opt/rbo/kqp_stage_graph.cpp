#include "kqp_stage_graph.h"

namespace {
using namespace NKikimr;
using namespace NKqp;
using namespace NYql;
using namespace NNodes;

void DFS(ui32 vertex, TList<ui32>& sortedStages, THashSet<ui32>& visited, const THashMap<ui32, TVector<ui32>>& stageInputs) {
    visited.emplace(vertex);
    for (auto u : stageInputs.at(vertex)) {
        if (!visited.contains(u)) {
            DFS(u, sortedStages, visited, stageInputs);
        }
    }
    sortedStages.push_back(vertex);
}
}

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NNodes;

template <typename DqConnectionType>
TExprNode::TPtr TConnection::BuildConnectionImpl(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) {
    // clang-format off
    return Build<DqConnectionType>(ctx, pos)
        .Output()
            .Stage(inputStage)
            .Index().Build(ToString(OutputIndex))
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TBroadcastConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) {
    return BuildConnectionImpl<TDqCnBroadcast>(inputStage, pos, ctx);
}

TExprNode::TPtr TMapConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) {
    return BuildConnectionImpl<TDqCnMap>(inputStage, pos, ctx);
}

TExprNode::TPtr TUnionAllConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) {
    return Parallel ? BuildConnectionImpl<TDqCnParallelUnionAll>(inputStage, pos, ctx) : BuildConnectionImpl<TDqCnUnionAll>(inputStage, pos, ctx);
}

TExprNode::TPtr TShuffleConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) {
    TVector<TCoAtom> keyColumns;
    for (const auto& key : Keys) {
        const auto columnName = key.GetFullName();
        keyColumns.emplace_back(Build<TCoAtom>(ctx, pos).Value(columnName).Done());
    }

    // clang-format off
    return Build<TDqCnHashShuffle>(ctx, pos)
        .Output()
            .Stage(inputStage)
            .Index().Build(ToString(OutputIndex))
        .Build()
        .KeyColumns()
            .Add(keyColumns)
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TMergeConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) {
    TVector<TExprNode::TPtr> sortColumns;
    for (const auto& sortElement : Order) {
        // clang-format off
        sortColumns.push_back(Build<TDqSortColumn>(ctx, pos)
            .Column<TCoAtom>().Build(sortElement.SortColumn.GetFullName())
            .SortDirection().Build(sortElement.Ascending ? TTopSortSettings::AscendingSort : TTopSortSettings::DescendingSort)
            .Done().Ptr());
        // clang-format on
    }

    // clang-format off
    return Build<TDqCnMerge>(ctx, pos)
        .Output()
            .Stage(inputStage)
            .Index().Build(ToString(OutputIndex))
        .Build()
        .SortColumns()
            .Add(sortColumns)
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TSourceConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) {
    Y_UNUSED(pos);
    Y_UNUSED(ctx);
    return inputStage;
}

std::pair<TExprNode::TPtr, TExprNode::TPtr> TStageGraph::GenerateStageInput(ui32& stageInputCounter, TPositionHandle pos, TExprContext& ctx) const {
    const TString inputName = "input_arg_" + std::to_string(stageInputCounter++);
    YQL_CLOG(TRACE, CoreDq) << "Created stage argument " << inputName;
    const auto arg = Build<TCoArgument>(ctx, pos).Name(inputName).Done().Ptr();
    return std::make_pair(arg, arg);
}

TIntrusivePtr<TConnection> TStageGraph::GetInputConnection(ui32 stageId, ui32 inputIndex) const {
    const auto& stageInputs = StageInputs.at(stageId);
    Y_ENSURE(inputIndex < stageInputs.size(), "Stage input index is out of range.");

    const auto inputStageId = stageInputs[inputIndex];
    const auto& connections = Connections.at(std::make_pair(inputStageId, stageId));
    ui32 connectionIndex = 0;
    for (ui32 i = 0; i < inputIndex; ++i) {
        if (stageInputs[i] == inputStageId) {
            ++connectionIndex;
        }
    }

    Y_ENSURE(connectionIndex < connections.size(), "Stage input connection is missing.");
    return connections[connectionIndex];
}

void TStageGraph::TopologicalSort() {
    TList<ui32> sortedStages;
    THashSet<ui32> visited;

    for (auto id : StageIds) {
        if (!visited.contains(id)) {
            DFS(id, sortedStages, visited, StageInputs);
        }
    }

    StageIds.swap(sortedStages);
}
}
}