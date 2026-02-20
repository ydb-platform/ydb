#include "kqp_stage_graph.h"

namespace {
using namespace NKikimr;
using namespace NKqp;
using namespace NYql;
using namespace NNodes;

void DFS(ui32 vertex, TVector<ui32> &sortedStages, THashSet<ui32> &visited, const THashMap<ui32, TVector<ui32>> &stageInputs) {
    visited.emplace(vertex);

    for (auto u : stageInputs.at(vertex)) {
        if (!visited.contains(u)) {
            DFS(u, sortedStages, visited, stageInputs);
        }
    }

    sortedStages.push_back(vertex);
}

TExprNode::TPtr AddRenames(TExprNode::TPtr input, const TStageGraph::TSourceStageTraits& sourceStagesTraits, TExprContext& ctx) {
    if (sourceStagesTraits.Renames.empty()) {
        return input;
    }

    TVector<TExprBase> items;
    auto arg = Build<TCoArgument>(ctx, input->Pos()).Name("arg").Done().Ptr();

    for (const auto& rename : sourceStagesTraits.Renames) {
        // clang-format off
        auto tuple = Build<TCoNameValueTuple>(ctx, input->Pos())
            .Name().Build(rename.second.GetFullName())
            .Value<TCoMember>()
                .Struct(arg)
                .Name().Build(rename.first)
            .Build()
        .Done();
        // clang-format on
        items.push_back(tuple);
    }

    // clang-format off
    return Build<TCoMap>(ctx, input->Pos())
        .Input(input)
        .Lambda<TCoLambda>()
            .Args({arg})
            .Body<TCoAsStruct>()
                .Add(items)
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr BuildSourceStage(TExprNode::TPtr dqsource, TExprContext &ctx) {
    auto arg = Build<TCoArgument>(ctx, dqsource->Pos()).Name("arg").Done().Ptr();
    // clang-format off
    return Build<TDqPhyStage>(ctx, dqsource->Pos())
        .Inputs()
            .Add({dqsource})
        .Build()
        .Program()
            .Args({arg})
            .Body(arg)
        .Build()
        .Settings().Build()
    .Done().Ptr();
    // clang-format on
}

}

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NNodes;

template <typename DqConnectionType>
TExprNode::TPtr TConnection::BuildConnectionImpl(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr& newStage, TExprContext& ctx) {
    if (FromSourceStageStorageType == NYql::EStorageType::RowStorage) {
        inputStage = BuildSourceStage(inputStage, ctx);
        newStage = inputStage;
    }
    // clang-format off
    return Build<DqConnectionType>(ctx, pos)
        .Output()
            .Stage(inputStage)
            .Index().Build(ToString(OutputIndex))
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TBroadcastConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr& newStage, TExprContext& ctx) {
    return BuildConnectionImpl<TDqCnBroadcast>(inputStage, pos, newStage, ctx);
}

TExprNode::TPtr TMapConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr& newStage, TExprContext& ctx) {
    return BuildConnectionImpl<TDqCnMap>(inputStage, pos, newStage, ctx);
}

TExprNode::TPtr TUnionAllConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                                     TExprContext &ctx) {
    return BuildConnectionImpl<TDqCnUnionAll>(inputStage, pos, newStage, ctx);
}

TExprNode::TPtr TShuffleConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr& newStage,
                                                    TExprContext& ctx) {
    if (FromSourceStageStorageType == NYql::EStorageType::RowStorage) {
        inputStage = BuildSourceStage(inputStage, ctx);
        newStage = inputStage;
    }

    TVector<TCoAtom> keyColumns;
    for (const auto& k : Keys) {
        TString columnName;
        if (FromSourceStageStorageType != NYql::EStorageType::NA || k.GetAlias() == "") {
            columnName = k.GetColumnName();
        } else {
            columnName = k.GetFullName();
        }
        auto atom = Build<TCoAtom>(ctx, pos).Value(columnName).Done();
        keyColumns.push_back(atom);
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

TExprNode::TPtr TMergeConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr& newStage,
                                                  TExprContext& ctx) {
    if (FromSourceStageStorageType == NYql::EStorageType::RowStorage) {
        inputStage = BuildSourceStage(inputStage, ctx);
        newStage = inputStage;
    }

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

TExprNode::TPtr TSourceConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr &newStage,
                                                   TExprContext &ctx) {
    Y_UNUSED(pos);
    Y_UNUSED(newStage);
    Y_UNUSED(ctx);
    return inputStage;
}

std::pair<TExprNode::TPtr, TExprNode::TPtr> TStageGraph::GenerateStageInput(ui32& stageInputCounter, TExprNode::TPtr& node,
                                                                            TExprContext& ctx, ui32 fromStage) {
    TString inputName = "input_arg" + std::to_string(stageInputCounter++);
    YQL_CLOG(TRACE, CoreDq) << "Created stage argument " << inputName;
    auto arg = Build<TCoArgument>(ctx, node->Pos()).Name(inputName).Done().Ptr();
    auto output = arg;

    if (IsSourceStage(fromStage)) {
        output = AddRenames(arg, SourceStageRenames.at(fromStage), ctx);
    }

    return std::make_pair(arg, output);
}

void TStageGraph::TopologicalSort() {
    TVector<ui32> sortedStages;
    THashSet<ui32> visited;

    for (auto id : StageIds) {
        if (!visited.contains(id)) {
            DFS(id, sortedStages, visited, StageInputs);
        }
    }

    StageIds = sortedStages;
}

}
}