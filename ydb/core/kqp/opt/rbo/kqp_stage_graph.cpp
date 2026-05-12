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

TString FormatSortElements(const TVector<TSortElement>& sortElements) {
    TStringBuilder result;
    for (size_t i = 0; i < sortElements.size(); ++i) {
        if (i != 0) {
            result << ", ";
        }

        result << sortElements[i].ToString();
    }
    return result;
}

NJson::TJsonValue MakeKeyColumnsJson(const TVector<TInfoUnit>& keys) {
    NJson::TJsonValue keyColumns(NJson::EJsonValueType::JSON_ARRAY);
    for (const auto& key : keys) {
        keyColumns.AppendValue(key.GetFullName());
    }
    return keyColumns;
}
}

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NNodes;

TString TSortElement::ToString() const {
    TStringBuilder result;
    result << SortColumn.GetFullName()
        << (Ascending ? " asc " : " desc ")
        << (NullsFirst ? "nulls first" : "nulls last");
    return result;
}

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

NJson::TJsonValue TConnection::ToJson() const {
    NJson::TJsonValue json(NJson::EJsonValueType::JSON_MAP);
    json["PlanNodeType"] = "Connection";
    json["Node Type"] = GetExplainName();
    return json;
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

NJson::TJsonValue TShuffleConnection::ToJson() const {
    auto json = TConnection::ToJson();
    const auto keyColumns = MakeKeyColumnsJson(Keys);
    json["KeyColumns"] = keyColumns;
    json["Node Type"] = TStringBuilder()
        << GetExplainName()
        << " (KeyColumns: " << keyColumns << ")";
    return json;
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

NJson::TJsonValue TMergeConnection::ToJson() const {
    auto json = TConnection::ToJson();
    const auto sortBy = FormatSortElements(Order);
    json["SortBy"] = sortBy;
    json["Node Type"] = TStringBuilder()
        << GetExplainName()
        << " (SortBy: " << sortBy << ")";
    return json;
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

TIntrusivePtr<TConnection> TStageGraph::TryGetConnection(ui32 from, ui32 to, ui32 occurrence) const {
    const auto connectionsIt = Connections.find(std::make_pair(from, to));
    if (connectionsIt == Connections.end() || occurrence >= connectionsIt->second.size()) {
        return {};
    }

    return connectionsIt->second[occurrence];
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