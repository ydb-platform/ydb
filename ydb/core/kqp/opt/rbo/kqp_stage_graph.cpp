#include "kqp_stage_graph.h"

#include <yql/essentials/utils/log/log.h>

#include <util/generic/guid.h>

namespace NKikimr::NKqp {

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

TString FormatSortElements(const TSortIUs& sortElements, const TInfoUnitRegistry& registry) {
    TStringBuilder result;
    TStringBuf separator;
    for (const auto& [id, order] : sortElements.Items()) {
        result << separator << registry.GetDisplayName(id) << (order.Ascending ? " asc " : " desc ")
            << (order.NullsFirst ? "nulls first" : "nulls last");
        separator = ", ";
    }
    return result;
}

NJson::TJsonValue MakeKeyColumnsJson(const TOrderedIUs<>& keys, const TInfoUnitRegistry& registry) {
    NJson::TJsonValue keyColumns(NJson::EJsonValueType::JSON_ARRAY);
    for (const auto id : keys.Items()) {
        keyColumns.AppendValue(TString(TStringBuilder() << registry.GetDisplayName(id)));
    }
    return keyColumns;
}

NJson::TJsonValue MakeSortColumnsJson(const TSortIUs& sortElements, const TInfoUnitRegistry& registry) {
    NJson::TJsonValue sortColumns(NJson::EJsonValueType::JSON_ARRAY);
    for (const auto& [id, order] : sortElements.Items()) {
        TStringBuilder sortColumn;
        sortColumn << registry.GetDisplayName(id) << " (" << (order.Ascending ? "Asc" : "Desc") << ")";
        sortColumns.AppendValue(TString(sortColumn));
    }
    return sortColumns;
}

} // anonymous namespace

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

NJson::TJsonValue TConnection::ToJson(const TInfoUnitRegistry&) const {
    NJson::TJsonValue json(NJson::EJsonValueType::JSON_MAP);
    json["PlanNodeType"] = "Connection";
    json["Node Type"] = Type;
    return json;
}

TExprNode::TPtr TBroadcastConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx, const TPhysicalNames&) {
    return BuildConnectionImpl<TDqCnBroadcast>(inputStage, pos, ctx);
}

TExprNode::TPtr TMapConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx, const TPhysicalNames&) {
    return BuildConnectionImpl<TDqCnMap>(inputStage, pos, ctx);
}

TExprNode::TPtr TUnionAllConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx, const TPhysicalNames&) {
    return Parallel ? BuildConnectionImpl<TDqCnParallelUnionAll>(inputStage, pos, ctx) : BuildConnectionImpl<TDqCnUnionAll>(inputStage, pos, ctx);
}

NJson::TJsonValue TUnionAllConnection::ToJson(const TInfoUnitRegistry& registry) const {
    auto json = TConnection::ToJson(registry);
    if (Parallel) {
        json["Parallel"] = "True";
    }
    return json;
}

TExprNode::TPtr TShuffleConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx, const TPhysicalNames& names) {
    Y_ENSURE(HashFuncType, "Hash function type must be assigned before building a shuffle connection.");

    TVector<TCoAtom> keyColumns;
    for (const auto id : Keys.Items()) {
        keyColumns.emplace_back(Build<TCoAtom>(ctx, pos).Value(names.Get(id)).Done());
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
        .UseSpilling().Build(UseSpilling)
        .HashFunc().Build(ToString(*HashFuncType))
    .Done().Ptr();
    // clang-format on
}

NJson::TJsonValue TShuffleConnection::ToJson(const TInfoUnitRegistry& registry) const {
    auto json = TConnection::ToJson(registry);
    Y_ENSURE(HashFuncType, "Hash function type must be assigned before building explain JSON.");

    const auto hashFunc = ToString(*HashFuncType);
    json["HashFunc"] = hashFunc;

    const auto keyColumns = MakeKeyColumnsJson(Keys, registry);
    json["KeyColumns"] = keyColumns;
    return json;
}

TExprNode::TPtr TMergeConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx, const TPhysicalNames& names) {
    TVector<TExprNode::TPtr> sortColumns;
    for (const auto& [id, order] : Order.Items()) {
        // clang-format off
        sortColumns.push_back(Build<TDqSortColumn>(ctx, pos)
            .Column<TCoAtom>().Build(names.Get(id))
            .SortDirection().Build(order.Ascending ? TTopSortSettings::AscendingSort : TTopSortSettings::DescendingSort)
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

NJson::TJsonValue TMergeConnection::ToJson(const TInfoUnitRegistry& registry) const {
    auto json = TConnection::ToJson(registry);
    const auto sortBy = FormatSortElements(Order, registry);
    json["SortBy"] = sortBy;
    json["SortColumns"] = MakeSortColumnsJson(Order, registry);
    return json;
}

TExprNode::TPtr TSourceConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx, const TPhysicalNames&) {
    Y_UNUSED(pos);
    Y_UNUSED(ctx);
    return inputStage;
}

TExprNode::TPtr TStreamLookupConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx, const TPhysicalNames&) {
    Y_ENSURE(InputType, "Stream lookup input type has not been set");
    // clang-format off
    return Build<TKqpCnStreamLookup>(ctx, pos)
        .Output()
            .Stage(inputStage)
            .Index().Build(ToString(OutputIndex))
        .Build()
        .Table(Table)
        .Columns(Columns)
        .InputType(InputType)
        .Settings(Settings)
    .Done().Ptr();
    // clang-format on
}

ui32 TStageGraph::AddStage() {
    ui32 newStageId = StageCounter++;
    StageIds.push_back(newStageId);
    StageInputs[newStageId] = TVector<ui32>();
    StageOutputs[newStageId] = TVector<ui32>();
    StageGUIDs[newStageId] = CreateGuidAsString();
    return newStageId;
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

TList<ui32> TStageGraph::GetTopologicalOrder() const {
    TList<ui32> sortedStages;
    THashSet<ui32> visited;

    for (auto id : StageIds) {
        if (!visited.contains(id)) {
            DFS(id, sortedStages, visited, StageInputs);
        }
    }

    return sortedStages;
}

void TStageGraph::TopologicalSort() {
    StageIds = GetTopologicalOrder();
}

} // namespace NKikimr::NKqp
