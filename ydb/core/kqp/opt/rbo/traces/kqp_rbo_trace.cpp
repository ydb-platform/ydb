#include "kqp_rbo_trace.h"

#include <yql/essentials/ast/yql_type_string.h>

#include <algorithm>
#include <optional>
#include <sstream>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr {
namespace NKqp {

TString FormatSortElements(const TSortIUs& columns, const TInfoUnitRegistry& registry) {
    TStringBuilder result;
    TStringBuf separator;
    for (const auto& [id, order] : columns.Items()) {
        result << separator << FormatInfoUnit(id, registry) << (order.Ascending ? " asc " : " desc ")
               << (order.NullsFirst ? "nulls first" : "nulls last");
        separator = ", ";
    }
    return result;
}

std::string FormatTypeForInfoUnit(const IOperator& op, TInfoUnitId unit) {
    if (!op.Type || op.Type->GetKind() != ETypeAnnotationKind::List) {
        return "type unknown";
    }

    const auto* itemType = op.Type->Cast<TListExprType>()->GetItemType();
    if (!itemType || itemType->GetKind() != ETypeAnnotationKind::Struct) {
        return "type unknown";
    }

    const auto* columnType = itemType->Cast<TStructExprType>()->FindItemType(::ToString(unit));
    return columnType ? ToStdString(FormatType(columnType)) : "type unknown";
}

std::string FormatOutputType(const IOperator& op) {
    if (!op.Type) {
        return {};
    }

    return ToStdString(FormatType(op.Type));
}

template <class TRange>
std::vector<std::pair<std::string, std::string>> MakeTypedInfoUnitItems(const IOperator& op, const TRange& units, const TInfoUnitRegistry& registry) {
    std::vector<std::pair<std::string, std::string>> items;
    for (const auto& unit : units) {
        items.emplace_back(ToStdString(FormatInfoUnit(unit, registry)), FormatTypeForInfoUnit(op, unit));
    }
    return items;
}

template <class TRange>
optimizer_trace::Field& AddInfoUnitField(
    optimizer_trace::Node& node,
    const std::string& key,
    const std::string& title,
    const TRange& units,
    const TInfoUnitRegistry& registry,
    const IOperator* typedBy = nullptr)
{
    auto items = MakeInfoUnitItems(units, registry);
    auto& field = node.field(key, FormatCountedSummary(items));
    if (typedBy) {
        field.detail(optimizer_trace::Widget::list(title, MakeTypedInfoUnitItems(*typedBy, units, registry)).monospaceList());
    } else {
        field.detail(optimizer_trace::Widget::list(title, items).monospaceListText());
    }
    return field;
}

optimizer_trace::Field& AddStringListField(
    optimizer_trace::Node& node,
    const std::string& key,
    const std::string& title,
    const std::vector<std::string>& items)
{
    auto& field = node.field(key, FormatCountedSummary(items));
    field.detail(optimizer_trace::Widget::list(title, items).monospaceListText());
    return field;
}

std::string FormatStatisticsType(EStatisticsType type) {
    switch (type) {
        case NKikimr::NKqp::BaseTable: return "BaseTable";
        case NKikimr::NKqp::FilteredFactTable: return "FilteredFactTable";
        case NKikimr::NKqp::ManyManyJoin: return "ManyManyJoin";
        case NKikimr::NKqp::Constant: return "Constant";
        default: return "Unknown";
    }
}

std::string FormatStorageType(EStorageType storageType) {
    switch (storageType) {
        case NKikimr::NKqp::RowStorage: return "Row";
        case NKikimr::NKqp::ColumnStorage: return "Column";
        case NKikimr::NKqp::NA: return "N/A";
        default: return "Unknown";
    }
}

std::string FormatStorageType(NYql::EStorageType storageType) {
    switch (storageType) {
        case NYql::RowStorage: return "Row";
        case NYql::ColumnStorage: return "Column";
        case NYql::NA: return "N/A";
        default: return "Unknown";
    }
}

std::string FormatLogicalCardinality(ELogicalCardinality cardinality) {
    switch (cardinality) {
        case ZeroOrMore: return "ZeroOrMore";
        case Zero: return "Zero";
        case ZeroOrOne: return "ZeroOrOne";
        case One: return "One";
        case OneOrMore: return "OneOrMore";
    }
    return "Unknown";
}

std::string FormatJoinAlgo(EJoinAlgoType algo) {
    switch (algo) {
        case EJoinAlgoType::Undefined: return "Undefined";
        case EJoinAlgoType::LookupJoin: return "LookupJoin";
        case EJoinAlgoType::LookupJoinReverse: return "LookupJoinReverse";
        case EJoinAlgoType::MapJoin: return "MapJoin";
        case EJoinAlgoType::GraceJoin: return "GraceJoin";
        case EJoinAlgoType::ReverseBlockJoin: return "ReverseBlockJoin";
        case EJoinAlgoType::StreamLookupJoin: return "StreamLookupJoin";
        case EJoinAlgoType::MergeJoin: return "MergeJoin";
    }
    return "Unknown";
}

std::string FormatOrderEnforcerAction(EOrderEnforcerAction action) {
    switch (action) {
        case REQUIRE: return "REQUIRE";
        case MAINTAIN: return "MAINTAIN";
    }
    return "Unknown";
}

std::string FormatOrderEnforcerReason(EOrderEnforcerReason reason) {
    switch (reason) {
        case USER: return "USER";
        case INTERNAL: return "INTERNAL";
    }
    return "Unknown";
}

std::vector<std::pair<std::string, std::string>> BuildOrderEnforcerRows(const TOrderEnforcer& enforcer, const TInfoUnitRegistry& registry) {
    std::vector<std::pair<std::string, std::string>> rows = {
        {"Action", FormatOrderEnforcerAction(enforcer.Action)},
        {"Reason", FormatOrderEnforcerReason(enforcer.Reason)}
    };
    if (!enforcer.SortElements.Items().empty()) {
        rows.emplace_back("Sort", ToStdString(FormatSortElements(enforcer.SortElements, registry)));
    }
    return rows;
}

optimizer_trace::Widget BuildColumnValueTable(
    const std::string& title,
    const std::vector<std::pair<std::string, std::string>>& rows)
{
    return optimizer_trace::Widget::table(title, rows).monospaceValues();
}

optimizer_trace::Widget BuildColumnTable(
    const std::string& title,
    const std::vector<std::pair<std::string, std::string>>& rows)
{
    return optimizer_trace::Widget::table(title, rows).monospaceTable();
}

std::string FormatOrderEnforcer(const TOrderEnforcer& enforcer, const TInfoUnitRegistry& registry) {
    std::vector<std::string> parts = {
        FormatOrderEnforcerAction(enforcer.Action),
        FormatOrderEnforcerReason(enforcer.Reason)
    };
    if (!enforcer.SortElements.Items().empty()) {
        parts.push_back(ToStdString(FormatSortElements(enforcer.SortElements, registry)));
    }
    return JoinStrings(parts);
}

void AddShuffleDecisionField(
    optimizer_trace::Node& node,
    const std::string& key,
    const std::string& title,
    const std::optional<TOrderedIUs<>>& shuffleBy,
    const TInfoUnitRegistry& registry,
    const IOperator* typedBy)
{
    if (!shuffleBy) {
        return;
    }

    if (shuffleBy->Items().empty()) {
        node.field(key, "eliminated")
            .detail(optimizer_trace::Widget::warning(title, "Shuffle is eliminated for this input.", "info"));
        return;
    }

    AddInfoUnitField(node, key, title, shuffleBy->Items(), registry, typedBy);
}

std::string FormatSubplanType(ESubplanType type) {
    switch (type) {
        case EXPR: return "EXPR";
        case IN_SUBPLAN: return "IN_SUBPLAN";
        case EXISTS: return "EXISTS";
    }
    return "Unknown";
}

std::string FormatLineageSource(const TColumnLineageEntry& entry) {
    return ToStdString(TStringBuilder()
        << "/" << entry.GetRawAlias()
        << "#" << entry.Relation
        << "/" << entry.ColumnName);
}

std::vector<std::pair<std::string, std::string>> BuildLineageRows(
    const TColumnLineage& lineage,
    const TUnorderedIUs& outputIUs,
    const TInfoUnitRegistry& registry)
{
    std::vector<std::pair<std::string, std::string>> rows;
    for (const auto& unit : outputIUs) {
        if (const auto* entry = lineage.Find(unit)) {
            rows.emplace_back(ToStdString(FormatInfoUnit(unit, registry)), FormatLineageSource(*entry));
        }
    }
    return rows;
}

std::string FormatPairSummary(const std::vector<std::pair<std::string, std::string>>& rows, size_t maxItems = 4) {
    std::vector<std::string> items;
    items.reserve(rows.size());
    const size_t limit = std::min(maxItems, rows.size());
    for (size_t i = 0; i < limit; ++i) {
        items.push_back(rows[i].first + ": " + rows[i].second);
    }
    if (rows.size() > limit) {
        items.push_back("...");
    }
    if (rows.empty()) {
        return "(0)";
    }
    return "(" + std::to_string(rows.size()) + ") " + JoinStrings(items);
}

std::vector<std::pair<std::string, std::string>> BuildStageRows(const TStageGraph& graph, ui32 stageId) {
    std::vector<std::pair<std::string, std::string>> rows;
    rows.emplace_back("Stage", std::to_string(stageId));

    const auto guidIt = graph.StageGUIDs.find(stageId);
    if (guidIt != graph.StageGUIDs.end() && !guidIt->second.empty()) {
        rows.emplace_back("Guid", ToStdString(guidIt->second));
    }

    const auto sourceIt = graph.SourceStages.find(stageId);
    if (sourceIt != graph.SourceStages.end()) {
        rows.emplace_back("Source storage", FormatStorageType(sourceIt->second.StorageType));
    }

    const auto inputIt = graph.StageInputs.find(stageId);
    if (inputIt != graph.StageInputs.end() && !inputIt->second.empty()) {
        std::vector<std::string> inputs;
        inputs.reserve(inputIt->second.size());
        for (const auto input : inputIt->second) {
            inputs.push_back(std::to_string(input));
        }
        rows.emplace_back("Inputs", JoinStrings(inputs));
    }

    const auto outputIt = graph.StageOutputs.find(stageId);
    if (outputIt != graph.StageOutputs.end() && !outputIt->second.empty()) {
        std::vector<std::string> outputs;
        outputs.reserve(outputIt->second.size());
        for (const auto output : outputIt->second) {
            outputs.push_back(std::to_string(output));
        }
        rows.emplace_back("Outputs", JoinStrings(outputs));
    }

    return rows;
}

struct TStageEdge {
    ui32 From = 0;
    ui32 To = 0;
    ui32 Index = 0;
    TIntrusivePtr<TConnection> Connection;
};

std::vector<TStageEdge> CollectStageEdges(const TStageGraph& graph) {
    std::vector<TStageEdge> edges;
    for (const auto& [key, connections] : graph.Connections) {
        for (ui32 index = 0; index < connections.size(); ++index) {
            edges.push_back({key.first, key.second, index, connections[index]});
        }
    }
    std::sort(edges.begin(), edges.end(), [](const TStageEdge& lhs, const TStageEdge& rhs) {
        return std::tie(lhs.From, lhs.To, lhs.Index) < std::tie(rhs.From, rhs.To, rhs.Index);
    });
    return edges;
}

std::string FormatConnectionLabel(const TConnection& connection) {
    return ToStdString(connection.Type) + " connection";
}

std::string FormatConnectionDetails(const TConnection& connection, const TInfoUnitRegistry& registry) {
    std::vector<std::string> details = {
        "type=" + ToStdString(connection.Type),
        "outputIndex=" + std::to_string(connection.GetOutputIndex())
    };

    if (const auto* shuffle = dynamic_cast<const TShuffleConnection*>(&connection)) {
        if (!shuffle->Keys.Items().empty()) {
            details.push_back("hashKeys=" + ToStdString(FormatInfoUnits(shuffle->Keys.Items(), registry)));
        }
        if (shuffle->HashFuncType) {
            details.push_back("hashFunc=" + ToStdString(ToString(*shuffle->HashFuncType)));
        }
        details.push_back("useSpilling=" + FormatBool(shuffle->UseSpilling));
    } else if (const auto* merge = dynamic_cast<const TMergeConnection*>(&connection)) {
        if (!merge->Order.Items().empty()) {
            details.push_back("mergeOrder=" + ToStdString(FormatSortElements(merge->Order, registry)));
        }
    }

    return JoinStrings(details);
}

optimizer_trace::Widget BuildStageGraphDetailsWidget(const TStageGraph& graph, const TInfoUnitRegistry& registry) {
    std::vector<std::pair<std::string, std::string>> rows;

    for (const auto stageId : graph.StageIds) {
        std::vector<std::string> details;
        for (const auto& [key, value] : BuildStageRows(graph, stageId)) {
            if (key == "Stage") {
                continue;
            }
            details.push_back(key + "=" + value);
        }
        rows.emplace_back("Stage " + std::to_string(stageId), details.empty() ? "" : JoinStrings(details));
    }

    for (const auto& edge : CollectStageEdges(graph)) {
        rows.emplace_back(
            "Connection " + std::to_string(edge.From) + " -> " + std::to_string(edge.To) + " #" + std::to_string(edge.Index),
            FormatConnectionDetails(*edge.Connection, registry));
    }

    return BuildColumnValueTable("Stage graph details", rows);
}

void AttachStageTarget(TTraceBuildState* state, const IOperator& op, const std::string& nodeId) {
    if (!state || !op.Props.StageId) {
        return;
    }
    state->StageTargets[*op.Props.StageId].push_back(optimizer_trace::Target::node(nodeId));
}

void AttachOperatorTarget(TTraceBuildState* state, const IOperator& op, const std::string& nodeId) {
    if (!state) {
        return;
    }
    state->OperatorTargets[&op].push_back(optimizer_trace::Target::subtree(nodeId));
}

std::string EnsureOverviewNode(TTraceBuildState* state, const IOperator& op) {
    if (!state) {
        return {};
    }

    if (const auto it = state->OverviewNodeIds.find(&op); it != state->OverviewNodeIds.end()) {
        return it->second;
    }

    const std::string overviewId = "op-" + std::to_string(state->NextOverviewNodeId++);
    state->OverviewNodeIds[&op] = overviewId;
    state->OverviewNodes.push_back({
        &op,
        overviewId,
        ToStdString(op.GetExplainName())
    });
    return overviewId;
}

void AttachOverviewEdge(TTraceBuildState* state, const IOperator& parent, const IOperator& child) {
    if (!state) {
        return;
    }

    const std::string from = EnsureOverviewNode(state, parent);
    const std::string to = EnsureOverviewNode(state, child);
    if (from.empty() || to.empty()) {
        return;
    }

    const std::string edgeKey = from + "->" + to;
    if (state->OverviewEdgeIds.contains(edgeKey)) {
        return;
    }

    state->OverviewEdgeIds[edgeKey] = true;
    state->OverviewEdges.push_back({
        "edge-" + std::to_string(state->NextOverviewEdgeId++),
        from,
        to,
        &child
    });
}

std::vector<optimizer_trace::Target> GetOperatorTargets(
    const TTraceBuildState& state,
    const IOperator& op)
{
    const auto it = state.OperatorTargets.find(&op);
    if (it == state.OperatorTargets.end()) {
        return {};
    }
    return it->second;
}

optimizer_trace::Widget BuildPlanOverviewWidget(const TTraceBuildState& state) {
    optimizer_trace::Graph overview;
    overview.layout("TB", 48, 30);

    for (const auto& node : state.OverviewNodes) {
        auto& graphNode = overview.node(node.Id, node.Label);
        if (node.Op) {
            std::vector<optimizer_trace::Target> targets;
            for (const auto& target : GetOperatorTargets(state, *node.Op)) {
                targets.push_back(optimizer_trace::Target::node(target.nodeId()));
            }
            if (!targets.empty()) {
                graphNode.targets(targets).primaryTarget(targets.front());
            }
        }
    }

    for (const auto& edge : state.OverviewEdges) {
        auto& graphEdge = overview.edge(edge.From, edge.To)
            .setId(edge.Id);
        if (edge.Child) {
            const auto targets = GetOperatorTargets(state, *edge.Child);
            if (!targets.empty()) {
                graphEdge.targets(targets).primaryTarget(targets.front());
            }
        }
    }

    return optimizer_trace::Widget::graph("Plan overview", overview);
}

std::string FormatReadNameForJoinOrder(const TOpRead& read) {
    if (read.TableCallable) {
        const auto path = NYql::NNodes::TKqpTable(read.TableCallable).Path().StringValue();
        const auto slash = path.rfind('/');
        return ToStdString((slash == TString::npos) ? path : path.substr(slash + 1));
    }

    if (!read.Alias.empty()) {
        return ToStdString(read.Alias);
    }

    return "TableFullScan";
}

struct TJoinOrderJson {
    NJson::TJsonValue Json;
    bool HasJoin = false;
};

TJoinOrderJson BuildJoinOrderJson(const IOperator* op) {
    if (!op) {
        return {NJson::TJsonValue("null"), false};
    }

    if (op->Kind == EOperator::Source) {
        return {NJson::TJsonValue(FormatReadNameForJoinOrder(*static_cast<const TOpRead*>(op))), false};
    }

    if (op->Kind == EOperator::CBOTree) {
        const auto* cboTree = static_cast<const TOpCBOTree*>(op);
        return BuildJoinOrderJson(cboTree->TreeRoot.get());
    }

    if (op->Kind == EOperator::Join) {
        NJson::TJsonValue children(NJson::EJsonValueType::JSON_ARRAY);
        for (const auto& child : op->GetChildren()) {
            children.AppendValue(BuildJoinOrderJson(child).Json);
        }
        return {std::move(children), true};
    }

    if (op->GetChildren().size() == 1) {
        return BuildJoinOrderJson(op->GetChildren().front());
    }

    bool hasJoin = false;
    NJson::TJsonValue children(NJson::EJsonValueType::JSON_ARRAY);
    for (const auto& child : op->GetChildren()) {
        auto childOrder = BuildJoinOrderJson(child);
        hasJoin = hasJoin || childOrder.HasJoin;
        children.AppendValue(std::move(childOrder.Json));
    }

    if (hasJoin) {
        NJson::TJsonValue wrapper(NJson::EJsonValueType::JSON_MAP);
        wrapper[op->GetExplainName()] = std::move(children);
        return {std::move(wrapper), true};
    }

    return {NJson::TJsonValue(ToStdString(op->GetExplainName())), false};
}

std::optional<optimizer_trace::Widget> BuildJoinOrderWidget(const TOpRoot& root) {
    if (root.GetChildren().empty()) {
        return std::nullopt;
    }

    auto joinOrder = BuildJoinOrderJson(root.GetChildren().front());
    if (!joinOrder.HasJoin) {
        return std::nullopt;
    }

    return optimizer_trace::Widget::unwrappedText(
        "Join order",
        ToStdString(NJson::WriteJson(joinOrder.Json, true, false, true)),
        false);
}

optimizer_trace::Widget BuildStageGraphWidget(const TStageGraph& graph, const TTraceBuildState& state) {
    optimizer_trace::Graph stageGraph;
    stageGraph.layout("LR", 70, 42);

    for (const auto stageId : graph.StageIds) {
        auto& graphNode = stageGraph.node(std::to_string(stageId), "Stage " + std::to_string(stageId));
        const auto rows = BuildStageRows(graph, stageId);
        std::vector<std::string> notes;
        notes.reserve(rows.size());
        for (const auto& [key, value] : rows) {
            notes.push_back(key + ": " + value);
        }
        graphNode.note(JoinStrings(notes, "\n"));

        const auto targetIt = state.StageTargets.find(stageId);
        if (targetIt != state.StageTargets.end() && !targetIt->second.empty()) {
            graphNode.targets(targetIt->second).primaryTarget(targetIt->second.front());
        }
    }

    for (const auto& edge : CollectStageEdges(graph)) {
        auto& graphEdge = stageGraph.edge(std::to_string(edge.From), std::to_string(edge.To))
            .setId(std::to_string(edge.From) + "-" + std::to_string(edge.To) + "-" + std::to_string(edge.Index))
            .setLabel(FormatConnectionLabel(*edge.Connection));

        std::vector<optimizer_trace::Target> targets;
        const auto fromIt = state.StageTargets.find(edge.From);
        if (fromIt != state.StageTargets.end()) {
            targets.insert(targets.end(), fromIt->second.begin(), fromIt->second.end());
        }
        const auto toIt = state.StageTargets.find(edge.To);
        if (toIt != state.StageTargets.end()) {
            targets.insert(targets.end(), toIt->second.begin(), toIt->second.end());
        }
        if (!targets.empty()) {
            graphEdge.targets(targets).primaryTarget(targets.front());
        }
    }

    return optimizer_trace::Widget::graph("Stage graph", stageGraph).monospaceGraphEdges();
}

optimizer_trace::Widget BuildStageGraphSwitcher(const TStageGraph& graph, const TTraceBuildState& state, const TInfoUnitRegistry& registry) {
    return optimizer_trace::Widget::switcher("Stage graph")
        .defaultOption("graph")
        .option("graph", "Graph", {BuildStageGraphWidget(graph, state)})
        .option("details", "Details", {BuildStageGraphDetailsWidget(graph, registry)});
}

std::vector<optimizer_trace::Widget> BuildPlanWidgets(const TOpRoot& root, const TTraceBuildState& state) {
    std::vector<optimizer_trace::Widget> widgets;
    if (root.PlanProps.StageGraph.StageIds.empty()) {
        return widgets;
    }

    widgets.push_back(BuildStageGraphSwitcher(root.PlanProps.StageGraph, state, root.PlanProps.InfoUnitRegistry));
    return widgets;
}

void AddPlanWidgets(optimizer_trace::Trace::Tile& tile, const TOpRoot& root, const TTraceBuildState& state) {
    if (!state.OverviewNodes.empty()) {
        auto& overview = tile.info().tab("overview", "Overview")
            .widget(BuildPlanOverviewWidget(state));
        if (auto joinOrder = BuildJoinOrderWidget(root)) {
            overview.widget(*joinOrder);
        }
    }

    auto widgets = BuildPlanWidgets(root, state);
    if (widgets.empty()) {
        return;
    }

    auto& tab = tile.info().tab("Plan", "Plan");
    for (const auto& widget : widgets) {
        tab.widget(widget);
    }
}

void AttachPlanOverview(TTraceBuildState* state, IOperator* op) {
    if (!state) {
        return;
    }

    EnsureOverviewNode(state, *op);
    for (const auto& child : op->GetChildren()) {
        AttachOverviewEdge(state, *op, *child);
    }
}

optimizer_trace::Node BuildPlanNode(
    IOperator* op,
    TExprContext& ctx,
    TPlanProps& planProps,
    ui32 opts,
    const std::string& id,
    TTraceBuildState* state)
{
    const auto& registry = planProps.InfoUnitRegistry;
    optimizer_trace::Node node(id, ToStdString(op->GetExplainName()), ToStdString(op->ToString(ctx, registry)));
    AttachStageTarget(state, *op, id);
    AttachOperatorTarget(state, *op, id);
    AttachPlanOverview(state, op);

    const auto& outputIUs = op->GetOutputIUs();
    AddInfoUnitField(node, "OutputColumns", "Output columns", outputIUs, registry, op);
    if (const auto outputType = FormatOutputType(*op); !outputType.empty()) {
        node.field("OutputType", outputType);
    }

    if (op->Props.StageId.has_value()) {
        auto& stageField = node.field("Stage", std::to_string(*op->Props.StageId));
        stageField.detail(optimizer_trace::Widget::table("Stage", BuildStageRows(planProps.StageGraph, *op->Props.StageId)));
    }

    if (op->Props.Algorithm) {
        node.field("Algorithm", ToStdString(*op->Props.Algorithm));
    }

    if (op->Props.JoinAlgo) {
        node.field("JoinAlgo", FormatJoinAlgo(*op->Props.JoinAlgo));
    }

    AddShuffleDecisionField(node, "LeftShuffleBy", "Left shuffle by", op->Props.LeftShuffleBy, registry, op);
    AddShuffleDecisionField(node, "RightShuffleBy", "Right shuffle by", op->Props.RightShuffleBy, registry, op);

    if (op->Props.OrderEnforcer) {
        node.field("OrderEnforcer", FormatOrderEnforcer(*op->Props.OrderEnforcer, registry))
            .detail(BuildColumnValueTable("Order enforcer", BuildOrderEnforcerRows(*op->Props.OrderEnforcer, registry)));
    }

    if ((opts & (EPrintPlanOptions::PrintBasicMetadata | EPrintPlanOptions::PrintFullMetadata))
        && op->Props.Metadata.has_value()) {
        const auto& meta = *op->Props.Metadata;

        if (meta.StorageType != EStorageType::NA) {
            node.field("Storage", FormatStorageType(meta.StorageType));
        }

        if (!meta.KeyColumns.Items().empty()) {
            AddInfoUnitField(node, "KeyColumns", "Key columns", meta.KeyColumns.Items(), registry, op);
        }

        if (!meta.ShuffledByColumns.Items().empty()) {
            AddInfoUnitField(node, "ShuffledBy", "Shuffled by", meta.ShuffledByColumns.Items(), registry, op);
        }

        node.field("Type", FormatStatisticsType(meta.Type));
        node.field("LogicalCard", FormatLogicalCardinality(meta.LogicalCard));

        if (const auto rows = BuildLineageRows(planProps.ColumnLineage, outputIUs, registry); !rows.empty()) {
            node.field("Lineage", FormatPairSummary(rows))
                .detail(BuildColumnTable("Lineage", rows));
        }
    }

    if (op->Props.Analysis.LiveOut) {
        AddInfoUnitField(node, "LiveOut", "Live out", *op->Props.Analysis.LiveOut, registry);
    }

    const auto usedIUs = op->GetUsedIUs(planProps);
    if (!usedIUs.Empty()) {
        AddInfoUnitField(node, "UsedIUs", "Used IUs", usedIUs, registry, op);
    }

    if ((opts & (EPrintPlanOptions::PrintBasicStatistics | EPrintPlanOptions::PrintFullStatistics))
        && op->Props.Statistics.has_value()) {
        const auto& stats = *op->Props.Statistics;
        std::ostringstream rowsStr, bytesStr, selectivityStr;
        rowsStr << stats.ERows;
        bytesStr << stats.EBytes;
        selectivityStr << stats.Selectivity;
        node.field("ERows", rowsStr.str());
        node.field("EBytes", bytesStr.str());
        node.field("Selectivity", selectivityStr.str());
    }

    if (op->Props.Cost.has_value()) {
        std::ostringstream costStr;
        costStr << *op->Props.Cost;
        node.field("Cost", costStr.str());
    }

    for (size_t i = 0; i < op->GetChildren().size(); ++i) {
        node.child(BuildPlanNode(op->GetChildren()[i], ctx, planProps, opts, id + "-" + std::to_string(i), state));
    }
    return node;
}

optimizer_trace::Node BuildPlanNodeFromRoot(TOpRoot& root, TExprContext& ctx, ui32 opts, TTraceBuildState* state) {
    const auto& registry = root.PlanProps.InfoUnitRegistry;
    const auto& subplans = root.PlanProps.Subplans;
    if (subplans.Empty()) {
        return BuildPlanNode(root.GetInput().Get(), ctx, root.PlanProps, opts, "n-0", state);
    }
    optimizer_trace::Node container("n", "Plan", "Plan");
    size_t index = 0;
    for (const auto& [iu, subplan] : subplans) {
        const std::string subplanId = "n-" + std::to_string(index++);
        optimizer_trace::Node sub(subplanId, "Subplan", "Subplan [" + ToStdString(FormatInfoUnit(iu, registry)) + "]");
        sub.field("SubplanType", FormatSubplanType(subplan.Type));
        if (!subplan.Tuple.Items().empty()) {
            AddInfoUnitField(sub, "SubplanTuple", "Subplan tuple", subplan.Tuple.Items(), registry);
        }
        if (!subplan.DependentIUs.Empty()) {
            AddInfoUnitField(sub, "SubplanDependentIUs", "Subplan dependent IUs", subplan.DependentIUs, registry);
        }
        sub.child(BuildPlanNode(subplan.Plan.Get(), ctx, root.PlanProps, opts, subplanId + "-0", state));
        container.child(sub);
    }
    container.child(BuildPlanNode(root.GetInput().Get(), ctx, root.PlanProps, opts, "n-" + std::to_string(index), state));
    return container;
}

void DefineHtmlTraceFields(optimizer_trace::Trace& trace) {
    trace.defineFields({
        {"OutputColumns", "Output columns"},
        {"OutputType", "Output type"},
        {"SubplanType", "Subplan type"},
        {"SubplanTuple", "Subplan tuple"},
        {"SubplanDependentIUs", "Subplan deps"},
        {"Stage", "Stage"},
        {"Algorithm", "Algorithm"},
        {"JoinAlgo", "Join algo"},
        {"LeftShuffleBy", "Left shuffle"},
        {"RightShuffleBy", "Right shuffle"},
        {"OrderEnforcer", "Order"},
        {"Storage", "Storage"},
        {"KeyColumns", "Key columns"},
        {"ShuffledBy", "Shuffled by"},
        {"Type", "Type"},
        {"LogicalCard", "Logical card"},
        {"Lineage", "Lineage"},
        {"LiveOut", "Live out"},
        {"UsedIUs", "Used IUs"},
        {"ERows", "Rows"},
        {"EBytes", "Bytes"},
        {"Selectivity", "Selectivity"},
        {"Cost", "Cost"}
    });
    trace.pinFields({"ERows", "EBytes", "Selectivity", "Cost"});
    trace.definePinnedFieldPresets({
        {"None", {}},
        {"Stages", {"Stage"}},
        {"Stats", {"ERows", "EBytes", "Selectivity", "Cost"}}
    });
    trace.defineDiffFieldPresets({
        {"None", {}},
        {"Stages", {"Stage"}}
    });
}

} // namespace NKqp
} // namespace NKikimr
