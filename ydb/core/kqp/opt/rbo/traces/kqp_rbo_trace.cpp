#include "kqp_rbo_trace.h"

#include <yql/essentials/ast/yql_type_string.h>

#include <algorithm>
#include <optional>
#include <sstream>
#include <tuple>
#include <util/string/builder.h>

namespace NKikimr {
namespace NKqp {

TString FormatSortColumns(const TVector<std::pair<TInfoUnit, bool>>& sortColumns) {
    TStringBuilder result;
    for (size_t i = 0; i < sortColumns.size(); ++i) {
        if (i) {
            result << ", ";
        }
        result << FormatInfoUnit(sortColumns[i].first) << (sortColumns[i].second ? " ASC" : " DESC");
    }
    return result;
}

TString FormatSortElements(const TVector<TSortElement>& sortElements) {
    TStringBuilder result;
    for (size_t i = 0; i < sortElements.size(); ++i) {
        if (i) {
            result << ", ";
        }
        result << sortElements[i].ToString();
    }
    return result;
}

std::vector<std::string> MakeSortColumnItems(const TVector<std::pair<TInfoUnit, bool>>& sortColumns) {
    std::vector<std::string> items;
    items.reserve(sortColumns.size());
    for (const auto& [unit, ascending] : sortColumns) {
        items.push_back(ToStdString(TStringBuilder() << FormatInfoUnit(unit) << (ascending ? " ASC" : " DESC")));
    }
    return items;
}

std::vector<std::string> MakeSortElementItems(const TVector<TSortElement>& sortElements) {
    std::vector<std::string> items;
    items.reserve(sortElements.size());
    for (const auto& sortElement : sortElements) {
        items.push_back(ToStdString(sortElement.ToString()));
    }
    return items;
}

std::string FormatTypeForInfoUnit(const IOperator& op, const TInfoUnit& unit) {
    if (!op.Type || op.Type->GetKind() != ETypeAnnotationKind::List) {
        return "type unknown";
    }

    const auto* itemType = op.Type->Cast<TListExprType>()->GetItemType();
    if (!itemType || itemType->GetKind() != ETypeAnnotationKind::Struct) {
        return "type unknown";
    }

    const auto* columnType = itemType->Cast<TStructExprType>()->FindItemType(unit.GetFullName());
    return columnType ? ToStdString(FormatType(columnType)) : "type unknown";
}

std::string FormatOutputType(const IOperator& op) {
    if (!op.Type) {
        return {};
    }

    return ToStdString(FormatType(op.Type));
}

std::vector<std::pair<std::string, std::string>> MakeTypedInfoUnitItems(const IOperator& op, const TVector<TInfoUnit>& units) {
    std::vector<std::pair<std::string, std::string>> items;
    items.reserve(units.size());
    for (const auto& unit : units) {
        items.emplace_back(ToStdString(FormatInfoUnit(unit)), FormatTypeForInfoUnit(op, unit));
    }
    return items;
}

optimizer_trace::Field& AddInfoUnitField(
    optimizer_trace::Node& node,
    const std::string& key,
    const std::string& title,
    const TVector<TInfoUnit>& units,
    const IOperator* typedBy = nullptr)
{
    auto items = MakeInfoUnitItems(units);
    auto& field = node.field(key, FormatCountedSummary(items));
    if (typedBy) {
        field.detail(optimizer_trace::Widget::list(title, MakeTypedInfoUnitItems(*typedBy, units)).monospaceList());
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

std::vector<std::pair<std::string, std::string>> BuildOrderEnforcerRows(const TOrderEnforcer& enforcer) {
    std::vector<std::pair<std::string, std::string>> rows = {
        {"Action", FormatOrderEnforcerAction(enforcer.Action)},
        {"Reason", FormatOrderEnforcerReason(enforcer.Reason)}
    };
    if (!enforcer.SortElements.empty()) {
        rows.emplace_back("Sort", ToStdString(FormatSortElements(enforcer.SortElements)));
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

std::string FormatOrderEnforcer(const TOrderEnforcer& enforcer) {
    std::vector<std::string> parts = {
        FormatOrderEnforcerAction(enforcer.Action),
        FormatOrderEnforcerReason(enforcer.Reason)
    };
    if (!enforcer.SortElements.empty()) {
        parts.push_back(ToStdString(FormatSortElements(enforcer.SortElements)));
    }
    return JoinStrings(parts);
}

void AddShuffleDecisionField(
    optimizer_trace::Node& node,
    const std::string& key,
    const std::string& title,
    const std::optional<TVector<TInfoUnit>>& shuffleBy,
    const IOperator* typedBy)
{
    if (!shuffleBy) {
        return;
    }

    if (shuffleBy->empty()) {
        node.field(key, "eliminated")
            .detail(optimizer_trace::Widget::warning(title, "Shuffle is eliminated for this input.", "info"));
        return;
    }

    AddInfoUnitField(node, key, title, *shuffleBy, typedBy);
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
        << "/" << entry.GetCannonicalAlias()
        << "/" << entry.ColumnName);
}

std::vector<std::pair<std::string, std::string>> BuildLineageRows(
    const TColumnLineage& lineage,
    const TVector<TInfoUnit>& outputIUs)
{
    std::vector<std::pair<std::string, std::string>> rows;
    TInfoUnitSet emitted;

    for (const auto& unit : outputIUs) {
        const auto it = lineage.Mapping.find(unit);
        if (it == lineage.Mapping.end()) {
            continue;
        }
        emitted.insert(unit);
        rows.emplace_back(ToStdString(FormatInfoUnit(unit)), FormatLineageSource(it->second));
    }

    TVector<TInfoUnit> extra;
    for (const auto& [unit, _] : lineage.Mapping) {
        if (!emitted.contains(unit)) {
            extra.push_back(unit);
        }
    }
    for (const auto& unit : SortInfoUnits(std::move(extra))) {
        rows.emplace_back(ToStdString(FormatInfoUnit(unit)), FormatLineageSource(lineage.Mapping.at(unit)));
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

TInfoUnit GetCanonicalAlias(const TPlanAliases::TCandidates& candidates) {
    Y_ENSURE(!candidates.empty());
    const auto* best = &candidates.front();
    for (const auto& candidate : candidates) {
        if (candidate.Priority < best->Priority ||
            (candidate.Priority == best->Priority && candidate.IU.GetFullName() < best->IU.GetFullName())) {
            best = &candidate;
        }
    }
    return best->IU;
}

std::string FormatAliasCandidate(const TAliasCandidate& candidate) {
    std::string result = ToStdString(FormatInfoUnit(candidate.IU));
    if (candidate.Priority != 0) {
        result += " (priority " + std::to_string(candidate.Priority) + ")";
    }
    return result;
}

std::vector<std::pair<std::string, std::string>> BuildAliasRows(const TPlanAliases::TAliasMap& aliases) {
    TVector<TInfoUnit> keys;
    keys.reserve(aliases.size());
    for (const auto& [unit, _] : aliases) {
        keys.push_back(unit);
    }

    std::vector<std::pair<std::string, std::string>> rows;
    TInfoUnitSet emittedCanonicals;
    for (const auto& unit : SortInfoUnits(std::move(keys))) {
        const auto it = aliases.find(unit);
        Y_ENSURE(it != aliases.end());
        if (it->second.empty()) {
            continue;
        }

        const auto canonical = GetCanonicalAlias(it->second);
        if (!emittedCanonicals.insert(canonical).second) {
            continue;
        }

        auto candidates = it->second;
        std::sort(candidates.begin(), candidates.end(), [](const TAliasCandidate& lhs, const TAliasCandidate& rhs) {
            if (lhs.Priority != rhs.Priority) {
                return lhs.Priority < rhs.Priority;
            }
            return lhs.IU.GetFullName() < rhs.IU.GetFullName();
        });

        if (candidates.size() == 1 && candidates.front().IU == canonical && candidates.front().Priority == 0) {
            continue;
        }

        std::vector<std::string> candidateItems;
        candidateItems.reserve(candidates.size());
        for (const auto& candidate : candidates) {
            candidateItems.push_back(FormatAliasCandidate(candidate));
        }
        rows.emplace_back(ToStdString(FormatInfoUnit(canonical)), JoinStrings(candidateItems));
    }
    return rows;
}

struct TForbiddenOutEntry {
    std::string Label;
    std::vector<std::string> Columns;
};

std::vector<TForbiddenOutEntry> BuildForbiddenOutEntries(const IOperator& op, const TPlanProps& props) {
    std::vector<std::tuple<std::string, IOperator*, ui32>> parents;
    parents.reserve(op.Parents.size());
    for (const auto& [parent, childIdx] : op.Parents) {
        parents.emplace_back(
            ToStdString(TStringBuilder() << parent->GetExplainName() << " child " << childIdx),
            parent,
            childIdx);
    }
    std::sort(parents.begin(), parents.end(), [](const auto& lhs, const auto& rhs) {
        return std::get<0>(lhs) < std::get<0>(rhs);
    });

    std::vector<TForbiddenOutEntry> entries;
    for (const auto& [label, parent, childIdx] : parents) {
        const auto& forbidden = props.NameConstraints.GetForbiddenOut(parent, childIdx, const_cast<IOperator*>(&op));
        if (forbidden.empty()) {
            continue;
        }
        entries.push_back({label, MakeInfoUnitItems(SortInfoUnitSet(forbidden))});
    }
    return entries;
}

std::vector<std::string> BuildForbiddenOutSummaryItems(const std::vector<TForbiddenOutEntry>& entries) {
    std::vector<std::string> items;
    items.reserve(entries.size());
    for (const auto& entry : entries) {
        items.push_back(entry.Label + ": " + FormatCountedSummary(entry.Columns));
    }
    return items;
}

std::vector<optimizer_trace::Widget> BuildForbiddenOutWidgets(const std::vector<TForbiddenOutEntry>& entries) {
    std::vector<optimizer_trace::Widget> widgets;
    widgets.reserve(entries.size());
    for (const auto& entry : entries) {
        widgets.push_back(optimizer_trace::Widget::list(entry.Label, entry.Columns).monospaceListText());
    }
    return widgets;
}

std::vector<std::string> FindDuplicateOutputColumns(const TVector<TInfoUnit>& outputIUs) {
    THashMap<TInfoUnit, ui32, TInfoUnit::THashFunction> counts;
    for (const auto& unit : outputIUs) {
        ++counts[unit];
    }

    std::vector<std::string> duplicates;
    for (const auto& [unit, count] : counts) {
        if (count > 1) {
            duplicates.push_back(ToStdString(TStringBuilder() << FormatInfoUnit(unit) << " x" << count));
        }
    }
    std::sort(duplicates.begin(), duplicates.end());
    return duplicates;
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
    return ToStdString(connection.GetExplainName());
}

std::string FormatConnectionDetails(const TConnection& connection) {
    std::vector<std::string> details = {
        "type=" + ToStdString(connection.GetExplainName()),
        "outputIndex=" + std::to_string(connection.GetOutputIndex())
    };

    if (const auto* shuffle = dynamic_cast<const TShuffleConnection*>(&connection)) {
        if (!shuffle->Keys.empty()) {
            details.push_back("hashKeys=" + ToStdString(FormatInfoUnits(shuffle->Keys)));
        }
        if (shuffle->HashFuncType) {
            details.push_back("hashFunc=" + ToStdString(ToString(*shuffle->HashFuncType)));
        }
        details.push_back("useSpilling=" + FormatBool(shuffle->UseSpilling));
    } else if (const auto* merge = dynamic_cast<const TMergeConnection*>(&connection)) {
        if (!merge->Order.empty()) {
            details.push_back("mergeOrder=" + ToStdString(FormatSortElements(merge->Order)));
        }
    }

    return JoinStrings(details);
}

optimizer_trace::Widget BuildStageGraphDetailsWidget(const TStageGraph& graph) {
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
            FormatConnectionDetails(*edge.Connection));
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
    overview.layout("TB", 70, 42);

    for (const auto& node : state.OverviewNodes) {
        auto& graphNode = overview.node(node.Id, node.Label);
        if (node.Op) {
            const auto targets = GetOperatorTargets(state, *node.Op);
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

optimizer_trace::Widget BuildStageGraphSwitcher(const TStageGraph& graph, const TTraceBuildState& state) {
    return optimizer_trace::Widget::switcher("Stage graph")
        .defaultOption("graph")
        .option("graph", "Graph", {BuildStageGraphWidget(graph, state)})
        .option("details", "Details", {BuildStageGraphDetailsWidget(graph)});
}

std::vector<optimizer_trace::Widget> BuildPlanWidgets(const TOpRoot& root, const TTraceBuildState& state) {
    std::vector<optimizer_trace::Widget> widgets;
    if (root.PlanProps.StageGraph.StageIds.empty()) {
        return widgets;
    }

    widgets.push_back(BuildStageGraphSwitcher(root.PlanProps.StageGraph, state));
    return widgets;
}

void AddPlanWidgets(optimizer_trace::Trace::Tile& tile, const TOpRoot& root, const TTraceBuildState& state) {
    if (!state.OverviewNodes.empty()) {
        tile.info().tab("overview", "Overview")
            .widget(BuildPlanOverviewWidget(state));
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

void AttachPlanOverview(TTraceBuildState* state, const TIntrusivePtr<IOperator>& op) {
    if (!state) {
        return;
    }

    EnsureOverviewNode(state, *op);
    for (const auto& child : op->Children) {
        AttachOverviewEdge(state, *op, *child);
    }
}

optimizer_trace::Node BuildPlanNode(
    const TIntrusivePtr<IOperator>& op,
    TExprContext& ctx,
    TPlanProps& planProps,
    ui32 opts,
    const std::string& id,
    TTraceBuildState* state)
{
    optimizer_trace::Node node(id, ToStdString(op->GetExplainName()), ToStdString(op->ToString(ctx)));
    AttachStageTarget(state, *op, id);
    AttachOperatorTarget(state, *op, id);
    AttachPlanOverview(state, op);

    const auto outputIUs = op->GetOutputIUs();
    AddInfoUnitField(node, "OutputColumns", "Output columns", outputIUs, op.Get());
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

    AddShuffleDecisionField(node, "LeftShuffleBy", "Left shuffle by", op->Props.LeftShuffleBy, op.Get());
    AddShuffleDecisionField(node, "RightShuffleBy", "Right shuffle by", op->Props.RightShuffleBy, op.Get());

    if (op->Props.OrderEnforcer) {
        node.field("OrderEnforcer", FormatOrderEnforcer(*op->Props.OrderEnforcer))
            .detail(BuildColumnValueTable("Order enforcer", BuildOrderEnforcerRows(*op->Props.OrderEnforcer)));
    }

    if (op->Props.EnsureAtMostOne) {
        node.field("EnsureAtMostOne", "true");
    }

    if ((opts & (EPrintPlanOptions::PrintBasicMetadata | EPrintPlanOptions::PrintFullMetadata))
        && op->Props.Metadata.has_value()) {
        const auto& meta = *op->Props.Metadata;

        if (meta.StorageType != EStorageType::NA) {
            node.field("Storage", FormatStorageType(meta.StorageType));
        }

        if (!meta.KeyColumns.empty()) {
            AddInfoUnitField(node, "KeyColumns", "Key columns", meta.KeyColumns, op.Get());
        }

        if (!meta.SortColumns.empty()) {
            AddStringListField(node, "SortBy", "Sort by", MakeSortColumnItems(meta.SortColumns));
        }

        if (!meta.ShuffledByColumns.empty()) {
            AddInfoUnitField(node, "ShuffledBy", "Shuffled by", meta.ShuffledByColumns, op.Get());
        }

        if (meta.ShufflingOrderingIdx) {
            node.field("ShufflingOrderingIdx", std::to_string(*meta.ShufflingOrderingIdx))
                .detail(BuildColumnValueTable("Shuffling ordering", {
                    {"Ordering index", std::to_string(*meta.ShufflingOrderingIdx)},
                    {"Shuffled by", ToStdString(FormatInfoUnits(meta.ShuffledByColumns))}
                }));
        }

        if (meta.SortingOrderingIdx) {
            node.field("SortingOrderingIdx", std::to_string(*meta.SortingOrderingIdx))
                .detail(BuildColumnValueTable("Sorting ordering", {
                    {"Ordering index", std::to_string(*meta.SortingOrderingIdx)},
                    {"Sort by", ToStdString(FormatSortColumns(meta.SortColumns))}
                }));
        }

        node.field("Type", FormatStatisticsType(meta.Type));
        node.field("LogicalCard", FormatLogicalCardinality(meta.LogicalCard));

        if (!meta.ColumnLineage.Mapping.empty()) {
            const auto rows = BuildLineageRows(meta.ColumnLineage, outputIUs);
            node.field("Lineage", FormatPairSummary(rows))
                .detail(BuildColumnTable("Lineage", rows));
        }
    }

    const auto liveOutIt = planProps.LiveOut.find(op.get());
    if (liveOutIt != planProps.LiveOut.end()) {
        AddInfoUnitField(node, "LiveOut", "Live out", SortInfoUnitSet(liveOutIt->second));
    }

    const auto usedIUs = SortInfoUnits(UniqueInfoUnits(op->GetUsedIUs(planProps)));
    if (!usedIUs.empty()) {
        AddInfoUnitField(node, "UsedIUs", "Used IUs", usedIUs, op.Get());
    }

    const auto aliasIt = planProps.Aliases.AliasesAtOutput.find(op.get());
    if (aliasIt != planProps.Aliases.AliasesAtOutput.end()) {
        const auto rows = BuildAliasRows(aliasIt->second);
        if (!rows.empty()) {
            node.field("Aliases", FormatPairSummary(rows))
                .detail(BuildColumnTable("Aliases at output", rows));
        }
    }

    const auto forbiddenEntries = BuildForbiddenOutEntries(*op, planProps);
    if (!forbiddenEntries.empty()) {
        node.field("ForbiddenOut", FormatCountedSummary(BuildForbiddenOutSummaryItems(forbiddenEntries), 3))
            .details(BuildForbiddenOutWidgets(forbiddenEntries));
    }

    const auto duplicateOutputColumns = FindDuplicateOutputColumns(outputIUs);
    if (!duplicateOutputColumns.empty()) {
        node.field("OutputConflicts", FormatCountedSummary(duplicateOutputColumns))
            .details({
                optimizer_trace::Widget::warning(
                    "Output conflicts",
                    "The operator output contains duplicate information unit names.",
                    "warning"),
                optimizer_trace::Widget::list("Duplicate output columns", duplicateOutputColumns).monospaceListText()
            });
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

    for (size_t i = 0; i < op->Children.size(); ++i) {
        node.child(BuildPlanNode(op->Children[i], ctx, planProps, opts, id + "-" + std::to_string(i), state));
    }
    return node;
}

optimizer_trace::Node BuildPlanNodeFromRoot(TOpRoot& root, TExprContext& ctx, ui32 opts, TTraceBuildState* state) {
    const auto& subplans = root.PlanProps.Subplans.PlanMap;
    if (subplans.empty()) {
        return BuildPlanNode(root.GetInput(), ctx, root.PlanProps, opts, "n-0", state);
    }
    optimizer_trace::Node container("n", "Plan", "Plan");
    size_t index = 0;
    for (const auto& [iu, subplan] : subplans) {
        const std::string subplanId = "n-" + std::to_string(index++);
        optimizer_trace::Node sub(subplanId, "Subplan", "Subplan [" + std::string(iu.GetFullName().c_str()) + "]");
        sub.field("SubplanType", FormatSubplanType(subplan.Type));
        if (!subplan.Tuple.empty()) {
            AddInfoUnitField(sub, "SubplanTuple", "Subplan tuple", subplan.Tuple);
        }
        if (!subplan.DependentIUs.empty()) {
            AddInfoUnitField(sub, "SubplanDependentIUs", "Subplan dependent IUs", subplan.DependentIUs);
        }
        sub.child(BuildPlanNode(CastOperator<IOperator>(subplan.Plan), ctx, root.PlanProps, opts, subplanId + "-0", state));
        container.child(sub);
    }
    container.child(BuildPlanNode(root.GetInput(), ctx, root.PlanProps, opts, "n-" + std::to_string(index), state));
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
        {"EnsureAtMostOne", "At most one"},
        {"Storage", "Storage"},
        {"KeyColumns", "Key columns"},
        {"SortBy", "Sort by"},
        {"ShuffledBy", "Shuffled by"},
        {"ShufflingOrderingIdx", "Shuffle ordering"},
        {"SortingOrderingIdx", "Sort ordering"},
        {"Type", "Type"},
        {"LogicalCard", "Logical card"},
        {"Lineage", "Lineage"},
        {"LiveOut", "Live out"},
        {"UsedIUs", "Used IUs"},
        {"Aliases", "Aliases"},
        {"ForbiddenOut", "Forbidden out"},
        {"OutputConflicts", "Conflicts"},
        {"ERows", "Rows"},
        {"EBytes", "Bytes"},
        {"Selectivity", "Selectivity"},
        {"Cost", "Cost"}
    });
    trace.pinFields({"ERows", "EBytes", "Selectivity", "Cost"});
    trace.definePinnedFieldPresets({
        {"None", {}},
        {"Stages", {"Stage"}}
    });
    trace.defineDiffFieldPresets({
        {"None", {}},
        {"Stages", {"Stage"}},
        {"All", {
            "OutputColumns",
            "OutputType",
            "SubplanType",
            "SubplanTuple",
            "SubplanDependentIUs",
            "Stage",
            "Algorithm",
            "JoinAlgo",
            "LeftShuffleBy",
            "RightShuffleBy",
            "OrderEnforcer",
            "EnsureAtMostOne",
            "Storage",
            "KeyColumns",
            "SortBy",
            "ShuffledBy",
            "ShufflingOrderingIdx",
            "SortingOrderingIdx",
            "Type",
            "LogicalCard",
            "Lineage",
            "LiveOut",
            "UsedIUs",
            "Aliases",
            "ForbiddenOut",
            "OutputConflicts",
            "ERows",
            "EBytes",
            "Selectivity",
            "Cost"
        }}
    });
}

} // namespace NKqp
} // namespace NKikimr
