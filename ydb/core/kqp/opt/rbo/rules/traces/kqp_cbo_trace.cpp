#include "kqp_cbo_trace.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_cbo_latency_predictor.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_make_join_hypergraph.h>
#include <ydb/core/kqp/opt/rbo/traces/kqp_rbo_trace.h>

#include <library/cpp/iterator/zip.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

#include <algorithm>
#include <bitset>
#include <cctype>
#include <sstream>
#include <tuple>
#include <vector>

namespace NKikimr::NKqp {

namespace {

struct THypergraphInfo {
    std::string Dump;
    std::string EnumerationAlgorithm;
    std::string PredictedTime;
    ui64 PredictedTimeNs = 0;
    std::optional<optimizer_trace::Widget> GraphWidget;
};

std::vector<std::pair<std::string, std::string>> BuildCBOColumnMappingRows(const TVector<TCBOLeaf>& leaves) {
    struct TColumnMappingRow {
        size_t LeafIndex = 0;
        std::string InputColumn;
        std::string OptimizerColumn;
    };

    std::vector<TColumnMappingRow> sortedRows;
    for (size_t leafIndex = 0; leafIndex < leaves.size(); ++leafIndex) {
        const auto& leaf = leaves[leafIndex];
        for (const auto& [rboColumn, cboColumn] : leaf.ColumnsToCBO) {
            sortedRows.push_back({
                leafIndex,
                ToStdString(FormatInfoUnit(rboColumn)),
                ToStdString(FormatInfoUnit(cboColumn))
            });
        }
    }

    std::sort(sortedRows.begin(), sortedRows.end(), [](const TColumnMappingRow& lhs, const TColumnMappingRow& rhs) {
        return std::tie(lhs.LeafIndex, lhs.InputColumn, lhs.OptimizerColumn) <
            std::tie(rhs.LeafIndex, rhs.InputColumn, rhs.OptimizerColumn);
    });

    std::vector<std::pair<std::string, std::string>> rows;
    rows.reserve(sortedRows.size());
    for (const auto& row : sortedRows) {
        rows.emplace_back(row.InputColumn, row.OptimizerColumn);
    }
    return rows;
}

std::string StripGraphPath(const TString& value);
std::string ShortenGraphToken(std::string token);

using TTraceTargets = std::vector<optimizer_trace::Target>;
using TRelationTargetMap = THashMap<std::string, TTraceTargets>;

void AppendUniqueTargets(TTraceTargets& dst, const TTraceTargets& src) {
    for (const auto& target : src) {
        const bool exists = std::any_of(dst.begin(), dst.end(), [&](const optimizer_trace::Target& current) {
            return current.type() == target.type() && current.nodeId() == target.nodeId();
        });
        if (!exists) {
            dst.push_back(target);
        }
    }
}

TTraceTargets GetLeafTargets(const TTraceBuildState& state, const TCBOLeaf& leaf) {
    return GetOperatorTargets(state, *leaf.Op);
}

TTraceTargets GetCboTreeTargets(
    const TTraceBuildState& state,
    const TOpCBOTree& cboTree,
    const TVector<TCBOLeaf>& leaves)
{
    TTraceTargets targets;
    AppendUniqueTargets(targets, GetOperatorTargets(state, cboTree));
    AppendUniqueTargets(targets, GetOperatorTargets(state, *cboTree.TreeRoot));
    for (const auto& node : cboTree.TreeNodes) {
        AppendUniqueTargets(targets, GetOperatorTargets(state, *node));
    }
    for (const auto& leaf : leaves) {
        AppendUniqueTargets(targets, GetLeafTargets(state, leaf));
    }
    return targets;
}

TRelationTargetMap BuildRelationTargetMap(const TTraceBuildState& state, const TVector<TCBOLeaf>& leaves) {
    TRelationTargetMap targets;
    for (const auto& leaf : leaves) {
        const auto leafTargets = GetLeafTargets(state, leaf);
        if (leafTargets.empty()) {
            continue;
        }
        AppendUniqueTargets(targets[ToStdString(leaf.RelationName)], leafTargets);
        if (!leaf.SourceTableName.empty()) {
            AppendUniqueTargets(targets[ToStdString(leaf.SourceTableName)], leafTargets);
            AppendUniqueTargets(targets[StripGraphPath(leaf.SourceTableName)], leafTargets);
        }
        AppendUniqueTargets(targets[StripGraphPath(leaf.RelationName)], leafTargets);
    }
    return targets;
}

TTraceTargets GetRelationTargets(
    const TRelationTargetMap* relationTargets,
    const std::string& relationName)
{
    if (!relationTargets) {
        return {};
    }
    TTraceTargets result;
    if (const auto it = relationTargets->find(relationName); it != relationTargets->end()) {
        AppendUniqueTargets(result, it->second);
    }
    const auto shortened = ShortenGraphToken(relationName);
    if (shortened != relationName) {
        if (const auto it = relationTargets->find(shortened); it != relationTargets->end()) {
            AppendUniqueTargets(result, it->second);
        }
    }
    return result;
}

TTraceTargets GetTextRelationTargets(
    const TRelationTargetMap* relationTargets,
    const std::string& text)
{
    TTraceTargets result;
    if (!relationTargets) {
        return result;
    }
    for (const auto& [relationName, targets] : *relationTargets) {
        if (!relationName.empty() && text.find(relationName) != std::string::npos) {
            AppendUniqueTargets(result, targets);
        }
    }
    return result;
}

std::string StripGraphPath(const TString& value) {
    const auto lastSlash = value.rfind('/');
    if (lastSlash == TString::npos) {
        return ToStdString(value);
    }
    return ToStdString(value.substr(lastSlash + 1));
}

std::string ShortenGraphToken(std::string token) {
    const auto lastSlash = token.rfind('/');
    if (lastSlash == std::string::npos) {
        return token;
    }
    return token.substr(lastSlash + 1);
}

bool IsGraphTokenChar(char ch) {
    return std::isalnum(static_cast<unsigned char>(ch)) ||
        ch == '_' || ch == '/' || ch == '.' || ch == '$' || ch == '#' || ch == '-';
}

std::string RemoveSingletonSetBraces(const std::string& text) {
    std::string result;
    result.reserve(text.size());

    for (size_t pos = 0; pos < text.size();) {
        if (text[pos] != '{') {
            result.push_back(text[pos++]);
            continue;
        }

        const size_t close = text.find('}', pos + 1);
        if (close == std::string::npos) {
            result.push_back(text[pos++]);
            continue;
        }

        const std::string content = text.substr(pos + 1, close - pos - 1);
        if (!content.empty() &&
            content.find(',') == std::string::npos &&
            content.find('{') == std::string::npos &&
            content.find('}') == std::string::npos) {
            result += content;
        } else {
            result.append(text, pos, close - pos + 1);
        }
        pos = close + 1;
    }

    return result;
}

std::string ShortenGraphText(const TString& value) {
    const std::string text = ToStdString(value);
    std::string result;
    result.reserve(text.size());

    for (size_t pos = 0; pos < text.size();) {
        if (!IsGraphTokenChar(text[pos])) {
            result.push_back(text[pos++]);
            continue;
        }

        const size_t start = pos;
        while (pos < text.size() && IsGraphTokenChar(text[pos])) {
            ++pos;
        }
        result += ShortenGraphToken(text.substr(start, pos - start));
    }

    return RemoveSingletonSetBraces(result);
}

std::string ShortenGraphConditionToken(const std::string& token) {
    const auto shortened = ShortenGraphToken(token);
    const auto dot = shortened.rfind('.');
    if (dot == std::string::npos || dot + 1 == shortened.size()) {
        return shortened;
    }
    return shortened.substr(dot + 1);
}

std::string ShortenGraphConditionText(const TString& value) {
    const std::string text = ToStdString(value);
    std::string result;
    result.reserve(text.size());

    for (size_t pos = 0; pos < text.size();) {
        if (!IsGraphTokenChar(text[pos])) {
            result.push_back(text[pos++]);
            continue;
        }

        const size_t start = pos;
        while (pos < text.size() && IsGraphTokenChar(text[pos])) {
            ++pos;
        }
        result += ShortenGraphConditionToken(text.substr(start, pos - start));
    }

    return RemoveSingletonSetBraces(result);
}

std::string FormatJoinCondition(const TJoinColumn& left, const TJoinColumn& right) {
    return ToStdString(TStringBuilder()
        << left.AttributeName
        << " = "
        << right.AttributeName);
}

template <typename TNodeSet>
std::vector<std::string> BuildHypergraphRelationLabels(const TJoinHypergraph<TNodeSet>& hypergraph) {
    const auto& nodes = hypergraph.GetNodes();
    std::vector<std::string> labels;
    labels.reserve(nodes.size());

    for (size_t nodeId = 0; nodeId < nodes.size(); ++nodeId) {
        if (nodes[nodeId].RelationOptimizerNode) {
            const auto nodeLabels = nodes[nodeId].RelationOptimizerNode->Labels();
            if (!nodeLabels.empty()) {
                labels.push_back(StripGraphPath(nodeLabels.front()));
                continue;
            }
        }

        labels.push_back("rel#" + std::to_string(nodeId));
    }

    return labels;
}

template <typename TNodeSet>
std::vector<std::string> BuildHypergraphSetItems(
    const TNodeSet& nodeSet,
    const std::vector<std::string>& relationLabels)
{
    std::vector<std::string> items;
    for (size_t i = 0; i < relationLabels.size() && i < nodeSet.size(); ++i) {
        if (nodeSet[i]) {
            items.push_back(relationLabels[i]);
        }
    }
    return items;
}

std::string FormatHypergraphSet(const std::vector<std::string>& items) {
    if (items.empty()) {
        return "{}";
    }
    if (items.size() == 1) {
        return items.front();
    }
    return "{" + JoinStrings(items) + "}";
}

template <typename TNodeSet>
std::string BuildHypergraphSetKey(const TNodeSet& nodeSet) {
    std::vector<std::string> items;
    for (size_t i = 0; i < nodeSet.size(); ++i) {
        if (nodeSet[i]) {
            items.push_back(std::to_string(i));
        }
    }
    return JoinStrings(items);
}

template <typename TNodeSet>
std::string BuildHypergraphEdgeNote(const typename TJoinHypergraph<TNodeSet>::TEdge& edge) {
    std::vector<std::string> lines = {
        "Join kind: " + ToStdString(ConvertToJoinString(edge.JoinKind)),
        "Commutative: " + FormatBool(edge.IsCommutative),
        "Left any: " + FormatBool(edge.LeftAny),
        "Right any: " + FormatBool(edge.RightAny)
    };

    std::vector<std::string> conditions;
    conditions.reserve(edge.LeftJoinKeys.size());
    for (const auto& [left, right] : Zip(edge.LeftJoinKeys, edge.RightJoinKeys)) {
        conditions.push_back(FormatJoinCondition(left, right));
    }
    if (!conditions.empty()) {
        lines.push_back("Conditions: " + JoinStrings(conditions));
    }

    return JoinStrings(lines, "\n");
}

template <typename TNodeSet>
optimizer_trace::Widget BuildHypergraphGraphWidget(
    TJoinHypergraph<TNodeSet>& hypergraph,
    const TRelationTargetMap* relationTargets = nullptr)
{
    const auto relationLabels = BuildHypergraphRelationLabels(hypergraph);

    optimizer_trace::Graph graph;
    graph.layout("LR", 62, 36);
    graph.undirected();

    THashMap<std::string, std::string> setNodeIds;
    auto ensureSetNode = [&](const TNodeSet& nodeSet) -> std::string {
        const auto key = BuildHypergraphSetKey(nodeSet);
        const auto setItems = BuildHypergraphSetItems(nodeSet, relationLabels);
        const auto title = FormatHypergraphSet(setItems);
        if (const auto it = setNodeIds.find(key); it != setNodeIds.end()) {
            return it->second;
        }

        const std::string id = "set-" + std::to_string(setNodeIds.size());
        setNodeIds.emplace(key, id);
        auto& graphNode = graph.node(id, title)
            .note(setItems.empty() ? "Empty set" : "Relations: " + JoinStrings(setItems));

        TTraceTargets targets;
        for (size_t i = 0; i < relationLabels.size() && i < nodeSet.size(); ++i) {
            if (nodeSet[i]) {
                AppendUniqueTargets(targets, GetRelationTargets(relationTargets, relationLabels[i]));
            }
        }
        if (!targets.empty()) {
            graphNode.targets(targets).primaryTarget(targets.front());
        }
        return id;
    };

    size_t displayedEdgeId = 0;
    for (const auto& edge : hypergraph.GetEdges()) {
        if (edge.IsReversed) {
            continue;
        }

        const auto leftNodeId = ensureSetNode(edge.Left);
        const auto rightNodeId = ensureSetNode(edge.Right);
        graph.edge(leftNodeId, rightNodeId)
            .setId("hyperedge-" + std::to_string(displayedEdgeId++))
            .setLabel(ToStdString(ConvertToJoinString(edge.JoinKind)))
            .note(BuildHypergraphEdgeNote<TNodeSet>(edge));
    }

    return optimizer_trace::Widget::graph("CBO hypergraph", graph).monospaceGraph();
}

template <typename TNodeSet>
THypergraphInfo BuildHypergraphInfoImpl(
    const std::shared_ptr<IBaseOptimizerNode>& joinTree,
    const TOptimizerHints& hints,
    const TRelationTargetMap* relationTargets = nullptr)
{
    auto hypergraph = MakeJoinHypergraph<TNodeSet>(joinTree, hints, false);
    const ui64 predictedTimeNs = PredictCBOTime(hypergraph);
    return {
        .Dump = std::string(hypergraph.String().c_str()),
        .EnumerationAlgorithm = "DPHypClassic",
        .PredictedTime = FormatTime(predictedTimeNs),
        .PredictedTimeNs = predictedTimeNs,
        .GraphWidget = BuildHypergraphGraphWidget(hypergraph, relationTargets)
    };
}

THypergraphInfo BuildHypergraphInfo(
    const std::shared_ptr<TJoinOptimizerNode>& joinTree,
    const TOptimizerHints& hints,
    const TRelationTargetMap* relationTargets = nullptr)
{
    const auto relsCount = joinTree->Labels().size();
    if (relsCount <= 64) {
        return BuildHypergraphInfoImpl<std::bitset<64>>(joinTree, hints, relationTargets);
    }
    if (relsCount <= 128) {
        return BuildHypergraphInfoImpl<std::bitset<128>>(joinTree, hints, relationTargets);
    }
    if (relsCount <= 192) {
        return BuildHypergraphInfoImpl<std::bitset<192>>(joinTree, hints, relationTargets);
    }

    return {
        .Dump = "Hypergraph is not built for joins with more than 192 relations.",
        .EnumerationAlgorithm = "Disabled: more than 192 relations",
        .PredictedTime = "N/A",
        .PredictedTimeNs = 0,
        .GraphWidget = std::nullopt
    };
}

template <typename THints>
void AddHintWidget(
    std::vector<optimizer_trace::Widget>& widgets,
    const char* title,
    const THints& hints)
{
    if (hints.empty()) {
        return;
    }

    TStringBuilder content;
    for (size_t i = 0; i < hints.size(); ++i) {
        if (i) {
            content << "\n";
        }
        content << hints[i].StringRepr;
    }
    widgets.push_back(optimizer_trace::Widget::unwrappedText(title, std::string(content.c_str())));
}

void AddOptimizerHintsInfo(std::vector<optimizer_trace::Widget>& widgets, const TOptimizerHints& hints) {
    AddHintWidget(widgets, "Join algorithm hints", hints.JoinAlgoHints->Hints);
    AddHintWidget(widgets, "Join order hints", hints.JoinOrderHints->Hints);
    AddHintWidget(widgets, "Cardinality hints", hints.CardinalityHints->Hints);
    AddHintWidget(widgets, "Bytes hints", hints.BytesHints->Hints);
}

struct TOrderingsGraphNode {
    TString Id;
    TString Label;
    TString Kind;
    TString Tooltip;
    TVector<std::pair<TString, TString>> Metrics;
};

struct TOrderingsGraphEdge {
    TString Id;
    TString From;
    TString To;
    TString Label;
    TString Kind;
};

struct TOrderingsGraphModel {
    TVector<TOrderingsGraphNode> Nodes;
    TVector<TOrderingsGraphEdge> Edges;
};

TString FormatGraphColumn(const TFDStorage& storage, std::size_t idx) {
    const auto& columns = storage.GetColumns();
    if (idx >= columns.size()) {
        return ToString(idx);
    }

    const auto& column = columns[idx];
    if (column.RelName.empty()) {
        return column.AttributeName;
    }
    return TStringBuilder() << column.RelName << "." << column.AttributeName;
}

TString FormatGraphOrdering(const TFDStorage& storage, const TOrdering& ordering) {
    TStringBuilder result;
    result << "{";
    for (size_t i = 0; i < ordering.Items.size(); ++i) {
        if (i) {
            result << ", ";
        }

        result << FormatGraphColumn(storage, ordering.Items[i]);
        if (i < ordering.Directions.size()) {
            switch (ordering.Directions[i]) {
                case TOrdering::TItem::EDirection::EAscending:
                    result << " ASC";
                    break;
                case TOrdering::TItem::EDirection::EDescending:
                    result << " DESC";
                    break;
                default:
                    break;
            }
        }
    }
    result << "}";
    return result;
}

TString FormatGraphFD(const TFDStorage& storage, const TFunctionalDependency& fd) {
    TStringBuilder result;
    result << "{";
    for (size_t i = 0; i < fd.AntecedentItems.size(); ++i) {
        if (i) {
            result << ", ";
        }
        result << FormatGraphColumn(storage, fd.AntecedentItems[i]);
    }
    result << "}";
    result << (fd.Type == TFunctionalDependency::EEquivalence ? " = " : " -> ");
    result << FormatGraphColumn(storage, fd.ConsequentItem);
    if (fd.AlwaysActive) {
        result << " (AA)";
    }
    return result;
}

TString FormatGraphFDByIndex(
    const TFDStorage& storage,
    const std::vector<TFunctionalDependency>& fds,
    i64 fdIdx)
{
    if (fdIdx < 0) {
        return "eps";
    }
    if (static_cast<size_t>(fdIdx) >= fds.size()) {
        return TStringBuilder() << "fd " << fdIdx;
    }
    return FormatGraphFD(storage, fds[fdIdx]);
}

TString JoinIndexList(const std::vector<std::size_t>& values, size_t limit = 8) {
    if (values.empty()) {
        return "-";
    }

    TStringBuilder result;
    for (size_t i = 0; i < values.size() && i < limit; ++i) {
        if (i) {
            result << ", ";
        }
        result << values[i];
    }
    if (values.size() > limit) {
        result << ", ...";
    }
    return result;
}

TString InitOrderingList(const TOrderingsStateMachine::TDebugView& view, size_t stateIdx) {
    TStringBuilder result;
    bool wrote = false;
    for (size_t i = 0; i < view.InitStates.size(); ++i) {
        if (view.InitStates[i].StateIdx != stateIdx) {
            continue;
        }
        if (wrote) {
            result << ", ";
        }
        result << i;
        wrote = true;
    }
    return wrote ? TString(result) : TString("-");
}

TOrderingsGraphModel BuildNFSMGraphModel(const TOrderingsStateMachine::TDebugView& view) {
    TOrderingsGraphModel model;
    if (!view.Storage || !view.ProcessedFDs) {
        return model;
    }

    model.Nodes.reserve(view.NFSMNodes.size());
    for (size_t i = 0; i < view.NFSMNodes.size(); ++i) {
        const auto& node = view.NFSMNodes[i];
        TOrderingsGraphNode graphNode;
        graphNode.Id = TStringBuilder() << "nfsm-" << i;
        graphNode.Label = FormatGraphOrdering(*view.Storage, node.Ordering);
        graphNode.Kind = node.IsInteresting ? "interesting" : "artificial";
        graphNode.Tooltip = node.Details;
        graphNode.Metrics.emplace_back("node", ::ToString(i));
        graphNode.Metrics.emplace_back("kind", graphNode.Kind);
        if (node.InterestingOrderingIdx >= 0) {
            graphNode.Metrics.emplace_back("interesting", ::ToString(node.InterestingOrderingIdx));
        }
        model.Nodes.push_back(std::move(graphNode));
    }

    model.Edges.reserve(view.NFSMEdges.size());
    for (size_t i = 0; i < view.NFSMEdges.size(); ++i) {
        const auto& edge = view.NFSMEdges[i];
        model.Edges.push_back({
            .Id = TStringBuilder() << "nfsm-edge-" << i,
            .From = TStringBuilder() << "nfsm-" << edge.SrcNodeIdx,
            .To = TStringBuilder() << "nfsm-" << edge.DstNodeIdx,
            .Label = FormatGraphFDByIndex(*view.Storage, *view.ProcessedFDs, edge.FdIdx),
            .Kind = edge.FdIdx < 0 ? TString("epsilon") : TString("fd")
        });
    }

    return model;
}

TOrderingsGraphModel BuildDFSMGraphModel(const TOrderingsStateMachine::TDebugView& view) {
    TOrderingsGraphModel model;
    if (!view.Storage || !view.ProcessedFDs) {
        return model;
    }

    model.Nodes.reserve(view.DFSMNodes.size());
    for (size_t i = 0; i < view.DFSMNodes.size(); ++i) {
        const auto& node = view.DFSMNodes[i];
        const auto initOrderings = InitOrderingList(view, i);
        model.Nodes.push_back({
            .Id = TStringBuilder() << "dfsm-" << i,
            .Label = TStringBuilder() << "State " << i,
            .Kind = initOrderings == "-" ? TString("state") : TString("init"),
            .Tooltip = node.Details,
            .Metrics = {
                {"nfsm", JoinIndexList(node.NFSMNodes)},
                {"interesting", JoinIndexList(node.InterestingOrderingIdxes)},
                {"init", initOrderings},
                {"out fd", JoinIndexList(node.OutgoingFDIdxes)}
            }
        });
    }

    model.Edges.reserve(view.DFSMEdges.size());
    for (size_t i = 0; i < view.DFSMEdges.size(); ++i) {
        const auto& edge = view.DFSMEdges[i];
        model.Edges.push_back({
            .Id = TStringBuilder() << "dfsm-edge-" << i,
            .From = TStringBuilder() << "dfsm-" << edge.SrcNodeIdx,
            .To = TStringBuilder() << "dfsm-" << edge.DstNodeIdx,
            .Label = FormatGraphFDByIndex(*view.Storage, *view.ProcessedFDs, edge.FdIdx),
            .Kind = "fd"
        });
    }

    return model;
}

std::string BuildGraphNodeNote(
    const TString& kind,
    const TString& tooltip,
    const TVector<std::pair<TString, TString>>& metrics)
{
    TStringBuilder note;
    bool wrote = false;
    if (!kind.empty()) {
        note << "Kind: " << kind;
        wrote = true;
    }
    for (const auto& [key, value] : metrics) {
        if (wrote) {
            note << "\n";
        }
        note << ShortenGraphText(key) << ": " << ShortenGraphText(value);
        wrote = true;
    }
    if (!tooltip.empty()) {
        if (wrote) {
            note << "\n\n";
        }
        note << ShortenGraphText(tooltip);
    }
    return ToStdString(note);
}

optimizer_trace::Widget BuildOrderingsGraphWidget(
    const std::string& title,
    const TOrderingsGraphModel& model,
    const TRelationTargetMap* relationTargets = nullptr)
{
    optimizer_trace::Graph graph;
    graph.layout("LR", 58, 34);

    for (const auto& node : model.Nodes) {
        auto& graphNode = graph.node(ToStdString(node.Id), ShortenGraphText(node.Label))
            .note(BuildGraphNodeNote(node.Kind, node.Tooltip, node.Metrics));

        std::string targetText = ToStdString(node.Label);
        targetText += "\n";
        targetText += ToStdString(node.Tooltip);
        for (const auto& [key, value] : node.Metrics) {
            targetText += "\n";
            targetText += ToStdString(key);
            targetText += "=";
            targetText += ToStdString(value);
        }
        const auto targets = GetTextRelationTargets(relationTargets, targetText);
        if (!targets.empty()) {
            graphNode.targets(targets).primaryTarget(targets.front());
        }
    }

    for (const auto& edge : model.Edges) {
        auto& graphEdge = graph.edge(ToStdString(edge.From), ToStdString(edge.To))
            .setId(ToStdString(edge.Id))
            .setLabel(ShortenGraphConditionText(edge.Label))
            .note(ToStdString(TStringBuilder() << "Kind: " << edge.Kind));
        const auto targets = GetTextRelationTargets(relationTargets, ToStdString(edge.Label));
        if (!targets.empty()) {
            graphEdge.targets(targets).primaryTarget(targets.front());
        }
    }

    return optimizer_trace::Widget::graph(title, graph).monospaceGraph();
}

std::string BuildOrderingsGraphModelText(
    const std::string& title,
    const TOrderingsGraphModel& model)
{
    std::ostringstream out;
    out << title << "\n\nNodes:\n";
    for (const auto& node : model.Nodes) {
        out << ToStdString(node.Id) << ": " << ToStdString(node.Label);
        if (!node.Kind.empty()) {
            out << " [" << ToStdString(node.Kind) << "]";
        }
        if (!node.Metrics.empty()) {
            out << " ";
            for (size_t i = 0; i < node.Metrics.size(); ++i) {
                if (i) {
                    out << ", ";
                }
                out << ToStdString(node.Metrics[i].first) << "=" << ToStdString(node.Metrics[i].second);
            }
        }
        if (!node.Tooltip.empty()) {
            out << "\n  " << ToStdString(node.Tooltip);
        }
        out << "\n";
    }

    out << "\nEdges:\n";
    for (const auto& edge : model.Edges) {
        out << ToStdString(edge.Id)
            << ": " << ToStdString(edge.From)
            << " -> " << ToStdString(edge.To);
        if (!edge.Label.empty()) {
            out << " [" << ToStdString(edge.Label) << "]";
        }
        if (!edge.Kind.empty()) {
            out << " (" << ToStdString(edge.Kind) << ")";
        }
        out << "\n";
    }
    return out.str();
}

optimizer_trace::Widget BuildGraphTextSwitcher(
    const std::string& id,
    const std::string& title,
    const optimizer_trace::Widget& graphWidget,
    const std::string& textTitle,
    const std::string& text)
{
    return optimizer_trace::Widget::switcher(id, title)
        .defaultOption("graph")
        .option("graph", "Graph", {graphWidget})
        .option("text", "Text", {optimizer_trace::Widget::unwrappedText(textTitle, text)});
}

optimizer_trace::Widget BuildOrderingsFSMWidget(
    const TOrderingsStateMachine& fsm,
    const TRelationTargetMap* relationTargets = nullptr)
{
    const auto debugView = fsm.DebugView();
    const auto dfsm = BuildDFSMGraphModel(debugView);
    const auto nfsm = BuildNFSMGraphModel(debugView);
    return optimizer_trace::Widget::switcher("Shufflings FSM")
        .defaultOption("DFSM")
        .option("DFSM", "DFSM", {
            BuildGraphTextSwitcher(
                "shufflings-dfsm",
                "DFSM",
                BuildOrderingsGraphWidget("Shufflings DFSM", dfsm, relationTargets),
                "Shufflings DFSM",
                BuildOrderingsGraphModelText("Shufflings DFSM", dfsm))
        })
        .option("NFSM", "NFSM", {
            BuildGraphTextSwitcher(
                "shufflings-nfsm",
                "NFSM",
                BuildOrderingsGraphWidget("Shufflings NFSM", nfsm, relationTargets),
                "Shufflings NFSM",
                BuildOrderingsGraphModelText("Shufflings NFSM", nfsm))
        });
}

std::string FormatCboBudget(ui64 predictedTimeNs, ui32 timeoutMs) {
    const ui64 timeoutNs = static_cast<ui64>(timeoutMs) * 1'000'000ULL;
    return FormatTime(predictedTimeNs) + " < " + FormatTime(timeoutNs);
}

std::string FormatCboTime(ui64 runtimeNs, ui32 hardTimeoutMs) {
    const ui64 hardTimeoutNs = static_cast<ui64>(hardTimeoutMs) * 1'000'000ULL;
    return FormatTime(runtimeNs) + " < " + FormatTime(hardTimeoutNs);
}

std::vector<std::pair<std::string, std::string>> BuildCboSettingsRows(
    int optLevel,
    const THypergraphInfo& hypergraphInfo,
    const TVector<TCBOLeaf>& leaves,
    const TCBOSettings& settings,
    std::optional<ui64> cboRuntimeNs,
    bool enableShuffleElimination,
    bool useBlockHashJoin,
    bool hasRowStorageInput)
{
    std::vector<std::pair<std::string, std::string>> rows = {
        {"Optimization level", std::to_string(optLevel)},
        {"Enumeration algorithm", hypergraphInfo.EnumerationAlgorithm},
        {"CBO leafs", std::to_string(leaves.size())},
        {"CBO budget", FormatCboBudget(hypergraphInfo.PredictedTimeNs, settings.CBOTimeout)}
    };

    if (cboRuntimeNs) {
        rows.emplace_back("CBO time", FormatCboTime(*cboRuntimeNs, settings.CBOHardTimeout));
    }

    rows.insert(rows.end(), {
        {"SE enabled", FormatBool(enableShuffleElimination)},
        {"Join cutoff", std::to_string(settings.ShuffleEliminationJoinNumCutoff)},
        {"Use block hash join", FormatBool(useBlockHashJoin)},
        {"Has row-storage input", FormatBool(hasRowStorageInput)}
    });
    return rows;
}

std::vector<optimizer_trace::Widget> BuildCboRunWidgets(
    const THypergraphInfo& hypergraphInfo,
    const TVector<TCBOLeaf>& leaves,
    const TCBOSettings& settings,
    int optLevel,
    bool enableShuffleElimination,
    bool useBlockHashJoin,
    bool hasRowStorageInput,
    const TOptimizerHints& hints,
    const TOrderingsStateMachine* fsm,
    std::optional<ui64> cboRuntimeNs = std::nullopt,
    const TRelationTargetMap* relationTargets = nullptr)
{
    std::vector<optimizer_trace::Widget> widgets;

    if (hypergraphInfo.GraphWidget) {
        widgets.push_back(BuildGraphTextSwitcher(
            "cbo-hypergraph",
            "CBO hypergraph",
            *hypergraphInfo.GraphWidget,
            "CBO hypergraph",
            hypergraphInfo.Dump));
    } else {
        widgets.push_back(optimizer_trace::Widget::unwrappedText("CBO hypergraph", hypergraphInfo.Dump));
    }

    const auto columnMappingRows = BuildCBOColumnMappingRows(leaves);
    if (!columnMappingRows.empty()) {
        widgets.push_back(optimizer_trace::Widget::table("Column mapping", columnMappingRows).monospaceTable());
    }

    if (fsm) {
        widgets.push_back(BuildOrderingsFSMWidget(*fsm, relationTargets));
    }

    widgets.push_back(optimizer_trace::Widget::table(
        "CBO settings",
        BuildCboSettingsRows(
            optLevel,
            hypergraphInfo,
            leaves,
            settings,
            cboRuntimeNs,
            enableShuffleElimination,
            useBlockHashJoin,
            hasRowStorageInput)));

    AddOptimizerHintsInfo(widgets, hints);
    return widgets;
}

void SetTabTargets(optimizer_trace::InfoTab& tab, const TTraceTargets& targets) {
    if (!targets.empty()) {
        tab.targets(targets).primaryTarget(targets.front());
    }
}

} // anonymous namespace

void AddCboWarning(TRBOContext& ctx, const TString& message) {
    if (!ctx.NeedToLog()) {
        return;
    }

    const std::string messageText = ToStdString(message);
    ctx.EnrichRuleLog([messageText](optimizer_trace::Trace::Tile& rule) {
        rule.info().tab("cbo-run", "CBO run")
            .widget(optimizer_trace::Widget::warning("CBO did not run", messageText, "warning"));
    });
}

std::shared_ptr<TCboRunTiming> AddCboRunTrace(
    TRBOContext& ctx,
    const TIntrusivePtr<TOpCBOTree>& cboTree,
    const std::shared_ptr<TJoinOptimizerNode>& initialJoinTree,
    const TVector<TCBOLeaf>& leaves,
    const TCBOSettings& settings,
    int optLevel,
    bool enableShuffleElimination,
    bool useBlockHashJoin,
    bool hasRowStorageInput,
    const TOptimizerHints& hints,
    const TSimpleSharedPtr<TOrderingsStateMachine>& shuffleFsm)
{
    if (!ctx.NeedToLog()) {
        return {};
    }

    auto cboRunTiming = std::make_shared<TCboRunTiming>();
    ctx.EnrichRuleLog([
        initialJoinTree,
        leaves,
        settings,
        optLevel,
        enableShuffleElimination,
        useBlockHashJoin,
        hasRowStorageInput,
        hints,
        shuffleFsm
    ](optimizer_trace::Trace::Tile& rule) {
        const auto hypergraphInfo = BuildHypergraphInfo(initialJoinTree, hints);
        auto& tab = rule.info().tab("cbo-run", "CBO run");
        tab.setWidgets(BuildCboRunWidgets(
            hypergraphInfo,
            leaves,
            settings,
            optLevel,
            enableShuffleElimination,
            useBlockHashJoin,
            hasRowStorageInput,
            hints,
            shuffleFsm.Get()));
    });

    ctx.EnrichRuleLogAfterTree([
        cboTree,
        initialJoinTree,
        leaves,
        settings,
        optLevel,
        enableShuffleElimination,
        useBlockHashJoin,
        hasRowStorageInput,
        hints,
        shuffleFsm,
        cboRunTiming
    ](optimizer_trace::Trace::Tile& rule, const TTraceBuildState& state) {
        const auto relationTargets = BuildRelationTargetMap(state, leaves);
        const auto hypergraphInfo = BuildHypergraphInfo(initialJoinTree, hints, &relationTargets);
        auto& runTab = rule.info().tab("cbo-run", "CBO run");
        SetTabTargets(runTab, GetCboTreeTargets(state, *cboTree, leaves));
        runTab.setWidgets(BuildCboRunWidgets(
            hypergraphInfo,
            leaves,
            settings,
            optLevel,
            enableShuffleElimination,
            useBlockHashJoin,
            hasRowStorageInput,
            hints,
            shuffleFsm.Get(),
            cboRunTiming ? cboRunTiming->RuntimeNs : std::nullopt,
            &relationTargets));

        auto& detailsTab = rule.info().tab("cbo-details", "CBO details");
        SetTabTargets(detailsTab, GetCboTreeTargets(state, *cboTree, leaves));
    });

    return cboRunTiming;
}

void AddCboDetailsTextTrace(TRBOContext& ctx, const std::string& title, const std::string& text) {
    if (!ctx.NeedToLog()) {
        return;
    }

    ctx.EnrichRuleLog([title, text](optimizer_trace::Trace::Tile& rule) {
        rule.info().tab("cbo-details", "CBO details")
            .widget(optimizer_trace::Widget::unwrappedText(title, text));
    });
}

} // namespace NKikimr::NKqp
