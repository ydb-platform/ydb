#pragma once

#include "../html_log/cpp/optimizer_trace.h"

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <cstddef>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace NKikimr::NKqp {

namespace NYqlAstTrace {

namespace NDetail {

struct TBuildLimits {
    std::size_t MaxDepth = 48;
    std::size_t MaxChildrenPerNode = 64;
};

struct TLinkInfo {
    ui64 ExprId = 0;
    const NYql::TExprNode* Expr = nullptr;
    std::string Name;
    std::string GraphLabel;
    std::string TreeNodeId;
    std::string GraphNodeId;
};

struct TBuildState {
    explicit TBuildState(std::string rootId, TBuildLimits limits = {})
        : RootId(std::move(rootId))
        , Limits(limits)
    {
    }

    std::string NextId() {
        return RootId + "-n" + std::to_string(NextNodeIndex++);
    }

    std::string RootId;
    TBuildLimits Limits;
    std::size_t NextNodeIndex = 0;
    bool DepthBudgetExceeded = false;
    bool ChildBudgetExceeded = false;
    THashMap<ui64, THashSet<ui64>> ParentIdsByExprId;
    THashSet<ui64> ParentCollectionVisited;
    THashSet<ui64> LinkAssignmentVisited;
    THashMap<ui64, std::string> LinkNameByExprId;
    std::vector<TLinkInfo> Links;
};

inline std::string ToStdString(TStringBuf value) {
    return std::string(value.data(), value.size());
}

inline const char* NodeTypeName(NYql::TExprNode::EType type) {
    switch (type) {
        case NYql::TExprNode::List:
            return "List";
        case NYql::TExprNode::Atom:
            return "Atom";
        case NYql::TExprNode::Callable:
            return "Callable";
        case NYql::TExprNode::Lambda:
            return "Lambda";
        case NYql::TExprNode::Argument:
            return "Argument";
        case NYql::TExprNode::Arguments:
            return "Arguments";
        case NYql::TExprNode::World:
            return "World";
    }
    return "Unknown";
}

inline bool HasContent(const NYql::TExprNode& node) {
    return node.IsCallable() || node.IsAtom() || node.IsArgument();
}

inline std::string RawContent(const NYql::TExprNode& node) {
    if (!HasContent(node)) {
        return {};
    }
    if (node.Flags() & NYql::TNodeFlags::BinaryContent) {
        return "<binary>";
    }
    return ToStdString(node.Content());
}

inline bool NeedsQuoting(std::string_view value) {
    if (value.empty()) {
        return true;
    }

    for (const char ch : value) {
        if (ch == '"' || ch == '\\' || ch == '(' || ch == ')' || ch == '[' || ch == ']' ||
            ch == '{' || ch == '}' || ch == '\n' || ch == '\r' || ch == '\t' || ch == ' ') {
            return true;
        }
    }

    return false;
}

inline std::string Quote(std::string_view value) {
    std::string out;
    out.reserve(value.size() + 2);
    out.push_back('"');
    for (const char ch : value) {
        switch (ch) {
            case '\\':
                out += "\\\\";
                break;
            case '"':
                out += "\\\"";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out.push_back(ch);
                break;
        }
    }
    out.push_back('"');
    return out;
}

inline std::string TokenFromContent(const std::string& value) {
    std::string token = value;
    if (NeedsQuoting(token)) {
        token = Quote(token);
    }
    return token;
}

inline std::string ExprHead(const NYql::TExprNode& node) {
    if (node.IsCallable()) {
        return TokenFromContent(RawContent(node));
    }
    if (node.IsAtom()) {
        return TokenFromContent(RawContent(node));
    }
    if (node.IsArgument()) {
        return "$" + TokenFromContent(RawContent(node));
    }
    if (node.IsList()) {
        return "List";
    }
    if (node.IsLambda()) {
        return "Lambda";
    }
    if (node.IsArguments()) {
        return "Arguments";
    }
    if (node.IsWorld()) {
        return "World";
    }
    return NodeTypeName(node.Type());
}

inline std::string OperatorName(const NYql::TExprNode& node) {
    if (node.IsCallable()) {
        return TokenFromContent(RawContent(node));
    }
    return NodeTypeName(node.Type());
}

inline void AppendToken(std::string& line, const std::string& token) {
    if (!line.empty()) {
        line.push_back(' ');
    }
    line += token;
}

inline bool IsInlineToken(const NYql::TExprNode& expr) {
    return expr.IsAtom() || expr.IsArgument() || expr.IsWorld();
}

inline std::optional<std::string> TryInlineNodeLabel(const NYql::TExprNode& expr) {
    if (IsInlineToken(expr)) {
        return ExprHead(expr);
    }

    std::string line = ExprHead(expr);

    for (std::size_t i = 0; i < expr.ChildrenSize(); ++i) {
        const auto* child = expr.Child(i);
        if (!child) {
            continue;
        }

        if (!IsInlineToken(*child)) {
            return std::nullopt;
        }

        AppendToken(line, ExprHead(*child));
    }

    return line;
}

inline void CollectParents(const NYql::TExprNode& expr, std::optional<ui64> parentId, TBuildState& state) {
    const ui64 exprId = expr.UniqueId();
    if (parentId) {
        state.ParentIdsByExprId[exprId].insert(*parentId);
    }

    if (!state.ParentCollectionVisited.insert(exprId).second) {
        return;
    }

    for (std::size_t i = 0; i < expr.ChildrenSize(); ++i) {
        if (const auto* child = expr.Child(i)) {
            CollectParents(*child, exprId, state);
        }
    }
}

inline bool IsLinkedExpr(const NYql::TExprNode& expr, const TBuildState& state) {
    if (const auto it = state.ParentIdsByExprId.find(expr.UniqueId()); it != state.ParentIdsByExprId.end()) {
        return it->second.size() > 1 &&
            !expr.IsAtom() &&
            !expr.IsArgument() &&
            !expr.IsWorld() &&
            expr.ChildrenSize() > 1 &&
            !TryInlineNodeLabel(expr).has_value();
    }
    return false;
}

inline void AssignLinks(const NYql::TExprNode& expr, TBuildState& state) {
    const ui64 exprId = expr.UniqueId();
    if (!state.LinkAssignmentVisited.insert(exprId).second) {
        return;
    }

    if (IsLinkedExpr(expr, state) && !state.LinkNameByExprId.contains(exprId)) {
        const std::size_t linkIndex = state.Links.size() + 1;
        const std::string name = "Link #" + std::to_string(linkIndex);
        state.LinkNameByExprId.emplace(exprId, name);
        state.Links.push_back(TLinkInfo{
            .ExprId = exprId,
            .Expr = &expr,
            .Name = name,
            .GraphLabel = std::to_string(linkIndex),
            .TreeNodeId = state.RootId + "-link-" + std::to_string(exprId),
            .GraphNodeId = "link-" + std::to_string(linkIndex)
        });
    }

    for (std::size_t i = 0; i < expr.ChildrenSize(); ++i) {
        if (const auto* child = expr.Child(i)) {
            AssignLinks(*child, state);
        }
    }
}

inline void AddIndexField(optimizer_trace::Node& node, const std::optional<std::string>& index) {
    if (index) {
        node.field("Index", *index);
    }
}

inline optimizer_trace::Node BuildTruncatedNode(const std::string& id, const std::string& label, const std::optional<std::string>& index = std::nullopt) {
    optimizer_trace::Node node(id, "Truncated", label);
    AddIndexField(node, index);
    return node;
}

inline optimizer_trace::Node BuildLinkReferenceNode(
    const std::string& id,
    const std::string& linkName,
    const std::optional<std::string>& index)
{
    optimizer_trace::Node node(id, "Link", linkName);
    AddIndexField(node, index);
    return node;
}

inline optimizer_trace::Node BuildNode(
    const NYql::TExprNode& expr,
    const std::string& id,
    const std::optional<std::string>& index,
    std::size_t depth,
    TBuildState& state,
    std::optional<ui64> expandedLink = std::nullopt)
{
    if (depth >= state.Limits.MaxDepth) {
        state.DepthBudgetExceeded = true;
        return BuildTruncatedNode(id, "AST depth budget exceeded", index);
    }

    const ui64 uniqueId = expr.UniqueId();
    if (const auto it = state.LinkNameByExprId.find(uniqueId);
        it != state.LinkNameByExprId.end() && (!expandedLink || *expandedLink != uniqueId))
    {
        return BuildLinkReferenceNode(id, it->second, index);
    }

    std::string line = ExprHead(expr);
    std::vector<std::pair<std::string, const NYql::TExprNode*>> nestedChildren;

    for (std::size_t i = 0; i < expr.ChildrenSize(); ++i) {
        if (i >= state.Limits.MaxChildrenPerNode) {
            state.ChildBudgetExceeded = true;
            AppendToken(line, "...(" + std::to_string(expr.ChildrenSize() - i) + " children omitted)");
            break;
        }

        const auto* child = expr.Child(i);
        if (!child) {
            continue;
        }

        auto childLine = IsInlineToken(*child)
            ? std::optional<std::string>(ExprHead(*child))
            : std::optional<std::string>();

        if (childLine) {
            AppendToken(line, *childLine);
            continue;
        }

        const std::string placeholder = "$" + std::to_string(i);
        AppendToken(line, placeholder);
        nestedChildren.emplace_back(placeholder, child);
    }

    optimizer_trace::Node node(id, OperatorName(expr), line);
    AddIndexField(node, index);

    for (const auto& [placeholder, child] : nestedChildren) {
        node.child(BuildNode(*child, state.NextId(), placeholder, depth + 1, state, expandedLink));
    }

    return node;
}

inline void AddLinkNodes(optimizer_trace::Node& dagRoot, TBuildState& state) {
    for (const auto& link : state.Links) {
        if (!link.Expr) {
            continue;
        }

        optimizer_trace::Node linkNode(
            link.TreeNodeId,
            "Link",
            link.Name);
        linkNode.child(BuildNode(
            *link.Expr,
            state.NextId(),
            std::nullopt,
            0,
            state,
            link.ExprId));
        dagRoot.child(linkNode);
    }
}

inline void CollectReferencedLinks(
    const NYql::TExprNode& expr,
    const TBuildState& state,
    std::optional<ui64> expandedLink,
    THashSet<ui64>& refs)
{
    const ui64 exprId = expr.UniqueId();
    if (state.LinkNameByExprId.contains(exprId) && (!expandedLink || *expandedLink != exprId)) {
        refs.insert(exprId);
        return;
    }

    for (std::size_t i = 0; i < expr.ChildrenSize(); ++i) {
        if (const auto* child = expr.Child(i)) {
            CollectReferencedLinks(*child, state, expandedLink, refs);
        }
    }
}

inline void AddLinkGraphEdges(
    optimizer_trace::Graph& graph,
    const std::string& sourceGraphNodeId,
    const THashSet<ui64>& referencedLinks,
    const TBuildState& state)
{
    for (const auto& link : state.Links) {
        if (!referencedLinks.contains(link.ExprId)) {
            continue;
        }

        const auto target = optimizer_trace::Target::subtree(link.TreeNodeId);
        graph.edge(sourceGraphNodeId, link.GraphNodeId)
            .target(target)
            .primaryTarget(target);
    }
}

inline optimizer_trace::Widget BuildLinkGraphWidget(
    const std::vector<const NYql::TExprNode*>& rootExprs,
    const std::string& rootTreeNodeId,
    const TBuildState& state)
{
    optimizer_trace::Graph graph;
    graph.layout("LR", 70, 42).directed();

    const auto rootTarget = optimizer_trace::Target::subtree(rootTreeNodeId);
    graph.node("root", "Root")
        .target(rootTarget)
        .primaryTarget(rootTarget);

    for (const auto& link : state.Links) {
        if (!link.Expr) {
            continue;
        }

        const auto target = optimizer_trace::Target::subtree(link.TreeNodeId);
        graph.node(link.GraphNodeId, link.GraphLabel)
            .target(target)
            .primaryTarget(target);
    }

    THashSet<ui64> rootReferencedLinks;
    for (const auto* rootExpr : rootExprs) {
        if (rootExpr) {
            CollectReferencedLinks(*rootExpr, state, std::nullopt, rootReferencedLinks);
        }
    }
    AddLinkGraphEdges(graph, "root", rootReferencedLinks, state);

    for (const auto& link : state.Links) {
        if (link.Expr) {
            THashSet<ui64> referencedLinks;
            CollectReferencedLinks(*link.Expr, state, link.ExprId, referencedLinks);
            AddLinkGraphEdges(graph, link.GraphNodeId, referencedLinks, state);
        }
    }

    return optimizer_trace::Widget::graph("DAG links", graph).monospaceGraph();
}

} // namespace NDetail

struct TBuildResult {
    TBuildResult(optimizer_trace::Node root, std::optional<optimizer_trace::Widget> linkGraph = std::nullopt)
        : Root(std::move(root))
        , LinkGraph(std::move(linkGraph))
    {
    }

    optimizer_trace::Node Root;
    std::optional<optimizer_trace::Widget> LinkGraph;
};

inline TBuildResult BuildExprTreeWithInfo(const NYql::TExprNode::TPtr& expr, const std::string& rootId) {
    if (!expr) {
        return TBuildResult(optimizer_trace::Node(rootId, "Empty", "Empty"));
    }

    NDetail::TBuildState state(rootId);
    NDetail::CollectParents(*expr, std::nullopt, state);
    NDetail::AssignLinks(*expr, state);

    if (state.Links.empty()) {
        return TBuildResult(NDetail::BuildNode(*expr, rootId, std::nullopt, 0, state));
    }

    optimizer_trace::Node dagRoot(rootId, "DAG", "DAG");
    const std::string rootTreeNodeId = rootId + "-root";
    optimizer_trace::Node rootNode(rootTreeNodeId, "Root", "Root");
    rootNode.child(NDetail::BuildNode(*expr, state.NextId(), std::nullopt, 0, state));
    dagRoot.child(rootNode);
    NDetail::AddLinkNodes(dagRoot, state);
    return TBuildResult(
        std::move(dagRoot),
        NDetail::BuildLinkGraphWidget({expr.Get()}, rootTreeNodeId, state));
}

inline TBuildResult BuildExprListTreeWithInfo(
    const TVector<NYql::TExprNode::TPtr>& items,
    const std::string& rootId,
    const std::string& rootLabel,
    const std::string& itemLabel)
{
    NDetail::TBuildState state(rootId);
    for (std::size_t i = 0; i < items.size(); ++i) {
        if (items[i]) {
            const ui64 stageParentId = std::numeric_limits<ui64>::max() - i;
            NDetail::CollectParents(*items[i], stageParentId, state);
        }
    }
    for (const auto& item : items) {
        if (item) {
            NDetail::AssignLinks(*item, state);
        }
    }

    const std::string listRootId = state.Links.empty() ? rootId : rootId + "-list";
    optimizer_trace::Node root(listRootId, rootLabel, rootLabel);
    for (std::size_t i = 0; i < items.size(); ++i) {
        optimizer_trace::Node itemNode(
            listRootId + "-item-" + std::to_string(i),
            itemLabel,
            itemLabel + " " + std::to_string(i));
        itemNode.field("Index", std::to_string(i));
        if (items[i]) {
            itemNode.child(NDetail::BuildNode(
                *items[i],
                state.NextId(),
                std::nullopt,
                0,
                state));
        }
        root.child(itemNode);
    }

    if (state.Links.empty()) {
        return TBuildResult(std::move(root));
    }

    optimizer_trace::Node dagRoot(rootId, "DAG", "DAG");
    const std::string rootTreeNodeId = rootId + "-root";
    optimizer_trace::Node rootNode(rootTreeNodeId, "Root", "Root");
    rootNode.child(root);
    dagRoot.child(rootNode);
    NDetail::AddLinkNodes(dagRoot, state);
    std::vector<const NYql::TExprNode*> itemRoots;
    itemRoots.reserve(items.size());
    for (const auto& item : items) {
        if (item) {
            itemRoots.push_back(item.Get());
        }
    }
    return TBuildResult(
        std::move(dagRoot),
        NDetail::BuildLinkGraphWidget(itemRoots, rootTreeNodeId, state));
}

inline TBuildResult BuildStageListTreeWithInfo(const TVector<NYql::TExprNode::TPtr>& stages, const std::string& rootId) {
    return BuildExprListTreeWithInfo(stages, rootId, "Stages", "Stage");
}

inline optimizer_trace::Node BuildExprTree(const NYql::TExprNode::TPtr& expr, const std::string& rootId) {
    auto result = BuildExprTreeWithInfo(expr, rootId);
    return std::move(result.Root);
}

inline optimizer_trace::Node BuildStageListTree(const TVector<NYql::TExprNode::TPtr>& stages, const std::string& rootId) {
    auto result = BuildStageListTreeWithInfo(stages, rootId);
    return std::move(result.Root);
}

} // namespace NYqlAstTrace

} // namespace NKikimr::NKqp
