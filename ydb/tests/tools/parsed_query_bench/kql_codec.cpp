#include "kql_codec.h"
#include "stage_error.h"

#include <yql/essentials/core/yql_type_annotation.h>

#include <util/generic/yexception.h>

#include <algorithm>

namespace NKikimr::NKqp::NParsedQueryBench {
namespace {

using namespace NYql;
using TWire = Ydb::Query::KqlQueryList;

constexpr size_t MaxNodes = 200000;
constexpr size_t MaxDepth = 512;

void CheckQueryList(const TExprNode& root) {
    Y_ENSURE(root.IsList() && root.ChildrenSize(), "Expected a nonempty TKqlQueryList");
    for (const auto& block : root.Children()) {
        Y_ENSURE(block->IsList() && block->ChildrenSize() == 2 && block->Head().IsList()
            && block->Head().ChildrenSize() && block->Tail().IsList() && !block->Tail().ChildrenSize(),
            "KQL benchmark requires read-only query blocks with results");
        for (const auto& result : block->Head().Children()) {
            Y_ENSURE(result->IsList() && result->ChildrenSize() == 2 && result->Tail().IsList()
                && std::ranges::all_of(result->Tail().Children(), [](const auto& hint) { return hint->IsAtom(); }),
                "Invalid KQL result column hints");
        }
    }
}

TWire::Node::Kind WireKind(TExprNode::EType kind) {
    switch (kind) {
        case TExprNode::Atom: return TWire::Node::ATOM;
        case TExprNode::List: return TWire::Node::LIST;
        case TExprNode::Callable: return TWire::Node::CALLABLE;
        case TExprNode::Lambda: return TWire::Node::LAMBDA;
        case TExprNode::Argument: return TWire::Node::ARGUMENT;
        case TExprNode::Arguments: return TWire::Node::ARGUMENTS;
        case TExprNode::World: return TWire::Node::WORLD;
    }
    ythrow yexception() << "Unknown KQL node kind";
}

ui32 ExportNode(const TExprNode& node, TExprContext& ctx, TWire& output, TNodeMap<ui32>& ids, size_t depth) {
    Y_ENSURE(depth <= MaxDepth, "KQL export graph exceeds depth limit");
    if (const auto it = ids.find(&node); it != ids.end()) {
        return it->second;
    }
    TVector<ui32> children;
    children.reserve(node.ChildrenSize());
    for (const auto& child : node.Children()) {
        children.push_back(ExportNode(*child, ctx, output, ids, depth + 1));
    }
    Y_ENSURE(ids.size() < MaxNodes, "KQL graph exceeds node limit");
    auto& wire = *output.add_nodes();
    wire.set_kind(WireKind(node.Type()));
    if (node.IsAtom() || node.IsCallable() || node.IsArgument()) {
        wire.set_content(node.Content().data(), node.Content().size());
    }
    if (node.IsAtom()) {
        wire.set_atom_flags(node.Flags());
    }
    wire.mutable_children()->Assign(children.begin(), children.end());
    const auto& pos = ctx.GetPosition(node.Pos());
    wire.set_row(pos.Row);
    wire.set_column(pos.Column);
    wire.set_file(pos.File.data(), pos.File.size());
    const ui32 id = output.nodes_size();
    ids.emplace(&node, id);
    return id;
}

TExprNode::TPtr ImportNode(const TWire::Node& wire, TExprNode::TListType children, TExprContext& ctx) {
    const auto pos = ctx.AppendPosition(TPosition(wire.column(), wire.row(), wire.file()));
    Y_ENSURE_EX(wire.kind() == TWire::Node::ATOM || !wire.atom_flags(), TStageError() << "Unexpected KQL atom flags");
    Y_ENSURE_EX(wire.kind() == TWire::Node::ATOM || wire.kind() == TWire::Node::ARGUMENT || wire.kind() == TWire::Node::CALLABLE
        || wire.content().empty(), TStageError() << "Unexpected KQL node content");
    switch (wire.kind()) {
        case TWire::Node::ATOM:
            Y_ENSURE_EX(children.empty() && !(wire.atom_flags() & ~TNodeFlags::FlagsMask), TStageError() << "Invalid KQL atom");
            return ctx.NewAtom(pos, wire.content(), wire.atom_flags());
        case TWire::Node::LIST:
            return ctx.NewList(pos, std::move(children));
        case TWire::Node::CALLABLE:
            Y_ENSURE_EX(!wire.content().empty(), TStageError() << "Missing KQL callable name");
            return ctx.NewCallable(pos, wire.content(), std::move(children));
        case TWire::Node::LAMBDA:
            Y_ENSURE_EX(children.size() >= 2 && children.front()->IsArguments(), TStageError() << "Invalid KQL lambda");
            return ctx.NewLambda(pos, std::move(children));
        case TWire::Node::ARGUMENT:
            Y_ENSURE_EX(children.empty(), TStageError() << "Invalid KQL argument");
            return ctx.NewArgument(pos, wire.content());
        case TWire::Node::ARGUMENTS:
            Y_ENSURE_EX(std::ranges::all_of(children, [](const auto& arg) { return arg->IsArgument(); }), TStageError() << "Invalid KQL arguments");
            return ctx.NewArguments(pos, std::move(children));
        case TWire::Node::WORLD:
            Y_ENSURE_EX(children.empty(), TStageError() << "Invalid KQL world");
            return ctx.NewWorld(pos);
        default:
            ythrow TStageError() << "Unknown KQL node kind";
    }
}

} // namespace

void ExportKqlQueryList(const TExprNode& query, TExprContext& ctx, const TTypeAnnotationContext& types, TWire& output) {
    CheckQueryList(query);
    output.Clear();
    if (types.DeriveColumnOrder && !types.OrderedColumns) {
        output.set_format_version(1);
    } else {
        output.set_format_version(2);
        output.set_derive_column_order(types.DeriveColumnOrder);
        output.set_ordered_columns(types.OrderedColumns);
    }
    TNodeMap<ui32> ids;
    ExportNode(query, ctx, output, ids, 1);
}

TExprNode::TPtr ImportKqlQueryList(const TWire& input, TExprContext& ctx, TTypeAnnotationContext& types) {
    const auto count = static_cast<size_t>(input.nodes_size());
    const bool legacy = input.format_version() == 1;
    Y_ENSURE_EX(legacy || input.format_version() == 2, TStageError() << "Unsupported KQL graph version");
    if (legacy) {
        Y_ENSURE_EX(!input.has_derive_column_order() && !input.has_ordered_columns(), TStageError() << "Unexpected column-order settings in KQL version 1");
    } else {
        Y_ENSURE_EX(input.has_derive_column_order() && input.has_ordered_columns(), TStageError() << "Missing KQL column-order settings");
    }
    Y_ENSURE_EX(count, TStageError() << "Empty KQL graph");
    Y_ENSURE(count <= MaxNodes, "KQL graph exceeds node limit");
    TVector<TExprNode::TPtr> nodes;
    TVector<size_t> depths;
    TVector<bool> used(count, false);
    nodes.reserve(count);
    depths.reserve(count);
    for (const auto& node : input.nodes()) {
        TExprNode::TListType children;
        children.reserve(node.children_size());
        size_t depth = 1;
        for (const auto id : node.children()) {
            Y_ENSURE_EX(id && id <= nodes.size(), TStageError() << "Invalid or forward KQL child reference");
            children.push_back(nodes[id - 1]);
            depth = std::max(depth, depths[id - 1] + 1);
            used[id - 1] = true;
        }
        Y_ENSURE(depth <= MaxDepth, "KQL graph exceeds depth limit");
        depths.push_back(depth);
        nodes.push_back(ImportNode(node, std::move(children), ctx));
    }
    Y_ENSURE_EX(std::find(used.begin(), used.end() - 1, false) == used.end() - 1, TStageError() << "Orphan KQL node");
    auto root = std::move(nodes.back());
    CheckQueryList(*root);
    CheckArguments(*root);
    types.DeriveColumnOrder = legacy || input.derive_column_order();
    types.OrderedColumns = !legacy && input.ordered_columns();
    return root;
}

} // namespace NKikimr::NKqp::NParsedQueryBench
