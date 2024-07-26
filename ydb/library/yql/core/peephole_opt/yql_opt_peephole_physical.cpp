#include "yql_opt_peephole_physical.h"

#include <ydb/library/yql/core/peephole_opt/yql_opt_json_peephole_physical.h>
#include <ydb/library/yql/core/yql_atom_enums.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_constraint.h>
#include <ydb/library/yql/core/yql_opt_range.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_opt_window.h>
#include <ydb/library/yql/core/yql_opt_match_recognize.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/common_opt/yql_co_transformer.h>
#include <ydb/library/yql/core/yql_gc_transformer.h>
#include <ydb/library/yql/core/yql_expr_csee.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/yql_aggregate_expander.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/core/services/yql_out_transformers.h>
#include <ydb/library/yql/utils/yql_paths.h>

#include <util/generic/xrange.h>

#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/yson/writer.h>

namespace NYql {

namespace {

using namespace NNodes;

using TPeepHoleOptimizerPtr = TExprNode::TPtr (*const)(const TExprNode::TPtr&, TExprContext&);
using TPeepHoleOptimizerMap = std::unordered_map<std::string_view, TPeepHoleOptimizerPtr>;

using TExtPeepHoleOptimizerPtr = TExprNode::TPtr (*const)(const TExprNode::TPtr&, TExprContext&, TTypeAnnotationContext& types);
using TExtPeepHoleOptimizerMap = std::unordered_map<std::string_view, TExtPeepHoleOptimizerPtr>;

struct TBlockFuncRule {
    std::string_view Name;
};

using TBlockFuncMap = std::unordered_map<std::string_view, TBlockFuncRule>;

TExprNode::TPtr MakeNothing(TPositionHandle pos, const TTypeAnnotationNode& type, TExprContext& ctx) {
    return ctx.NewCallable(pos, "Nothing", {ExpandType(pos, *ctx.MakeType<TOptionalExprType>(&type), ctx)});
}

std::string_view ToLiteral(const TGUID& uuid) { return std::string_view(reinterpret_cast<const char*>(&uuid), sizeof(uuid)); }
template <typename T> TString ToLiteral(const T value) { return ToString<T>(value); }

template <typename TRandomType>
TExprNode::TPtr Random0Arg(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    if (node->ChildrenSize() == 0U) {
        YQL_CLOG(DEBUG, CorePeepHole) << "0-arg " << node->Content();
        const auto random = types.GetCachedRandom<TRandomType>();
        return ctx.NewCallable(node->Pos(), node->GetTypeAnn()->Cast<TDataExprType>()->GetName(), { ctx.NewAtom(node->Pos(), ToLiteral(random)) });
    }
    return node;
}

template <ui64(*Convert)(ui64)>
TExprNode::TPtr Now0Arg(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    if (node->ChildrenSize() == 0U) {
        YQL_CLOG(DEBUG, CorePeepHole) << "0-arg " << node->Content();
        const auto now = types.GetCachedNow();
        return ctx.NewCallable(node->Pos(), node->GetTypeAnn()->Cast<TDataExprType>()->GetName(), { ctx.NewAtom(node->Pos(), ToString(bool(Convert) ? Convert(now) : now), TNodeFlags::Default) });
    }
    return node;
}

bool IsArgumentsOnlyLambda(const TExprNode& lambda, TVector<ui32>& argIndices) {
    TNodeMap<ui32> args;
    for (ui32 i = 0; i < lambda.Head().ChildrenSize(); ++i) {
        args.insert(std::make_pair(lambda.Head().Child(i), i));
    }

    for (ui32 i = 1; i < lambda.ChildrenSize(); ++i) {
        auto root = lambda.Child(i);
        if (!root->IsArgument()) {
            return false;
        }

        auto it = args.find(root);
        if (it == args.end()) {
            return false;
        }

        argIndices.push_back(it->second);
    }

    return true;
}

TExprNode::TPtr RebuildArgumentsOnlyLambdaForBlocks(const TExprNode& lambda, TExprContext& ctx, TTypeAnnotationContext& types) {
    TVector<const TTypeAnnotationNode*> argTypes;
    for (auto arg : lambda.Head().ChildrenList()) {
        argTypes.emplace_back(arg->GetTypeAnn());
    }

    YQL_ENSURE(types.ArrowResolver);
    auto resolveStatus = types.ArrowResolver->AreTypesSupported(ctx.GetPosition(lambda.Pos()), argTypes, ctx);
    YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
    if (resolveStatus != IArrowResolver::OK) {
        return {};
    }

    TVector<ui32> argIndicies;
    if (!IsArgumentsOnlyLambda(lambda, argIndicies)) {
        return {};
    }

    TExprNode::TListType newArgs, newRoots;
    for (ui32 i = 0; i < lambda.Head().ChildrenSize(); ++i) {
        newArgs.push_back(ctx.NewArgument(lambda.Head().Child(i)->Pos(), "arg" + ToString(i)));
    }

    newArgs.push_back(ctx.NewArgument(lambda.Pos(), "len"));
    for (const auto i : argIndicies) {
        newRoots.push_back(newArgs[i]);
    }

    newRoots.push_back(newArgs.back());
    return ctx.NewLambda(lambda.Pos(), ctx.NewArguments(lambda.Pos(), std::move(newArgs)), std::move(newRoots));
}

TExprNode::TPtr OptimizeWideToBlocks(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    Y_UNUSED(types);
    if (node->Head().IsCallable("WideFromBlocks")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Drop " << node->Content() << " over " << node->Head().Content();
        return ctx.NewCallable(node->Pos(), "ReplicateScalars", { node->Head().HeadPtr() });
    }

    if (const auto& input = node->Head(); input.IsCallable({"Extend", "OrderedExtend"})) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << input.Content();
        TExprNodeList newChildren;
        newChildren.reserve(input.ChildrenSize());
        for (auto& child : input.ChildrenList()) {
            newChildren.emplace_back(ctx.ChangeChild(*node, 0, std::move(child)));
        }
        return ctx.NewCallable(input.Pos(), input.IsCallable("Extend") ? "BlockExtend" : "BlockOrderedExtend", std::move(newChildren));
    }

    return node;
}

TExprNode::TPtr OptimizeWideFromBlocks(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    Y_UNUSED(types);
    if (node->Head().IsCallable("WideToBlocks")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Drop " << node->Content() << " over " << node->Head().Content();
        return node->Head().HeadPtr();
    }

    if (node->Head().IsCallable("ReplicateScalars")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Drop " << node->Head().Content() << " as input of " << node->Content();
        return ctx.ChangeChild(*node, 0, node->Head().HeadPtr());
    }

    return node;
}

TExprNode::TPtr OptimizeWideTakeSkipBlocks(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    Y_UNUSED(types);
    if (node->Head().IsCallable("ReplicateScalars")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << node->Head().Content();
        return ctx.SwapWithHead(*node);
    }

    return node;
}

TExprNode::TPtr OptimizeBlockCompress(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    Y_UNUSED(types);
    if (node->Head().IsCallable("ReplicateScalars")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << node->Head().Content();
        if (node->Head().ChildrenSize() == 1) {
            return ctx.SwapWithHead(*node);
        }

        const ui32 compressIndex = FromString<ui32>(node->Child(1)->Content());
        TExprNodeList newReplicateIndexes;
        for (auto atom : node->Head().Child(1)->ChildrenList()) {
            ui32 idx = FromString<ui32>(atom->Content());
            if (idx != compressIndex) {
                newReplicateIndexes.push_back((idx < compressIndex) ? atom : ctx.NewAtom(atom->Pos(), idx - 1));
            }
        }
        return ctx.Builder(node->Pos())
            .Callable("ReplicateScalars")
                .Add(0, ctx.ChangeChild(*node, 0, node->Head().HeadPtr()))
                .Add(1, ctx.NewList(node->Head().Child(1)->Pos(), std::move(newReplicateIndexes)))
            .Seal()
            .Build();
    }

    return node;
}

TExprNode::TPtr OptimizeBlocksTopOrSort(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    Y_UNUSED(types);
    if (node->Head().IsCallable("ReplicateScalars")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << node->Head().Content();
        return ctx.SwapWithHead(*node);
    }

    return node;
}

TExprNode::TPtr OptimizeBlockExtend(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    Y_UNUSED(types);
    TExprNodeList inputs = node->ChildrenList();
    bool hasReplicateScalars = false;
    for (auto& input : inputs) {
        if (input->IsCallable("ReplicateScalars")) {
            hasReplicateScalars = true;
            input = input->HeadPtr();
        }
    }

    if (hasReplicateScalars) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Drop ReplicateScalars as input of " << node->Content();
        return ctx.ChangeChildren(*node, std::move(inputs));
    }

    return node;
}

TExprNode::TPtr OptimizeReplicateScalars(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    Y_UNUSED(types);
    if (node->Head().IsCallable("ReplicateScalars")) {
        if (node->ChildrenSize() == 1) {
            YQL_CLOG(DEBUG, CorePeepHole) << "Drop " << node->Head().Content() << " as input of " << node->Content();
            return ctx.ChangeChild(*node, 0, node->Head().HeadPtr());
        }

        // child ReplicateScalar should also have indexes
        YQL_ENSURE(node->Head().ChildrenSize() == 2);

        TSet<ui32> mergedIndexes;
        for (auto atom : node->Child(1)->ChildrenList()) {
            mergedIndexes.insert(FromString<ui32>(atom->Content()));
        }
        for (auto atom : node->Head().Child(1)->ChildrenList()) {
            mergedIndexes.insert(FromString<ui32>(atom->Content()));
        }

        TExprNodeList newIndexes;
        for (auto& i : mergedIndexes) {
            newIndexes.push_back(ctx.NewAtom(node->Child(1)->Pos(), i));
        }

        YQL_CLOG(DEBUG, CorePeepHole) << "Merge nested " << node->Content();
        return ctx.ChangeChildren(*node, { node->Head().HeadPtr(), ctx.NewList(node->Child(1)->Pos(), std::move(newIndexes)) });
    }

    return node;
}

TExprNode::TPtr ExpandBlockExtend(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    Y_UNUSED(types);
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();

    TExprNodeList newChildren;
    newChildren.reserve(node->ChildrenSize());
    bool seenScalars = false;
    for (auto& child : node->ChildrenList()) {
        const auto& items = child->GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetItems();
        const ui32 width = items.size();
        YQL_ENSURE(width > 0);

        const bool hasScalars = AnyOf(items.begin(), items.end() - 1, [](const auto& item) { return item->IsScalar(); });
        seenScalars = seenScalars || hasScalars;
        newChildren.push_back(ctx.WrapByCallableIf(hasScalars, "ReplicateScalars", std::move(child)));
    }

    const TStringBuf newName = node->IsCallable("BlockOrdredExtend") ? "OrdredExtend" : "Extend";
    if (!seenScalars) {
        return ctx.RenameNode(*node, newName);
    }
    return ctx.NewCallable(node->Pos(), newName, std::move(newChildren));
}

TExprNode::TPtr ExpandReplicateScalars(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    Y_UNUSED(types);
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();
    const auto& items = node->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetItems();
    const ui32 width = items.size();

    TExprNodeList args;
    TExprNodeList bodyItems;

    args.reserve(width);
    bodyItems.reserve(width);
    auto lastColumn = ctx.NewArgument(node->Pos(), "height");

    TMaybe<THashSet<ui32>> replicateIndexes;
    if (node->ChildrenSize() == 2) {
        replicateIndexes.ConstructInPlace();
        for (auto atom : node->Head().Child(1)->ChildrenList()) {
            replicateIndexes->insert(FromString<ui32>(atom->Content()));
        }
    }

    for (ui32 i = 0; i < width; ++i) {
        auto arg = (i + 1 == width) ? lastColumn : ctx.NewArgument(node->Pos(), "arg");
        args.push_back(arg);

        if (i + 1 == width || items[i]->IsBlock()) {
            bodyItems.push_back(arg);
        } else {
            YQL_ENSURE(items[i]->IsScalar());
            bool doReplicate = !replicateIndexes.Defined() || replicateIndexes->contains(i);
            bodyItems.push_back(doReplicate ? ctx.NewCallable(node->Pos(), "ReplicateScalar", { arg, lastColumn}) : arg);
        }
    }

    return ctx.Builder(node->Pos())
        .Callable("WideMap")
            .Add(0, node->HeadPtr())
            .Add(1, ctx.NewLambda(node->Pos(), ctx.NewArguments(node->Pos(), std::move(args)), std::move(bodyItems)))
        .Seal()
        .Build();
}

TExprNode::TPtr SplitEquiJoinToPairsRecursive(const TExprNode& node, const TExprNode& joinTree, TExprContext& ctx,
    std::vector<std::string_view>& outLabels, const TExprNode::TPtr& settings) {
    const auto leftSubtree = joinTree.Child(1);
    const auto rightSubtree = joinTree.Child(2);
    TExprNode::TPtr leftList;
    std::vector<std::string_view> leftListLabels;
    TExprNode::TPtr leftLabelsNode;
    if (leftSubtree->IsAtom()) {
        TExprNode::TPtr listNode;
        for (ui32 i = 0; !listNode && i < node.ChildrenSize() - 1; ++i) {
            if (node.Child(i)->Child(1)->IsAtom()) {
                if (node.Child(i)->Child(1)->Content() == leftSubtree->Content()) {
                    listNode = node.Child(i)->HeadPtr();
                    leftLabelsNode = node.Child(i)->ChildPtr(1);
                    leftListLabels.push_back(leftSubtree->Content());
                    break;
                }
            } else {
                for (auto& child : node.Child(i)->Child(1)->Children()) {
                    if (child->Content() == leftSubtree->Content()) {
                        listNode = node.Child(i)->HeadPtr();
                        leftLabelsNode = node.Child(i)->ChildPtr(1);
                        break;
                    }
                }

                if (listNode) {
                    for (auto& child : node.Child(i)->Child(1)->Children()) {
                        leftListLabels.push_back(child->Content());
                    }
                }
            }
        }

        YQL_ENSURE(listNode);
        leftList = listNode;
    }
    else {
        leftList = SplitEquiJoinToPairsRecursive(node, *leftSubtree, ctx, leftListLabels, ctx.NewList(node.Pos(), {}));
        TExprNode::TListType leftLabelsChildren;
        for (auto& label : leftListLabels) {
            leftLabelsChildren.push_back(ctx.NewAtom(node.Pos(), label));
        }

        leftLabelsNode = ctx.NewList(node.Pos(), std::move(leftLabelsChildren));
    }

    TExprNode::TPtr rightList;
    std::vector<std::string_view> rightListLabels;
    TExprNode::TPtr rightLabelsNode;
    if (rightSubtree->IsAtom()) {
        TExprNode::TPtr listNode;
        for (ui32 i = 0; !listNode && i < node.ChildrenSize() - 1; ++i) {
            if (node.Child(i)->Child(1)->IsAtom()) {
                if (node.Child(i)->Child(1)->Content() == rightSubtree->Content()) {
                    listNode = node.Child(i)->HeadPtr();
                    rightLabelsNode = node.Child(i)->ChildPtr(1);
                    rightListLabels.push_back(rightSubtree->Content());
                    break;
                }
            }
            else {
                for (auto& child : node.Child(i)->Child(1)->Children()) {
                    if (child->Content() == rightSubtree->Content()) {
                        listNode = node.Child(i)->HeadPtr();
                        rightLabelsNode = node.Child(i)->ChildPtr(1);
                        break;
                    }
                }

                if (listNode) {
                    for (auto& child : node.Child(i)->Child(1)->Children()) {
                        rightListLabels.push_back(child->Content());
                    }
                }
            }
        }

        YQL_ENSURE(listNode);
        rightList = listNode;
    }
    else {
        rightList = SplitEquiJoinToPairsRecursive(node, *rightSubtree, ctx, rightListLabels, ctx.NewList(node.Pos(), {}));
        TExprNode::TListType rightLabelsChildren;
        for (auto& label : rightListLabels) {
            rightLabelsChildren.push_back(ctx.NewAtom(node.Pos(), label));
        }

        rightLabelsNode = ctx.NewList(node.Pos(), std::move(rightLabelsChildren));
    }

    outLabels.insert(outLabels.end(), leftListLabels.begin(), leftListLabels.end());
    outLabels.insert(outLabels.end(), rightListLabels.begin(), rightListLabels.end());

    auto result = ctx.Builder(node.Pos())
        .Callable(node.Content())
            .List(0)
                .Add(0, leftList)
                .Add(1, leftLabelsNode)
            .Seal()
            .List(1)
                .Add(0, rightList)
                .Add(1, rightLabelsNode)
            .Seal()
            .List(2)
                .Add(0, joinTree.HeadPtr())
                .Atom(1, leftListLabels.front())
                .Atom(2, rightListLabels.front())
                .Add(3, joinTree.ChildPtr(3))
                .Add(4, joinTree.ChildPtr(4))
                .Add(5, joinTree.ChildPtr(5))
            .Seal()
            .Add(3, settings)
        .Seal()
        .Build();
    return result;
}

TExprNode::TPtr SplitEquiJoinToPairs(const TExprNode& join, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Split " << join.Content() << " to pairs.";
    const auto joinTree = join.Child(join.ChildrenSize() - 2U);
    const auto joinSettings = join.TailPtr();
    std::vector<std::string_view> outLabels;
    return SplitEquiJoinToPairsRecursive(join, *joinTree, ctx, outLabels, joinSettings);
}

std::optional<std::string_view> CutAlias(const std::string_view& alias, const std::string_view& column) {
    if (!alias.empty() && column.starts_with(alias) && column.length() > alias.length() && '.' == column[alias.length()])
        return column.substr(alias.length() + 1U);
    return std::nullopt;
}

std::vector<std::tuple<TExprNode::TPtr, bool, TExprNode::TPtr>> GetRenames(const TExprNode& join, TExprContext& ctx) {
    std::unordered_map<std::string_view, std::array<TExprNode::TPtr, 2U>> renames(join.Tail().ChildrenSize());
    join.Tail().ForEachChild([&](const TExprNode& child) {
        if (child.Head().Content() == "rename" && !child.Child(2)->Content().empty())
            renames.emplace(child.Child(2)->Content(), std::array<TExprNode::TPtr, 2U>{child.ChildPtr(1), child.ChildPtr(2)});
    });

    const auto& lhs = join.Head();
    const auto& rhs = *join.Child(1);

    const std::string_view lAlias = lhs.Tail().IsAtom() ? lhs.Tail().Content() : "";
    const std::string_view rAlias = rhs.Tail().IsAtom() ? rhs.Tail().Content() : "";

    const auto lType = lhs.Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    const auto rType = rhs.Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    const auto oType = join.GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    std::vector<std::tuple<TExprNode::TPtr, bool, TExprNode::TPtr>> result;
    result.reserve(oType->GetSize());
    for (const auto& item : oType->GetItems()) {
        const auto& name = item->GetName();
        if (const auto it = renames.find(name); renames.cend() != it) {
            const auto& source = it->second.front()->Content();
            if (const auto& part = CutAlias(lAlias, source)) {
                if (const auto itemType = lType->FindItemType(*part))
                    result.emplace_back(ctx.NewAtom(join.Pos(), *part), true, std::move(it->second.back()));
            } else if (const auto& part = CutAlias(rAlias, source)) {
                if (const auto itemType = rType->FindItemType(*part))
                    result.emplace_back(ctx.NewAtom(join.Pos(), *part), false, std::move(it->second.back()));
            } else if (const auto itemType = lType->FindItemType(source)) {
                result.emplace_back(std::move(it->second.front()), true, std::move(it->second.back()));
            } else if (const auto itemType = rType->FindItemType(source)) {
                result.emplace_back(std::move(it->second.front()), false, std::move(it->second.back()));
            }
        } else {
            auto pass = ctx.NewAtom(join.Pos(), name);
            if (const auto& part = CutAlias(lAlias, name)) {
                if (const auto itemType = lType->FindItemType(*part))
                    result.emplace_back(ctx.NewAtom(join.Pos(), *part), true, std::move(pass));
            } else if (const auto& part = CutAlias(rAlias, name)) {
                if (const auto itemType = rType->FindItemType(*part))
                    result.emplace_back(ctx.NewAtom(join.Pos(), *part), false, std::move(pass));
            } else if (const auto itemType = lType->FindItemType(name)) {
                result.emplace_back(pass, true, pass);
            } else if (const auto itemType = rType->FindItemType(name)) {
                result.emplace_back(pass, false, pass);
            }
        }
    }
    return result;
}

void GetKeys(const TJoinLabels& joinLabels, const TExprNode& keys, TExprContext& ctx,
    TExprNode::TListType& result, TVector<ui32>& inputs) {
    result.reserve(keys.ChildrenSize() >> 1U);
    inputs.reserve(keys.ChildrenSize() >> 1U);

    for (auto i = 0U; i < keys.ChildrenSize(); ++i) {
        auto alias = keys.Child(i++)->Content();
        auto name = keys.Child(i)->Content();
        auto inputIndex = joinLabels.FindInputIndex(alias);
        YQL_ENSURE(inputIndex);
        const auto& input = joinLabels.Inputs[*inputIndex];
        auto memberName = input.MemberName(alias, name);
        result.push_back(ctx.NewAtom(keys.Pos(), memberName));
        inputs.push_back(*inputIndex);
    }
}

TExprNode::TPtr ExpandEquiJoinImpl(const TExprNode& node, TExprContext& ctx) {
    if (node.ChildrenSize() > 4U) {
        return SplitEquiJoinToPairs(node, ctx);
    }

    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node.Content();

    auto list1 = node.Head().HeadPtr();
    auto list2 = node.Child(1)->HeadPtr();

    const auto& renames = GetRenames(node, ctx);
    const auto& joinKind = node.Child(2)->Head().Content();
    if (joinKind == "Cross") {
        auto result = ctx.Builder(node.Pos())
            .Callable("FlatMap")
                .Add(0, std::move(list1))
                .Lambda(1)
                    .Param("left")
                    .Callable("Map")
                        .Add(0, std::move(list2))
                        .Lambda(1)
                            .Param("right")
                            .Callable("AsStruct")
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    ui32 pos = 0;
                                    for (auto& item : renames) {
                                        parent.List(pos++)
                                            .Add(0, std::move(std::get<2>(item)))
                                            .Callable(1, "Member")
                                                .Arg(0, std::get<bool>(item) ? "left" : "right")
                                                .Add(1, std::move(std::get<0>(item)))
                                            .Seal()
                                        .Seal();
                                    }
                                    return parent;
                                })
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal().Build();

        if (const auto iterator = FindNode(result->Tail().Tail().HeadPtr(),
            [] (const TExprNode::TPtr& node) { return node->IsCallable("Iterator"); })) {
            auto children = iterator->ChildrenList();
            children.emplace_back(ctx.NewCallable(iterator->Pos(), "DependsOn", {result->Tail().Head().HeadPtr()}));
            result = ctx.ReplaceNode(std::move(result), *iterator, ctx.ChangeChildren(*iterator, std::move(children)));
        }

        if (const auto forward = FindNode(result->Tail().Tail().HeadPtr(),
            [] (const TExprNode::TPtr& node) { return node->IsCallable("ForwardList"); })) {
            result = ctx.ReplaceNode(std::move(result), *forward, ctx.RenameNode(*forward, "Collect"));
        }

        return result;
    }

    const auto list1type = list1->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    const auto list2type = list2->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    TJoinLabels joinLabels;
    if (auto issue = joinLabels.Add(ctx, node.Child(0)->Tail(), list1type)) {
        MKQL_ENSURE(false, issue->ToString());
    }

    if (auto issue = joinLabels.Add(ctx, node.Child(1)->Tail(), list2type)) {
        MKQL_ENSURE(false, issue->ToString());
    }

    TExprNode::TListType keyMembers1;
    TVector<ui32> keyMembers1Inputs;
    GetKeys(joinLabels, *node.Child(2)->Child(3), ctx, keyMembers1, keyMembers1Inputs);
    TExprNode::TListType keyMembers2;
    TVector<ui32> keyMembers2Inputs;
    GetKeys(joinLabels, *node.Child(2)->Child(4), ctx, keyMembers2, keyMembers2Inputs);
    std::vector<std::string_view> lKeys(keyMembers1.size()), rKeys(keyMembers2.size());

    MKQL_ENSURE(keyMembers1.size() == keyMembers2.size(), "Expected same key sizes.");
    for (ui32 i = 0; i < keyMembers1.size(); ++i) {
        if (keyMembers1Inputs[i] != 0) {
            std::swap(keyMembers1[i], keyMembers2[i]);
        }
    }

    bool optKey = false, badKey = false;
    const bool filter = joinKind == "Inner" || joinKind.ends_with("Semi");
    const bool leftKind = joinKind.starts_with("Left");
    const bool rightKind = joinKind.starts_with("Right");
    TTypeAnnotationNode::TListType keyTypeItems;
    keyTypeItems.reserve(keyMembers1.size());
    for (auto i = 0U; i < keyMembers2.size() && !badKey; ++i) {
        const auto keyType1 = list1type->FindItemType(lKeys[i] = keyMembers1[i]->Content());
        const auto keyType2 = list2type->FindItemType(rKeys[i] = keyMembers2[i]->Content());
        if (leftKind) {
            keyTypeItems.emplace_back(JoinDryKeyType(keyType1, keyType2, optKey, ctx));
        } else if (rightKind){
            keyTypeItems.emplace_back(JoinDryKeyType(keyType2, keyType1, optKey, ctx));
        } else {
            keyTypeItems.emplace_back(CommonType<true>(node.Pos(), DryType(keyType1, optKey, ctx), DryType(keyType2, optKey, ctx), ctx));
            optKey = optKey && !filter;
        }
        badKey = !keyTypeItems.back();
    }

    if (badKey) {
        if (filter)
            return ctx.NewCallable(node.Pos(), "List", {ExpandType(node.Pos(), *node.GetTypeAnn(), ctx)});

        lKeys.clear();
        rKeys.clear();
        keyTypeItems.clear();
        keyMembers1.clear();
        keyMembers2.clear();
        keyMembers1.emplace_back(MakeBool<true>(node.Pos(), ctx));
        keyMembers2.emplace_back(MakeBool<false>(node.Pos(), ctx));
    }

    const bool filter1 = filter || rightKind;
    const bool filter2 = filter || leftKind;

    const auto linkSettings = GetEquiJoinLinkSettings(*node.Child(2)->Child(5));

    const auto lUnique = list1->GetConstraint<TUniqueConstraintNode>();
    const auto rUnique = list2->GetConstraint<TUniqueConstraintNode>();

    const bool uniqueLeft  = lUnique && lUnique->ContainsCompleteSet(lKeys) || linkSettings.LeftHints.contains("unique") || linkSettings.LeftHints.contains("any");
    const bool uniqueRight = rUnique && rUnique->ContainsCompleteSet(rKeys) || linkSettings.RightHints.contains("unique") || linkSettings.RightHints.contains("any");

    TExprNode::TListType flags;
    if (uniqueLeft)
        flags.emplace_back(ctx.NewAtom(node.Pos(), "LeftUnique", TNodeFlags::Default));
    if (uniqueRight)
        flags.emplace_back(ctx.NewAtom(node.Pos(), "RightUnique", TNodeFlags::Default));

    TExprNode::TListType payloads1, payloads2;
    for (const auto& rename : renames) {
        (std::get<bool>(rename) ? payloads1 : payloads2).emplace_back(std::get<0>(rename));
    }

    const bool payload1 = joinKind != "RightOnly" && joinKind != "RightSemi";
    const bool payload2 = joinKind != "LeftOnly" && joinKind != "LeftSemi";

    const bool multi1 = payload1 && !uniqueLeft;
    const bool multi2 = payload2 && !uniqueRight;

    list1 = PrepareListForJoin(std::move(list1), keyTypeItems, keyMembers1, std::move(payloads1), payload1, optKey, filter1, ctx);
    list2 = PrepareListForJoin(std::move(list2), keyTypeItems, keyMembers2, std::move(payloads2), payload2, optKey, filter2, ctx);

    return ctx.Builder(node.Pos())
        .Callable("Map")
            .Callable(0, "JoinDict")
                .Add(0, MakeDictForJoin(std::move(list1), payload1, multi1, ctx))
                .Add(1, MakeDictForJoin(std::move(list2), payload2, multi2, ctx))
                .Add(2, node.Child(2)->HeadPtr())
                .List(3).Add(std::move(flags)).Seal()
            .Seal()
            .Lambda(1)
                .Param("row")
                .Callable("AsStruct")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 pos = 0;
                        if (payload1 && payload2)
                            for (auto& item : renames) {
                                parent.List(pos++)
                                    .Add(0, std::move(std::get<2>(item)))
                                    .Callable(1, "Member")
                                        .Callable(0, "Nth")
                                            .Arg(0, "row")
                                            .Atom(1, std::get<bool>(item) ? 0 : 1)
                                        .Seal()
                                        .Add(1, std::move(std::get<0>(item)))
                                    .Seal()
                                .Seal();
                            }
                        else
                            for (auto& item : renames) {
                                parent.List(pos++)
                                    .Add(0, std::move(std::get<2>(item)))
                                    .Callable(1, "Member")
                                        .Arg(0, "row")
                                        .Add(1, std::move(std::get<0>(item)))
                                    .Seal()
                                .Seal();
                            }
                        return parent;
                    })
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr PeepHoleConvertGroupBySingleKey(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto keySelectorLambda = node->Child(1);
    if (keySelectorLambda->Tail().GetDependencyScope()->second == keySelectorLambda) {
        return node;
    }

    YQL_CLOG(DEBUG, CorePeepHole) << "Convert " << node->Content() << " single key";
    const auto handlerLambda = node->Child(2);
    const auto& keyArg = handlerLambda->Head().Head();
    const auto& listArg = handlerLambda->Head().Tail();
    auto ret = ctx.ReplaceNodes(handlerLambda->TailPtr(), {{&keyArg, keySelectorLambda->TailPtr()}, {&listArg, node->HeadPtr()}});
    return ctx.WrapByCallableIf(handlerLambda->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional, "ToList", std::move(ret));
}

TExprNode::TPtr PeepHolePlainKeyForPartitionByKey(const TExprNode::TPtr& node, TExprContext& ctx) {
    auto keySelectorLambda = node->Child(1);
    bool needPickle = false;
    if (RemoveOptionalType(keySelectorLambda->GetTypeAnn())->GetKind() == ETypeAnnotationKind::Data) {
        // ok
    } else if (keySelectorLambda->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
        auto tupleType = keySelectorLambda->GetTypeAnn()->Cast<TTupleExprType>();
        if (tupleType->GetSize() < 2) {
            needPickle = true;
        } else {
            for (const auto& item : tupleType->GetItems()) {
                if (RemoveOptionalType(item)->GetKind() != ETypeAnnotationKind::Data) {
                    needPickle = true;
                    break;
                }
            }
        }
    } else {
        needPickle = true;
    }

    if (!needPickle) {
        return node;
    }

    YQL_CLOG(DEBUG, CorePeepHole) << "Plain key for " << node->Content();
    return ctx.Builder(node->Pos())
        .Callable(node->Content())
            .Add(0, node->HeadPtr())
            .Lambda(1)
                .Param("item")
                .Callable("StablePickle")
                    .Apply(0, *node->Child(1)).With(0, "item").Seal()
                .Seal()
            .Seal()
            .Add(2, node->ChildPtr(2))
            .Add(3, node->ChildPtr(3))
            .Lambda(4)
                .Param("list")
                .Apply(node->Tail())
                    .With(0)
                        .Callable("OrderedMap")
                            .Arg(0, "list")
                            .Lambda(1)
                                .Param("item")
                                .List()
                                    .Callable(0, "Unpickle")
                                        .Add(0, ExpandType(keySelectorLambda->Pos(), *keySelectorLambda->GetTypeAnn(), ctx))
                                        .Callable(1, "Nth")
                                            .Arg(0, "item")
                                            .Atom(1, 0)
                                        .Seal()
                                    .Seal()
                                    .Callable(1, "Nth")
                                        .Arg(0, "item")
                                        .Atom(1, 1)
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr PeepHoleExpandExtractItems(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();

    const TCoExtractMembers extract(node);
    const auto& input = extract.Input();
    const auto& members = extract.Members();
    auto arg = ctx.NewArgument(extract.Pos(), "arg");
    TExprNode::TListType fields;
    for (const auto& x : members) {
        fields.emplace_back(ctx.Builder(extract.Pos())
            .List()
                .Atom(0, x.Value())
                .Callable(1, "Member")
                    .Add(0, arg)
                    .Atom(1, x.Value())
                .Seal()
            .Seal()
            .Build());
    }

    auto body = ctx.NewCallable(extract.Pos(), "AsStruct", std::move(fields));
    auto lambda = ctx.NewLambda(extract.Pos(), ctx.NewArguments(extract.Pos(), { std::move(arg) }), std::move(body));
    const bool ordered = input.Ref().GetConstraint<TSortedConstraintNode>();
    return ctx.Builder(extract.Pos())
        .Callable(ordered ? "OrderedMap" : "Map")
            .Add(0, input.Ptr())
            .Add(1, std::move(lambda))
        .Seal()
        .Build();
}

TExprNode::TPtr PeepHoleDictFromKeysToDict(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();

    const TCoDictFromKeys callable(node);
    const auto itemTypeAnnotation = callable.Type().Ptr()->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
    auto list = ctx.Builder(callable.Pos())
        .Callable("List")
            .Callable(0, "ListType")
                .Add(0, ExpandType(callable.Pos(), *itemTypeAnnotation, ctx))
            .Seal()
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                ui32 pos = 0;
                for (auto child : callable.Keys().Ptr()->Children()) {
                    parent.Add(++pos, child);
                }
                return parent;
            })
        .Seal()
        .Build();

    return Build<TCoToDict>(ctx, callable.Pos())
        .List(list)
        .KeySelector()
            .Args({"item"})
            .Body("item")
            .Build()
        .PayloadSelector()
            .Args({"item"})
            .Body<TCoVoid>().Build()
            .Build()
        .Settings()
            .Add().Build("One", TNodeFlags::Default)
            .Add().Build("Auto", TNodeFlags::Default)
            .Build()
        .Done()
        .Ptr();
}

TExprNode::TPtr ExpandEquiJoin(const TExprNode::TPtr& input, TExprContext& ctx) {
    YQL_ENSURE(input->ChildrenSize() >= 4);
    return ExpandEquiJoinImpl(*input, ctx);
}

template <bool Strong>
bool CastMayFail(const TTypeAnnotationNode* source, const TTypeAnnotationNode* target) {
    return NUdf::ECastOptions::MayFail & CastResult<Strong>(source, target);
}

template <bool Strong>
TExprNode::TPtr ExpandCastOverData(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto from = input->Head().GetTypeAnn()->Cast<TDataExprType>();
    auto to = input->GetTypeAnn()->Cast<TDataExprType>();
    auto ret = input;
    if constexpr (Strong) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content()
            << " for " << *input->Head().GetTypeAnn()
            << " to " << *input->GetTypeAnn();
         ret = ctx.RenameNode(*input, "SafeCast");
    }

    auto fromFeatures = NUdf::GetDataTypeInfo(from->GetSlot()).Features;
    auto toFeatures = NUdf::GetDataTypeInfo(to->GetSlot()).Features;
    if ((fromFeatures & NUdf::TzDateType) && (toFeatures & (NUdf::DateType| NUdf::TzDateType)) ||
        (toFeatures & NUdf::TzDateType) && (fromFeatures & (NUdf::DateType | NUdf::TzDateType))) {
        // use Make for conversion
        ret = ctx.Builder(input->Pos())
            .Callable("Apply")
                .Callable(0, "Udf")
                    .Atom(0, TString("DateTime2.Make") + to->GetName())
                .Seal()
                .Add(1, input->HeadPtr())
            .Seal()
            .Build();
    }
    return ret;
}

template <bool Strong>
TExprNode::TPtr ExpandCastOverPg(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto to = input->GetTypeAnn()->Cast<TPgExprType>();
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " for Pg";
    return ctx.Builder(input->Pos())
        .Callable("PgCast")
            .Add(0, input->HeadPtr())
            .Add(1, ExpandType(input->Pos(), *to, ctx))
        .Seal()
        .Build();
}

template <bool Strong>
TExprNode::TPtr ExpandCastOverOptionalData(const TExprNode::TPtr& input, TExprContext& ctx) {
    if constexpr (Strong) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content()
            << " for " << *input->Head().GetTypeAnn()
            << " to " << *input->GetTypeAnn();

        const auto sourceType = input->Head().GetTypeAnn();
        const auto targetType = input->GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
        const auto options = CastResult<false>(sourceType, targetType);

        auto casted = options & NUdf::ECastOptions::MayFail ?
            ctx.RenameNode(*input, "SafeCast"):
            ctx.Builder(input->Pos())
                .Callable("Just")
                    .Callable(0, "SafeCast")
                        .Add(0, input->HeadPtr())
                        .Add(1, ExpandType(input->Tail().Pos(), *targetType, ctx))
                    .Seal()
                .Seal().Build();

        if (options & NUdf::ECastOptions::MayLoseData) {
            casted = ctx.Builder(input->Pos())
                .Callable("Filter")
                    .Add(0, std::move(casted))
                    .Lambda(1)
                        .Param("casted")
                        .Callable("==")
                            .Add(0, input->HeadPtr())
                            .Arg(1, "casted")
                        .Seal()
                    .Seal()
                .Seal().Build();
        }

        return casted;
    }

    return input;
}

template <bool Strong>
TExprNode::TPtr ExpandCastOverOptional(const TExprNode::TPtr& input, TExprContext& ctx) {
    const auto sourceType = input->Head().GetTypeAnn();
    const auto targetType = input->GetTypeAnn();
    const auto sourceItemType = sourceType->Cast<TOptionalExprType>()->GetItemType();
    const auto targetItemType = targetType->Cast<TOptionalExprType>()->GetItemType();

    if (ETypeAnnotationKind::Null == targetItemType->GetKind()) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " for Null?";
        return ctx.Builder(input->Pos())
            .Callable("If")
                .Callable(0, "Exists")
                    .Add(0, input->HeadPtr())
                .Seal()
                .Callable(1, "Null").Seal()
                .Callable(2, "Just")
                    .Callable(0, "Null").Seal()
                .Seal()
            .Seal().Build();
    }

    const bool opt = CastMayFail<Strong>(sourceItemType, targetItemType);
    const auto sourceLevel = GetOptionalLevel(sourceItemType);
    const auto targetLevel = GetOptionalLevel(targetItemType);

    if (opt && targetLevel > 0U) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content();
        auto stub = ExpandType(input->Tail().Pos(), *targetType, ctx);
        auto type = ExpandType(input->Tail().Pos(), *targetItemType, ctx);

        if (sourceLevel == targetLevel) {
            return ctx.Builder(input->Pos())
                .Callable("FlatMap")
                    .Add(0, input->HeadPtr())
                    .Lambda(1)
                        .Param("item")
                        .Callable("If")
                            .Callable(0, "Or")
                                .Callable(0, "Exists")
                                    .Callable(0, input->Content())
                                        .Arg(0, "item")
                                        .Add(1, type)
                                    .Seal()
                                .Seal()
                                .Callable(1, "Not")
                                    .Callable(0, "Exists")
                                        .Arg(0, "item")
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Callable(1, "Just")
                                .Callable(0, input->Content())
                                    .Arg(0, "item")
                                    .Add(1, std::move(type))
                                .Seal()
                            .Seal()
                            .Callable(2, "Nothing")
                                .Add(0, std::move(stub))
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal().Build();
        } else if (sourceLevel < targetLevel) {
            auto casted = ctx.ChangeChild(*input, 1U, std::move(type));
            return ctx.Builder(input->Pos())
                .Callable("If")
                    .Callable(0, "Or")
                        .Callable(0, "Exists")
                            .Add(0, casted)
                        .Seal()
                        .Callable(1, "Not")
                            .Callable(0, "Exists")
                                .Add(0, input->HeadPtr())
                            .Seal()
                        .Seal()
                    .Seal()
                    .Callable(1, "Just")
                        .Add(0, std::move(casted))
                    .Seal()
                    .Callable(2, "Nothing")
                        .Add(0, std::move(stub))
                    .Seal()
                .Seal().Build();
        }
    }

    const bool flat = opt || sourceLevel > targetLevel;
    auto type = ExpandType(input->Tail().Pos(), flat ? *targetType : *targetItemType, ctx);
    if (!opt && sourceLevel < targetLevel) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " as Just";
        return ctx.Builder(input->Pos())
            .Callable("Just")
                .Callable(0, input->Content())
                    .Add(0, input->HeadPtr())
                    .Add(1, std::move(type))
                .Seal()
            .Seal().Build();
    }

    const auto worker = flat ? "FlatMap" : "Map";
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " as " << worker;
    return ctx.Builder(input->Pos())
        .Callable(worker)
            .Add(0, input->HeadPtr())
            .Lambda(1)
                .Param("item")
                .Callable(input->Content())
                    .Arg(0, "item")
                    .Add(1, std::move(type))
                .Seal()
            .Seal()
        .Seal().Build();
}

template <bool Strong>
TExprNode::TPtr ExpandCastOverList(const TExprNode::TPtr& input, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " for List";
    const auto sourceType = input->Head().GetTypeAnn();
    const auto targetType = input->GetTypeAnn();
    const auto sourceItemType = sourceType->Cast<TListExprType>()->GetItemType();
    const auto targetItemType = targetType->Cast<TListExprType>()->GetItemType();
    const bool opt = CastMayFail<Strong>(sourceItemType, targetItemType);
    auto type = ExpandType(input->Tail().Pos(), opt ? *ctx.MakeType<TOptionalExprType>(targetItemType) : *targetItemType, ctx);
    return ctx.Builder(input->Pos())
        .Callable(opt ? "OrderedFlatMap" : "OrderedMap")
            .Add(0, input->HeadPtr())
            .Lambda(1)
                .Param("item")
                .Callable(input->Content())
                    .Arg(0, "item")
                    .Add(1, std::move(type))
                .Seal()
            .Seal()
        .Seal().Build();
}

template <bool Strong, class TSeqType>
TExprNode::TPtr ExpandCastOverSequence(const TExprNode::TPtr& input, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " for sequence";
    const auto sourceType = input->Head().GetTypeAnn();
    const auto targetType = input->GetTypeAnn();
    const auto sourceItemType = sourceType->Cast<TSeqType>()->GetItemType();
    const auto targetItemType = targetType->Cast<TSeqType>()->GetItemType();
    const bool opt = CastMayFail<Strong>(sourceItemType, targetItemType);
    auto type = ExpandType(input->Tail().Pos(), opt ? *ctx.MakeType<TOptionalExprType>(targetItemType) : *targetItemType, ctx);
    return ctx.Builder(input->Pos())
        .Callable(opt ? "OrderedFlatMap" : "OrderedMap")
            .Add(0, input->HeadPtr())
            .Lambda(1)
                .Param("item")
                .Callable(input->Content())
                    .Arg(0, "item")
                    .Add(1, std::move(type))
                .Seal()
            .Seal()
        .Seal().Build();
}

template <bool Strong>
TExprNode::TPtr ExpandCastOverDict(const TExprNode::TPtr& input, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " for Dict";
    const auto targetType = input->GetTypeAnn();
    const auto targetDictType = targetType->Cast<TDictExprType>();
    const TTypeAnnotationNode::TListType items = {targetDictType->GetKeyType(), targetDictType->GetPayloadType()};
    auto type = ExpandType(input->Tail().Pos(), *ctx.MakeType<TListExprType>(ctx.MakeType<TTupleExprType>(items)), ctx);
    return ctx.Builder(input->Pos())
        .Callable("ToDict")
            .Callable(0, input->Content())
                .Callable(0, "DictItems")
                    .Add(0, input->HeadPtr())
                .Seal()
                .Add(1, std::move(type))
            .Seal()
            .Lambda(1)
                .Param("item")
                .Callable("Nth")
                    .Arg(0, "item")
                    .Atom(1, 0)
                .Seal()
            .Seal()
            .Lambda(2)
                .Param("item")
                .Callable("Nth")
                    .Arg(0, "item")
                    .Atom(1, 1)
                .Seal()
            .Seal()
            .List(3)
                .Atom(0, "Auto", TNodeFlags::Default)
                .Atom(1, "One", TNodeFlags::Default)
            .Seal()
        .Seal().Build();
}

template <bool Strong>
TExprNode::TPtr ExpandCastOverTuple(const TExprNode::TPtr& input, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " for Tuple";
    const auto targetType = input->GetTypeAnn();
    const auto& targetItems = targetType->Cast<TTupleExprType>()->GetItems();
    const auto sourceSize = input->Head().GetTypeAnn()->Cast<TTupleExprType>()->GetSize();
    TExprNode::TListType castedItems;
    castedItems.reserve(targetItems.size());
    ui32 i = 0U;
    for (const auto& item : targetItems) {
        auto type = ExpandType(input->Tail().Pos(), *item, ctx);
        castedItems.emplace_back(i < sourceSize ?
            ctx.Builder(input->Pos())
                .Callable(input->Content())
                    .Callable(0, "Nth")
                        .Add(0, input->HeadPtr())
                        .Atom(1, ToString(i), TNodeFlags::Default)
                    .Seal()
                    .Add(1, std::move(type))
                .Seal()
            .Build():
            ctx.Builder(input->Pos())
                .Callable("Nothing")
                    .Add(0, std::move(type))
                .Seal()
            .Build()
        );
        ++i;
    }
    return ctx.NewList(input->Pos(), std::move(castedItems));
}

template <bool Strong>
TExprNode::TPtr ExpandCastOverStruct(const TExprNode::TPtr& input, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " for Struct";
    const auto sourceType = input->Head().GetTypeAnn();
    const auto targetType = input->GetTypeAnn();
    const auto& sourceItems = sourceType->Cast<TStructExprType>()->GetItems();
    std::unordered_set<std::string_view> sourceNames(sourceItems.size());
    for (const auto& item : sourceItems) {
        YQL_ENSURE(sourceNames.emplace(item->GetName()).second);
    }
    const auto& targetItems = targetType->Cast<TStructExprType>()->GetItems();
    TExprNode::TListType castedItems;
    castedItems.reserve(targetItems.size());
    for (const auto& item : targetItems) {
        const auto& name = item->GetName();
        auto type = ExpandType(input->Tail().Pos(), *item->GetItemType(), ctx);
        castedItems.emplace_back(sourceNames.cend() == sourceNames.find(name) ?
            ctx.Builder(input->Pos())
                .List()
                    .Atom(0, name)
                    .Callable(1, "Nothing")
                        .Add(0, std::move(type))
                    .Seal()
                .Seal().Build():
            ctx.Builder(input->Pos())
                .List()
                    .Atom(0, name)
                    .Callable(1, input->Content())
                        .Callable(0, "Member")
                            .Add(0, input->HeadPtr())
                            .Atom(1, name)
                        .Seal()
                        .Add(1, std::move(type))
                    .Seal()
                .Seal().Build()
        );
    }
    return ctx.NewCallable(input->Pos(), "AsStruct", std::move(castedItems));
}

template <bool Strong>
TExprNode::TPtr ExpandCastOverVariant(const TExprNode::TPtr& input, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " for Variant";
    const auto targetType = input->GetTypeAnn();
    const auto sourceType = input->Head().GetTypeAnn();
    const auto targetUnderType = targetType->Cast<TVariantExprType>()->GetUnderlyingType();
    const auto sourceUnderType = sourceType->Cast<TVariantExprType>()->GetUnderlyingType();
    TExprNode::TListType variants, types;
    switch (targetUnderType->GetKind()) {
        case ETypeAnnotationKind::Tuple: {
            const auto sourceTupleType = sourceUnderType->Cast<TTupleExprType>();
            const auto targetTupleType = targetUnderType->Cast<TTupleExprType>();
            const auto size = std::min(sourceTupleType->GetSize(), targetTupleType->GetSize());
            types.resize(size);
            variants.resize(size);
            const auto& items = targetTupleType->GetItems();
            for (ui32 i = 0U; i < size; ++i) {
                types[i] = ExpandType(input->Tail().Pos(), *items[i], ctx);
                variants[i] = ctx.NewAtom(input->Pos(), i);
            }
            break;
        }
        case ETypeAnnotationKind::Struct: {
            const auto sourceStructType = sourceUnderType->Cast<TStructExprType>();
            const auto targetStructType = targetUnderType->Cast<TStructExprType>();
            const auto size = std::min(sourceStructType->GetSize(), targetStructType->GetSize());
            types.reserve(size);
            variants.reserve(size);
            for (const auto& item : sourceStructType->GetItems()) {
                if (const auto type = targetStructType->FindItemType(item->GetName())) {
                    types.emplace_back(ExpandType(input->Tail().Pos(), *type, ctx));
                    variants.emplace_back(ctx.NewAtom(input->Pos(), item->GetName()));
                }
            }
            break;
        }
        default: break;
    }
    const auto type = ExpandType(input->Tail().Pos(), *targetType, ctx);
    return ctx.Builder(input->Pos())
        .Callable("Visit")
            .Add(0, input->HeadPtr())
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                for (ui32 i = 0U; i < variants.size(); ++i) {
                    parent.Add(1 + 2 * i, variants[i]);
                    parent.Lambda(2 + 2 * i)
                        .Param("item")
                        .Callable("Variant")
                            .Callable(0, input->Content())
                                .Arg(0, "item")
                                .Add(1, std::move(types[i]))
                            .Seal()
                            .Add(1, std::move(variants[i]))
                            .Add(2, type)
                        .Seal()
                    .Seal();
                }
                return parent;
            })
        .Seal().Build();
}

template <bool Strong>
TExprNode::TPtr ExpandCastOverOptionalList(const TExprNode::TPtr& input, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " for List?";
    const auto targetType = input->GetTypeAnn();
    const auto targetItemType = targetType->Cast<TOptionalExprType>()->GetItemType()->Cast<TListExprType>()->GetItemType();
    auto type = ExpandType(input->Tail().Pos(), *ctx.MakeType<TOptionalExprType>(targetItemType), ctx);
    auto casted = ctx.Builder(input->Pos())
        .Callable("ListTakeWhile")
            .Callable(0, "ListMap")
                .Callable(0, "LazyList")
                    .Add(0, input->HeadPtr())
                .Seal()
                .Lambda(1)
                    .Param("item")
                    .Callable(input->Content())
                        .Arg(0, "item")
                        .Add(1, std::move(type))
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(1)
                .Param("item")
                .Callable("Exists")
                    .Arg(0, "item")
                .Seal()
            .Seal()
        .Seal().Build();

    auto stub = ExpandType(input->Tail().Pos(), *input->GetTypeAnn(), ctx);
    return ctx.Builder(input->Pos())
        .Callable("If")
            .Callable(0, "AggrEquals")
                .Callable(0, "Length")
                    .Add(0, input->HeadPtr())
                .Seal()
                .Callable(1, "Length")
                    .Add(0, casted)
                .Seal()
            .Seal()
            .Callable(1, "ListNotNull")
                .Add(0, std::move(casted))
            .Seal()
            .Callable(2, "Nothing")
                .Add(0, std::move(stub))
            .Seal()
        .Seal().Build();
}

template <bool Strong>
TExprNode::TPtr ExpandCastOverOptionalDict(const TExprNode::TPtr& input, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " for Dict?";
    const auto targetType = input->GetTypeAnn();
    const auto targetDictType = targetType->Cast<TOptionalExprType>()->GetItemType()->Cast<TDictExprType>();
    const TTypeAnnotationNode::TListType items = {targetDictType->GetKeyType(), targetDictType->GetPayloadType()};
    auto type = ExpandType(input->Tail().Pos(), *ctx.MakeType<TOptionalExprType>(ctx.MakeType<TListExprType>(ctx.MakeType<TTupleExprType>(items))), ctx);
    return ctx.Builder(input->Pos())
        .Callable("Map")
            .Callable(0, input->Content())
                .Callable(0, "DictItems")
                    .Add(0, input->HeadPtr())
                .Seal()
                .Add(1, std::move(type))
            .Seal()
            .Lambda(1)
                .Param("list")
                .Callable("ToDict")
                    .Arg(0, "list")
                    .Lambda(1)
                        .Param("item")
                        .Callable("Nth")
                            .Arg(0, "item")
                            .Atom(1, 0)
                        .Seal()
                    .Seal()
                    .Lambda(2)
                        .Param("item")
                        .Callable("Nth")
                            .Arg(0, "item")
                            .Atom(1, 1)
                        .Seal()
                    .Seal()
                    .List(3)
                        .Atom(0, "Auto", TNodeFlags::Default)
                        .Atom(1, "One", TNodeFlags::Default)
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();
}

TExprNodeBuilder& BuildExistsChecker(size_t level, TExprNodeBuilder& parent) {
    if (level) {
        parent
            .Callable("IfPresent")
                .Arg(0, "l" + ToString(level))
                .Lambda(1)
                    .Param("l" + ToString(--level))
                    .Do(std::bind(&BuildExistsChecker, level, std::placeholders::_1))
                .Seal()
                .Callable(2, "Bool")
                    .Atom(0, "false", TNodeFlags::Default)
                .Seal()
            .Seal();
    } else {
        parent
            .Callable("Not")
                .Callable(0, "Exists")
                    .Arg(0, "l" + ToString(level))
                .Seal()
            .Seal();
    }
    return parent;
}

TExprNode::TPtr FilterByOptionalItems(TExprNode::TPtr&& casted, TExprNode::TListType&& items, std::vector<size_t>&& levels, TExprContext& ctx) {
    if (items.empty()) {
        return casted;
    }

    return ctx.Builder(casted->Pos())
        .Callable("Filter")
            .Add(0, std::move(casted))
            .Lambda(1)
                .Param("casted")
                .Callable("And")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 i = 0U;
                        for (auto& item : items) {
                            const auto& getter = item->Content();
                            auto field = item->TailPtr();
                            if (auto level = levels[i]) {
                                item = ctx.Builder(item->Pos())
                                    .Callable("IfPresent")
                                        .Add(0, std::move(item))
                                        .Lambda(1)
                                            .Param("l" + ToString(--level))
                                            .Do(std::bind(&BuildExistsChecker, level, std::placeholders::_1))
                                        .Seal()
                                        .Callable(2, "Bool")
                                            .Atom(0, "false", TNodeFlags::Default)
                                        .Seal()
                                    .Seal().Build();
                            } else {
                                item = ctx.Builder(item->Pos())
                                    .Callable("Not")
                                        .Callable(0, "Exists")
                                            .Add(0, std::move(item))
                                        .Seal()
                                    .Seal().Build();
                            }

                            parent
                                .Callable(i++, "Or")
                                    .Callable(0, "Exists")
                                        .Callable(0, getter)
                                            .Arg(0, "casted")
                                            .Add(1, std::move(field))
                                        .Seal()
                                    .Seal()
                                    .Add(1, std::move(item))
                                .Seal();
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
        .Seal().Build();
}

template <bool Strong>
TExprNode::TPtr ExpandCastOverOptionalTuple(const TExprNode::TPtr& input, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " for Tuple?";
    const auto sourceType = input->Head().GetTypeAnn();
    const auto targetType = input->GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
    const auto& sourceItems = sourceType->Cast<TTupleExprType>()->GetItems();
    const auto& targetItems = targetType->Cast<TTupleExprType>()->GetItems();
    TExprNode::TListType castedItems, filteredItems, optionalItems;
    std::vector<size_t> optionalLevels;
    castedItems.reserve(targetItems.size());
    filteredItems.reserve(targetItems.size());
    optionalItems.reserve(targetItems.size());
    optionalLevels.reserve(targetItems.size());
    ui32 i = 0U;
    for (const auto& item : targetItems) {
        auto index = ctx.NewAtom(input->Pos(), ToString(i), TNodeFlags::Default);
        auto type = ExpandType(input->Tail().Pos(), *item, ctx);
        if (i >= sourceItems.size()) {
            castedItems.emplace_back(
                ctx.Builder(input->Pos())
                    .Callable("Nothing")
                        .Add(0, std::move(type))
                    .Seal()
                .Build()
            );
        } else {
            auto source = ctx.NewCallable(input->Pos(), "Nth", {input->HeadPtr(), index});
            if (CastMayFail<Strong>(sourceItems[i], targetItems[i])) {
                if (ETypeAnnotationKind::Optional == item->GetKind()) {
                    optionalItems.emplace_back(source);
                    const auto sourceLevel = GetOptionalLevel(sourceItems[i]);
                    const auto targetLevel = GetOptionalLevel(targetItems[i]);
                    optionalLevels.emplace_back(sourceLevel > targetLevel ? sourceLevel - targetLevel : 0ULL);
                } else {
                    filteredItems.emplace_back(index);
                }
            }
            castedItems.emplace_back(ctx.NewCallable(input->Pos(), input->Content(), {std::move(source), std::move(type)}));
        }
        ++i;
    }

    auto casted = ctx.Builder(input->Pos())
        .Callable("Just")
            .List(0)
                .Add(std::move(castedItems))
            .Seal()
        .Seal().Build();

    if (!filteredItems.empty()) {
        casted = ctx.Builder(input->Pos())
            .Callable("FilterNullElements")
                .Add(0, std::move(casted))
                .List(1)
                    .Add(std::move(filteredItems))
                .Seal()
            .Seal().Build();
    }

    casted = FilterByOptionalItems(std::move(casted), std::move(optionalItems), std::move(optionalLevels), ctx);

    if (Strong && sourceItems.size() > targetItems.size()) {
        TExprNode::TListType items;
        items.reserve(sourceItems.size() - targetItems.size());
        for (auto i = targetItems.size(); i < sourceItems.size(); ++i) {
            if (sourceItems[i]->GetKind() == ETypeAnnotationKind::Optional) {
                items.emplace_back(ctx.Builder(input->Pos())
                    .Callable("Exists")
                        .Callable(0, "Nth")
                            .Add(0, input->HeadPtr())
                            .Atom(1, i)
                        .Seal()
                    .Seal().Build()
                );
            }
        }

        if (!items.empty()) {
            auto type = ExpandType(input->Tail().Pos(), *input->GetTypeAnn(), ctx);
            casted = ctx.Builder(input->Pos())
                .Callable("If")
                    .Callable(0, "Not")
                        .Callable(0, "Or")
                            .Add(std::move(items))
                        .Seal()
                    .Seal()
                    .Add(1, std::move(casted))
                    .Callable(2, "Nothing")
                        .Add(0, std::move(type))
                    .Seal()
                .Seal().Build();
        }
    }

    return casted;
}

template <bool Strong>
TExprNode::TPtr ExpandCastOverOptionalStruct(const TExprNode::TPtr& input, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " for Struct?";
    const auto sourceType = input->Head().GetTypeAnn();
    const auto targetType = input->GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
    const auto& sourceItems = sourceType->Cast<TStructExprType>()->GetItems();
    std::unordered_map<std::string_view, const TTypeAnnotationNode*> sourceNames(sourceItems.size());
    for (const auto& item : sourceItems) {
        YQL_ENSURE(sourceNames.emplace(item->GetName(), item->GetItemType()).second);
    }
    const auto& targetItems = targetType->Cast<TStructExprType>()->GetItems();
    TExprNode::TListType castedItems, filteredItems, optionalItems;
    std::vector<size_t> optionalLevels;
    castedItems.reserve(targetItems.size());
    filteredItems.reserve(targetItems.size());
    optionalItems.reserve(targetItems.size());
    optionalLevels.reserve(targetItems.size());
    for (const auto& item : targetItems) {
        auto name = ctx.NewAtom(input->Pos(), item->GetName());
        auto type = ExpandType(input->Tail().Pos(), *item->GetItemType(), ctx);
        const auto it = sourceNames.find(item->GetName());
        if (sourceNames.cend() == it) {
            castedItems.emplace_back(
                ctx.Builder(input->Pos())
                    .List()
                        .Add(0, std::move(name))
                        .Callable(1, "Nothing")
                            .Add(0, std::move(type))
                        .Seal()
                    .Seal()
                .Build()
            );
        } else {
            auto source = ctx.NewCallable(input->Pos(), "Member", {input->HeadPtr(), name});
            if (CastMayFail<Strong>(it->second, item->GetItemType())) {
                if (ETypeAnnotationKind::Optional == item->GetItemType()->GetKind()) {
                    optionalItems.emplace_back(source);
                    const auto sourceLevel = GetOptionalLevel(it->second);
                    const auto targetLevel = GetOptionalLevel(item->GetItemType());
                    optionalLevels.emplace_back(sourceLevel > targetLevel ? sourceLevel - targetLevel : 0ULL);
                } else {
                    filteredItems.emplace_back(name);
                }
            }
            sourceNames.erase(it);
            castedItems.emplace_back(
                ctx.Builder(input->Pos())
                    .List()
                        .Add(0, std::move(name))
                        .Callable(1, input->Content())
                            .Add(0, std::move(source))
                            .Add(1, std::move(type))
                        .Seal()
                    .Seal()
                .Build()
            );
        }
    }

    auto casted = ctx.Builder(input->Pos())
        .Callable("Just")
            .Callable(0, "AsStruct")
                .Add(std::move(castedItems))
            .Seal()
        .Seal().Build();

    if (!filteredItems.empty()) {
        casted = ctx.Builder(input->Pos())
            .Callable("FilterNullMembers")
                .Add(0, std::move(casted))
                .List(1)
                    .Add(std::move(filteredItems))
                .Seal()
            .Seal().Build();
    }

    casted = FilterByOptionalItems(std::move(casted), std::move(optionalItems), std::move(optionalLevels),  ctx);

    if (Strong && !sourceNames.empty()) {
        TExprNode::TListType items;
        items.reserve(sourceNames.size());
        for (const auto& skipped : sourceNames) {
            if (skipped.second->GetKind() == ETypeAnnotationKind::Optional) {
                items.emplace_back(ctx.Builder(input->Pos())
                    .Callable("Exists")
                        .Callable(0, "Member")
                            .Add(0, input->HeadPtr())
                            .Atom(1, skipped.first)
                        .Seal()
                    .Seal().Build()
                );
            }
        }

        if (!items.empty()) {
            auto type = ExpandType(input->Tail().Pos(), *input->GetTypeAnn(), ctx);
            casted = ctx.Builder(input->Pos())
                .Callable("If")
                    .Callable(0, "Not")
                        .Callable(0, "Or")
                            .Add(std::move(items))
                        .Seal()
                    .Seal()
                    .Add(1, std::move(casted))
                    .Callable(2, "Nothing")
                        .Add(0, std::move(type))
                    .Seal()
                .Seal().Build();
        }
    }

    return casted;
}

template <bool Strong>
TExprNode::TPtr ExpandCastOverOptionalVariant(const TExprNode::TPtr& input, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " for Variant?";
    const auto sourceType = input->Head().GetTypeAnn();
    const auto targetType = input->GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
    const auto sourceUnderType = sourceType->Cast<TVariantExprType>()->GetUnderlyingType();
    const auto targetUnderType = targetType->Cast<TVariantExprType>()->GetUnderlyingType();
    TExprNode::TListType variants, types;
    std::vector<std::optional<bool>> checks;
    std::vector<std::optional<ui32>> renumIndex;
    switch (targetUnderType->GetKind()) {
        case ETypeAnnotationKind::Tuple: {
            const auto& sources = sourceUnderType->Cast<TTupleExprType>()->GetItems();
            const auto& targets = targetUnderType->Cast<TTupleExprType>()->GetItems();
            types.resize(targets.size());
            variants.resize(sources.size());
            checks.resize(sources.size());
            renumIndex.resize(sources.size());
            for (ui32 i = 0U; i < variants.size(); ++i) {
                renumIndex[i] = i;
                variants[i] = ctx.NewAtom(input->Pos(), i);
                if (i < types.size()) {
                    types[i] = ExpandType(input->Tail().Pos(), *targets[i], ctx);
                    checks[i] = CastMayFail<Strong>(sources[i], targets[i]);
                }
            }
            break;
        }
        case ETypeAnnotationKind::Struct: {
            const auto sourcesStructType = sourceUnderType->Cast<TStructExprType>();
            const auto targetsStructType = targetUnderType->Cast<TStructExprType>();
            const auto& sources = sourcesStructType->GetItems();
            const auto& targets = targetsStructType->GetItems();
            types.resize(targets.size());
            variants.resize(sources.size());
            checks.resize(sources.size());
            renumIndex.resize(sources.size());
            for (ui32 i = 0U; i < variants.size(); ++i) {
                variants[i] = ctx.NewAtom(input->Pos(), sources[i]->GetName());
                if (const auto idx = targetsStructType->FindItem(sources[i]->GetName())) {
                    const auto type = targets[*idx]->GetItemType();
                    types[*idx] = ExpandType(input->Tail().Pos(), *type, ctx);
                    checks[i] = CastMayFail<Strong>(sources[i]->GetItemType(), type);
                    renumIndex[i] = *idx;
                }
            }
            break;
        }
        default: break;
    }
    const auto type = ExpandType(input->Tail().Pos(), *targetType, ctx);
    const auto stub = ExpandType(input->Tail().Pos(), *input->GetTypeAnn(), ctx);
    return ctx.Builder(input->Pos())
        .Callable("Visit")
            .Add(0, input->HeadPtr())
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                for (ui32 i = 0U; i < variants.size(); ++i) {
                    parent.Add(1 + 2 * i, variants[i]);
                    if (const auto check = checks[i]) {
                        if (*check) {
                            parent.Lambda(2 + 2 * i)
                                .Param("item")
                                    .Callable("IfPresent")
                                        .Callable(0, input->Content())
                                            .Arg(0, "item")
                                            .Add(1, std::move(types[*renumIndex[i]]))
                                        .Seal()
                                        .Lambda(1)
                                            .Param("casted")
                                            .Callable("Just")
                                                .Callable(0, "Variant")
                                                    .Arg(0, "casted")
                                                    .Add(1, std::move(variants[i]))
                                                    .Add(2, type)
                                                .Seal()
                                            .Seal()
                                        .Seal()
                                        .Callable(2, "Nothing")
                                            .Add(0, stub)
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal();
                        } else {
                            parent.Lambda(2 + 2 * i)
                                .Param("item")
                                .Callable("Just")
                                    .Callable(0, "Variant")
                                        .Callable(0, input->Content())
                                            .Arg(0, "item")
                                            .Add(1, std::move(types[*renumIndex[i]]))
                                        .Seal()
                                        .Add(1, std::move(variants[i]))
                                        .Add(2, type)
                                    .Seal()
                                .Seal()
                            .Seal();
                        }
                    } else {
                        parent.Lambda(2 + 2 * i)
                            .Param("item")
                            .Callable("Nothing")
                                .Add(0, stub)
                            .Seal()
                        .Seal();
                    }
                }
                return parent;
            })
        .Seal().Build();
}

template <bool Strong>
TExprNode::TPtr ExpandCast(const TExprNode::TPtr& input, TExprContext& ctx) {
    const auto sKind = input->Head().GetTypeAnn()->GetKind();
    const auto tKind = input->GetTypeAnn()->GetKind();
    if (sKind == tKind) {
        switch (sKind) {
            case ETypeAnnotationKind::Data:     return ExpandCastOverData<Strong>(input, ctx);
            case ETypeAnnotationKind::Pg:       return ExpandCastOverPg<Strong>(input, ctx);
            case ETypeAnnotationKind::Optional: return ExpandCastOverOptional<Strong>(input, ctx);
            case ETypeAnnotationKind::List:     return ExpandCastOverList<Strong>(input, ctx);
            case ETypeAnnotationKind::Dict:     return ExpandCastOverDict<Strong>(input, ctx);
            case ETypeAnnotationKind::Tuple:    return ExpandCastOverTuple<Strong>(input, ctx);
            case ETypeAnnotationKind::Struct:   return ExpandCastOverStruct<Strong>(input, ctx);
            case ETypeAnnotationKind::Variant:  return ExpandCastOverVariant<Strong>(input, ctx);
            case ETypeAnnotationKind::Stream:     return ExpandCastOverSequence<Strong, TStreamExprType>(input, ctx);
            case ETypeAnnotationKind::Flow:     return ExpandCastOverSequence<Strong, TFlowExprType>(input, ctx);
            default: break;
        }
    } else if (tKind == ETypeAnnotationKind::Optional) {
        const auto targetItemType = input->GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType();
        auto type = ExpandType(input->Tail().Pos(), *targetItemType, ctx);
        if (CastMayFail<Strong>(input->Head().GetTypeAnn(), targetItemType)) {
            if (targetItemType->GetKind() == sKind) {
                switch (sKind) {
                    case ETypeAnnotationKind::Data:     return ExpandCastOverOptionalData<Strong>(input, ctx);
                    case ETypeAnnotationKind::List:     return ExpandCastOverOptionalList<Strong>(input, ctx);
                    case ETypeAnnotationKind::Dict:     return ExpandCastOverOptionalDict<Strong>(input, ctx);
                    case ETypeAnnotationKind::Tuple:    return ExpandCastOverOptionalTuple<Strong>(input, ctx);
                    case ETypeAnnotationKind::Struct:   return ExpandCastOverOptionalStruct<Strong>(input, ctx);
                    case ETypeAnnotationKind::Variant:  return ExpandCastOverOptionalVariant<Strong>(input, ctx);
                    default: break;
                }
            } else {
                YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " as Map Just";
                return ctx.Builder(input->Pos())
                    .Callable("Map")
                        .Callable(0, input->Content())
                            .Add(0, input->HeadPtr())
                            .Add(1, std::move(type))
                        .Seal()
                        .Lambda(1)
                            .Param("item")
                            .Callable("Just")
                                .Arg(0, "item")
                            .Seal()
                        .Seal()
                    .Seal().Build();
            }
        } else {
            YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content() << " as Just";
            return ctx.Builder(input->Pos())
                .Callable("Just")
                    .Callable(0, input->Content())
                        .Add(0, input->HeadPtr())
                        .Add(1, std::move(type))
                    .Seal()
                .Seal().Build();
        }
    } else if (tKind == ETypeAnnotationKind::List && sKind == ETypeAnnotationKind::EmptyList) {
        return ctx.NewCallable(input->Pos(), "List", { ExpandType(input->Pos(), *input->GetTypeAnn(), ctx) });
    } else if (tKind == ETypeAnnotationKind::Dict && sKind == ETypeAnnotationKind::EmptyDict) {
        return ctx.NewCallable(input->Pos(), "Dict", { ExpandType(input->Pos(), *input->GetTypeAnn(), ctx) });
    }

    return input;
}

TExprNode::TPtr ExpandAlterTo(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto sourceType = node->Head().GetTypeAnn();
    const auto targetType = node->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();

    if (targetType->GetKind() == ETypeAnnotationKind::Null) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content() << " to " << *targetType;
        return ctx.Builder(node->Pos())
            .Callable("If")
                .Callable(0, "Exists")
                    .Add(0, node->HeadPtr())
                .Seal()
                .Add(1, node->TailPtr())
                .Apply(2, *node->Child(2))
                    .With(0)
                        .Callable("Null").Seal()
                    .Done()
                .Seal()
            .Seal().Build();
    }

    if (CastMayFail<true>(sourceType, targetType)) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();
        auto type = ExpandType(node->Child(1)->Pos(), *ctx.MakeType<TOptionalExprType>(targetType), ctx);
        return ctx.Builder(node->Pos())
            .Callable("IfPresent")
                .Callable(0, "StrictCast")
                    .Add(0, node->HeadPtr())
                    .Add(1, std::move(type))
                .Seal()
                .Lambda(1)
                    .Param("casted")
                    .Apply(*node->Child(2))
                        .With(0, "casted")
                    .Seal()
                .Seal()
                .Add(2, node->TailPtr())
            .Seal().Build();
    }

    auto casted = ctx.NewCallable(node->Pos(), "StrictCast", {node->HeadPtr(), node->ChildPtr(1)});
    YQL_CLOG(DEBUG, CorePeepHole) << "Replace " << node->Content() << " on " << casted->Content();
    return ctx.ReplaceNode(node->Child(2)->TailPtr(), node->Child(2)->Head().Head(), std::move(casted));
}

TExprNode::TPtr BuildDictOverListOfStructs(TPositionHandle pos, const TExprNode::TPtr& collection,
    const TTypeAnnotationNode*& dictKeyType, TExprContext& ctx)
{
    auto collectionType = collection->GetTypeAnn();
    YQL_ENSURE(collectionType->GetKind() == ETypeAnnotationKind::List);

    auto listItemType = collectionType->Cast<TListExprType>()->GetItemType();
    YQL_ENSURE(listItemType->GetKind() == ETypeAnnotationKind::Struct);

    auto structType = listItemType->Cast<TStructExprType>();
    YQL_ENSURE(structType->GetSize() == 1);
    auto memberName = structType->GetItems()[0]->GetName();
    dictKeyType = structType->GetItems()[0]->GetItemType();

    return ctx.Builder(pos)
        .Callable("ToDict")
            .Add(0, collection)
            .Lambda(1) // keyExtractor
                .Param("item")
                .Callable("Member")
                    .Arg(0, "item")
                    .Atom(1, memberName)
                .Seal()
            .Seal()
            .Lambda(2) // payloadExtractor
                .Param("item")
                .Callable("Void")
                .Seal()
            .Seal()
            .List(3)
                .Atom(0, "Auto", TNodeFlags::Default)
                .Atom(1, "One", TNodeFlags::Default)
                .Atom(2, "Compact", TNodeFlags::Default)
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildDictOverList(TPositionHandle pos, const TExprNode::TPtr& collection, TExprContext& ctx) {
    auto collectionType = collection->GetTypeAnn();
    YQL_ENSURE(collectionType->GetKind() == ETypeAnnotationKind::List);

    return ctx.Builder(pos)
        .Callable("ToDict")
            .Add(0, collection)
            .Lambda(1) // keyExtractor
                .Param("item")
                .Arg("item")
            .Seal()
            .Lambda(2) // payloadExtractor
                .Param("item")
                .Callable("Void")
                .Seal()
            .Seal()
            .List(3)
                .Atom(0, "Auto", TNodeFlags::Default)
                .Atom(1, "One", TNodeFlags::Default)
                .Atom(2, "Compact", TNodeFlags::Default)
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildDictOverTuple(TExprNode::TPtr&& collection, const TTypeAnnotationNode*& dictKeyType, TExprContext& ctx)
{
    const auto pos = collection->Pos();
    const auto tupleType = collection->GetTypeAnn()->Cast<TTupleExprType>();
    if (!tupleType->GetSize()) {
        dictKeyType = nullptr;
        return nullptr;
    }
    TTypeAnnotationNode::TListType types(tupleType->GetItems());
    dictKeyType = CommonType(pos, types, ctx);
    YQL_ENSURE(dictKeyType, "Uncompatible collection elements.");

    TExprNode::TPtr tuple;
    if (collection->IsList()) {
        tuple = collection;
    } else {
        TExprNode::TListType items;
        items.reserve(tupleType->GetSize());
        for (size_t i = 0; i != tupleType->GetSize(); ++i) {
            items.push_back(
                ctx.NewCallable(
                    pos,
                    "Nth",
                    {collection, ctx.NewAtom(pos, ui32(i))}
                )
            );
        }
        tuple = ctx.NewList(pos, std::move(items));
    }
    return ctx.NewCallable(pos, "DictFromKeys", {ExpandType(pos, *dictKeyType, ctx), std::move(tuple)});
}

TExprNode::TPtr ExpandSqlIn(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto collection = input->HeadPtr();
    auto lookup = input->ChildPtr(1);
    auto options = input->ChildPtr(2);

    const bool ansiIn = HasSetting(*options, "ansi");
    const bool tableSource = HasSetting(*options, "tableSource");
    static const size_t MaxCollectionItemsToExpandAsOrChain = 5;
    const bool hasOptionals = collection->GetTypeAnn()->HasOptionalOrNull() ||
                              lookup->GetTypeAnn()->HasOptionalOrNull();
    if (ansiIn || !hasOptionals) {
        const size_t collectionSize = collection->ChildrenSize();
        if ((collection->IsCallable("AsList") || collection->IsList()) &&
            collectionSize <= MaxCollectionItemsToExpandAsOrChain &&
            collectionSize > 0)
        {
            TExprNodeList orItems;
            for (size_t i = 0; i < collectionSize; ++i) {
                TExprNode::TPtr collectionItem = collection->ChildPtr(i);
                if (tableSource) {
                    collectionItem = ctx.NewCallable(input->Pos(), "SingleMember", { collectionItem });
                }
                orItems.push_back(ctx.Builder(input->Pos())
                    .Callable("==")
                        .Add(0, lookup)
                        .Add(1, collectionItem)
                    .Seal()
                    .Build()
                );
            }
            YQL_CLOG(DEBUG, CorePeepHole) << "IN with small literal list/tuple (of size " << collectionSize << ")";
            return ctx.NewCallable(input->Pos(), "Or", std::move(orItems));
        }
    }

    auto collectionType = collection->GetTypeAnn();
    TExprNode::TPtr dict;
    const TTypeAnnotationNode* dictKeyType = nullptr;
    if (collectionType->GetKind() == ETypeAnnotationKind::List)
    {
        if (tableSource) {
            YQL_CLOG(DEBUG, CorePeepHole) << "IN List of Structs";
            dict = BuildDictOverListOfStructs(input->Pos(), collection, dictKeyType, ctx);
        } else {
            YQL_CLOG(DEBUG, CorePeepHole) << "IN List";
            dict = BuildDictOverList(input->Pos(), collection, ctx);
            dictKeyType = collectionType->Cast<TListExprType>()->GetItemType();
        }
    } else if (collectionType->GetKind() == ETypeAnnotationKind::Tuple) {
        if (ansiIn && collectionType->Cast<TTupleExprType>()->GetSize()) {
            return ctx.Builder(input->Pos())
                .Callable("SqlIn")
                    .Callable(0, "AsListStrict")
                        .Add(collection->ChildrenList())
                    .Seal()
                    .Add(1, std::move(lookup))
                    .Add(2, std::move(options))
                .Seal()
                .Build();
        }
        YQL_CLOG(DEBUG, CorePeepHole) << "IN Tuple";
        dict = BuildDictOverTuple(std::move(collection), dictKeyType, ctx);
    } else if (collectionType->GetKind() == ETypeAnnotationKind::EmptyDict) {
        YQL_CLOG(DEBUG, CorePeepHole) << "IN EmptyDict";
    } else if (collectionType->GetKind() == ETypeAnnotationKind::EmptyList) {
        YQL_CLOG(DEBUG, CorePeepHole) << "IN EmptyList";
    } else {
        YQL_ENSURE(collectionType->GetKind() == ETypeAnnotationKind::Dict);
        YQL_CLOG(DEBUG, CorePeepHole) << "IN Dict";
        dict = collection;
        dictKeyType = collectionType->Cast<TDictExprType>()->GetKeyType();
    }

    const auto lookupType = lookup->GetTypeAnn();
    const auto falseNode = MakeBool<false>(input->Pos(), ctx);
    const auto justFalseNode = ctx.NewCallable(input->Pos(), "Just", { falseNode });

    if (ansiIn && !dict) {
        YQL_CLOG(DEBUG, CorePeepHole) << "ANSI IN: with statically deduced empty collection";
        return lookupType->HasOptionalOrNull() ? justFalseNode: falseNode;
    }

    TExprNode::TPtr contains = falseNode;
    if (!dictKeyType) {
        YQL_CLOG(DEBUG, CorePeepHole) << "IN: Trivial Contains() due to statically deduced empty collection";
    } else if (NUdf::ECastOptions::Impossible & CastResult<true>(lookupType, dictKeyType)) {
        YQL_CLOG(DEBUG, CorePeepHole) << "IN: Trivial Contains() due to uncompatible type of lookup (" << *lookupType
                              << ") and collection item (" << *dictKeyType << ")";
    } else {
        YQL_ENSURE(dict);
        contains = ctx.Builder(input->Pos())
            .Callable("Contains")
                .Add(0, dict)
                .Add(1, lookup)
            .Seal()
            .Build();
    }

    const bool nullableCollectionItems = IsSqlInCollectionItemsNullable(NNodes::TCoSqlIn(input));
    if (ansiIn && (nullableCollectionItems || lookupType->HasOptionalOrNull())) {
        YQL_CLOG(DEBUG, CorePeepHole) << "ANSI IN: with nullable items in collection or lookup";
        YQL_ENSURE(dict);
        const auto trueNode = MakeBool(input->Pos(), true, ctx);
        const auto nullNode = MakeNull(input->Pos(), ctx);

        const auto nakedLookupType = RemoveAllOptionals(lookupType);
        if (nakedLookupType->GetKind() == ETypeAnnotationKind::Data ||
            nakedLookupType->GetKind() == ETypeAnnotationKind::Null)
        {
            return ctx.Builder(input->Pos())
                .Callable("If")
                    .Callable(0, "HasNull")
                        .Add(0, lookup)
                    .Seal()
                    .Callable(1, "If")
                        .Callable(0, "Not")
                            .Callable(0, "HasItems")
                                .Add(0, dict)
                            .Seal()
                        .Seal()
                        .Add(1, falseNode)
                        .Add(2, nullNode)
                    .Seal()
                    .Callable(2, "If")
                        .Add(0, contains)
                        .Add(1, trueNode)
                        .Callable(2, "If")
                            .Callable(0, "HasNull")
                                .Callable(0, "DictKeys")
                                    .Add(0, dict)
                                .Seal()
                            .Seal()
                            .Add(1, nullNode)
                            .Add(2, falseNode)
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        // a IN (b1, b2, ...) -> false OR (a == b1 OR a == b2 OR ...)
        auto inViaEqualChain = ctx.Builder(input->Pos())
            .Callable("Fold")
                .Callable(0, "TakeWhileInclusive")
                    .Callable(0, "DictKeys")
                        .Add(0, dict)
                    .Seal()
                    .Lambda(1)
                        .Param("collectionItem")
                        .Callable("Coalesce")
                            .Callable(0, "!=")
                                .Arg(0, "collectionItem")
                                .Add(1, lookup)
                            .Seal()
                            .Add(1, trueNode)
                        .Seal()
                    .Seal()
                .Seal()
                .Add(1, justFalseNode)
                .Lambda(2)
                    .Param("collectionItem")
                    .Param("result")
                    .Callable("Or")
                        .Arg(0, "result")
                        .Callable(1, "==")
                            .Arg(0, "collectionItem")
                            .Add(1, lookup)
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        return ctx.Builder(input->Pos())
            .Callable("If")
                .Callable(0, "Or")
                    .Callable(0, "HasNull")
                        .Add(0, lookup)
                    .Seal()
                    .Callable(1, "HasNull")
                        .Callable(0, "DictKeys")
                            .Add(0, dict)
                        .Seal()
                    .Seal()
                .Seal()
                .Add(1, inViaEqualChain)
                .Add(2, contains)
            .Seal()
            .Build();
    }

    auto result = contains;
    if (lookupType->HasOptionalOrNull()) {
        result = ctx.Builder(input->Pos())
            .Callable("If")
                .Callable(0, "HasNull")
                    .Add(0, lookup)
                .Seal()
                .Callable(1, "Null")
                .Seal()
                .Add(2, result)
            .Seal()
            .Build();
    }

    return result;
}

template <ui8 FirstLambdaIndex = 1u, ui8 LastLambdaIndex = FirstLambdaIndex>
TExprNode::TPtr CleckClosureOnUpperLambdaOverList(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (input->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List) {
        for (auto i = FirstLambdaIndex; i <= LastLambdaIndex; ++i) {
            const auto lambda = input->Child(i);
            const auto outerLambda = lambda->GetDependencyScope()->first;
            if (outerLambda && lambda != outerLambda &&
                !SkipCallables(input->Head(), SkippableCallables).IsCallable({"Collect", "AsList", "List", "ListIf"})) {
                YQL_CLOG(DEBUG, CorePeepHole) << input->Content() << " closure on upper lambda over list";
                return ctx.ChangeChild(*input, 0U, ctx.NewCallable(input->Head().Pos(), "Collect", {input->HeadPtr()}));
            }
        }
    }

    return input;
}

template <bool Inverse = false>
TExprNode::TPtr ExpandFilter(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (ETypeAnnotationKind::Optional == input->GetTypeAnn()->GetKind()) {
        YQL_CLOG(DEBUG, CorePeepHole) << input->Content() << " over Optional";
        auto none = ctx.Builder(input->Pos())
            .Callable("Nothing")
                .Add(0, ExpandType(input->Pos(), *input->GetTypeAnn(), ctx))
            .Seal().Build();

        return ctx.Builder(input->Pos())
            .Callable("IfPresent")
                .Add(0, input->HeadPtr())
                .Lambda(1)
                    .Param("arg")
                    .Callable("If")
                        .Apply(0, input->Tail()).With(0, "arg").Seal()
                        .Add(1, Inverse ? TExprNode::TPtr(none) : input->HeadPtr())
                        .Add(2, Inverse ? input->HeadPtr() : TExprNode::TPtr(none))
                    .Seal()
                .Seal()
                .Add(2, std::move(none))
            .Seal().Build();
    }

    if (input->Head().IsCallable("NarrowMap")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << input->Content() << " with " << input->Head().Content();
        const auto width = input->Head().Tail().Head().ChildrenSize();
        auto children = input->ChildrenList();
        children[1U] = ctx.Builder(children[1U]->Pos())
            .Lambda()
                .Params("items", width)
                .Apply(*children[1U])
                    .With(0)
                        .Apply(input->Head().Tail())
                            .With("items")
                        .Seal()
                    .Done()
                .Seal()
            .Seal().Build();

        children.front() = input->Head().HeadPtr();
        auto wide = ctx.NewCallable(input->Pos(), "WideFilter", std::move(children));
        return ctx.ChangeChild(input->Head(), 0U, std::move(wide));
    }

    return input;
}

IGraphTransformer::TStatus PeepHoleCommonStage(const TExprNode::TPtr& input, TExprNode::TPtr& output,
                                               TExprContext& ctx, TTypeAnnotationContext& types,
                                               const TPeepHoleOptimizerMap& optimizers, const TExtPeepHoleOptimizerMap& extOptimizers)
{
    TOptimizeExprSettings settings(&types);
    settings.CustomInstantTypeTransformer = types.CustomInstantTypeTransformer.Get();

    return OptimizeExpr(input, output, [&optimizers, &extOptimizers, &types](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        if (const auto rule = optimizers.find(node->Content()); optimizers.cend() != rule)
            return (rule->second)(node, ctx);

        if (const auto rule = extOptimizers.find(node->Content()); extOptimizers.cend() != rule)
            return (rule->second)(node, ctx, types);

        return node;
    }, ctx, settings);
}

IGraphTransformer::TStatus PeepHoleFinalStage(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    TExprContext& ctx, TTypeAnnotationContext& types, bool* hasNonDeterministicFunctions,
    const TPeepHoleOptimizerMap& optimizers, const TExtPeepHoleOptimizerMap& extOptimizers,
    const TExtPeepHoleOptimizerMap& nonDetOptimizers)
{
    TOptimizeExprSettings settings(&types);
    settings.CustomInstantTypeTransformer = types.CustomInstantTypeTransformer.Get();

    return OptimizeExpr(input, output, [hasNonDeterministicFunctions, &types, &extOptimizers, &optimizers, &nonDetOptimizers](
        const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        if (const auto nrule = nonDetOptimizers.find(node->Content()); nonDetOptimizers.cend() != nrule) {
            if (hasNonDeterministicFunctions) {
                *hasNonDeterministicFunctions = true;
            }
            return (nrule->second)(node, ctx, types);
        }

        if (const auto xrule = extOptimizers.find(node->Content()); extOptimizers.cend() != xrule) {
            return (xrule->second)(node, ctx, types);
        }

        if (const auto rule = optimizers.find(node->Content()); optimizers.cend() != rule) {
            return (rule->second)(node, ctx);
        }

        return node;
    }, ctx, settings);
}

IGraphTransformer::TStatus PeepHoleBlockStage(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    TExprContext& ctx, TTypeAnnotationContext& types, const TExtPeepHoleOptimizerMap& extOptimizers)
{
    TOptimizeExprSettings settings(&types);
    settings.CustomInstantTypeTransformer = types.CustomInstantTypeTransformer.Get();

    return OptimizeExpr(input, output, [&types, &extOptimizers](
        const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        if (const auto xrule = extOptimizers.find(node->Content()); extOptimizers.cend() != xrule) {
            return (xrule->second)(node, ctx, types);
        }

        return node;
    }, ctx, settings);
}

template<bool FinalStage>
void AddStandardTransformers(TTransformationPipeline& pipelene, IGraphTransformer* typeAnnotator) {
    auto issueCode = TIssuesIds::CORE_EXEC;
    pipelene.AddServiceTransformers(issueCode);
    if (typeAnnotator) {
        pipelene.Add(*typeAnnotator, "TypeAnnotation", issueCode);
    } else {
        pipelene.AddTypeAnnotationTransformer(issueCode);
    }

    pipelene.AddPostTypeAnnotation(true, FinalStage, issueCode);
    pipelene.Add(TExprLogTransformer::Sync("PeepHoleOpt", NLog::EComponent::CorePeepHole, NLog::ELevel::TRACE),
        "PeepHoleOptTrace", issueCode, "PeepHoleOptTrace");
}

template<bool FlatOrMulti>
TExprNode::TPtr FuseNarrowMap(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Fuse " << node.Content() << " with " << node.Head().Content();
    return ctx.Builder(node.Pos())
        .Callable(FlatOrMulti ? "NarrowFlatMap" : "NarrowMultiMap")
            .Add(0, node.Head().HeadPtr())
            .Lambda(1)
                .Params("items", node.Head().Tail().Head().ChildrenSize())
                .Apply(node.Tail())
                    .With(0)
                        .Apply(node.Head().Tail())
                            .With("items")
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
        .Seal().Build();
}

template <bool Ordered>
TExprNode::TPtr ExpandFlatMap(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto& lambda = node->Tail();
    const auto& body = lambda.Tail();
    constexpr auto map = Ordered ? "OrderedMap" : "Map";
    if ((node->Head().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional || body.GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) &&
        body.IsCallable({"Just","AsList"}) && body.ChildrenSize() == 1U) {

        YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " to " << map;
        return ctx.Builder(node->Pos())
            .Callable(map)
                .Add(0, node->HeadPtr())
                .Lambda(1)
                    .Param("item")
                    .ApplyPartial(lambda.HeadPtr(), body.HeadPtr()).With(0, "item").Seal()
                .Seal()
            .Seal().Build();
    }

    if (const auto kind = node->Head().GetTypeAnn()->GetKind(); (kind == ETypeAnnotationKind::Flow || kind == ETypeAnnotationKind::List) &&
        body.IsCallable("AsList") && body.ChildrenSize() > 1U) {
        constexpr auto multimap = Ordered ? "OrderedMultiMap" : "MultiMap";
        YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content() << " as " << multimap << " of size " << body.ChildrenSize();
        return ctx.NewCallable(node->Pos(), multimap, {node->HeadPtr(), ctx.DeepCopyLambda(lambda, body.ChildrenList())});
    }

    if (body.IsCallable("If") && 3U == body.ChildrenSize() && 1U == body.Tail().ChildrenSize() && body.Tail().IsCallable({"List", "Nothing", "EmptyFrom"}) && (
        (1U == body.Child(1)->ChildrenSize() && body.Child(1)->IsCallable({"AsList", "Just"})) ||
        (2U == body.Child(1)->ChildrenSize() && body.Child(1)->IsCallable("List")))) {
        const bool haveSharedCallables = HaveSharedNodes(body.HeadPtr(), body.Child(1)->TailPtr(),
            [&lambda] (const TExprNode::TPtr& node) {
                if (!node->IsCallable())
                    return false;

                if (node->GetTypeAnn() && node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Type) {
                    return false;
                }

                if (node->IsCallable({"Member", "Nth", "Just", "Plus", "Minus", "Abs", "Not"}) && node->Head().IsArgument()) {
                    return false;
                }

                return node->GetDependencyScope()->second == &lambda;
            });

        if (!haveSharedCallables) {
            constexpr auto filter = Ordered ? "OrderedFilter" : "Filter";
            YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over " << body.Content() << " to " << filter;
            auto ret = ctx.Builder(node->Pos())
                .Callable(filter)
                    .Add(0, node->HeadPtr())
                    .Lambda(1)
                        .Param("item")
                        .ApplyPartial(lambda.HeadPtr(), body.HeadPtr()).With(0, "item").Seal()
                    .Seal()
                .Seal().Build();

            if (&body.Child(1)->Tail() != &lambda.Head().Head()) {
                ret = ctx.Builder(node->Pos())
                    .Callable(map)
                        .Add(0, std::move(ret))
                        .Lambda(1)
                            .Param("item")
                            .ApplyPartial(lambda.HeadPtr(), body.Child(1)->TailPtr()).With(0, "item").Seal()
                        .Seal()
                    .Seal().Build();
            }

            const bool toList = node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List
                && node->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional;
            return ctx.WrapByCallableIf(toList, "ToList", std::move(ret));
        }
    }

    if (ETypeAnnotationKind::Optional == node->GetTypeAnn()->GetKind()) {
        YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over Optional";
        return ctx.Builder(node->Pos())
            .Callable("IfPresent")
                .Add(0, node->HeadPtr())
                .Lambda(1)
                    .Param("item")
                    .Apply(node->Tail()).With(0, "item").Seal()
                .Seal()
                .Callable(2, "Nothing")
                    .Add(0, ExpandType(node->Pos(), *node->GetTypeAnn(), ctx))
                .Seal()
            .Seal()
            .Build();
    }

    if (node->Head().IsCallable("NarrowMap")) {
        return FuseNarrowMap<true>(*node, ctx);
    }

    if (body.IsCallable("NarrowMap") && !IsDepended(body.Tail().Tail(), lambda.Head().Head())) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << body.Content();
        return ctx.ChangeChild(body, 0U, ctx.ChangeChild(*node, 1U, ctx.ChangeChild(lambda, 1U, body.HeadPtr())));
    }

    return node;
}

template<bool Ordered>
TExprNode::TPtr OptimizeMultiMap(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (const auto& input = node->Head(); input.IsCallable("NarrowMap")) {
        return FuseNarrowMap<false>(*node, ctx);
    } else if (input.IsCallable({"MultiMap", "OrderedMultiMap", "NarrowMultiMap"})) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Fuse " << node->Content() << " with " << input.Content();
        return ctx.NewCallable(node->Pos(), Ordered || input.IsCallable("NarrowMultiMap") ? input.Content() : node->Content(), {input.HeadPtr(), ctx.FuseLambdas(node->Tail(), input.Tail())});
    }
    return node;
}

TExprNode::TPtr WrapIteratorForOptionalList(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (node->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Wrap " << node->Content() << " for optional list.";
        return ctx.Builder(node->Pos())
            .Callable("IfPresent")
                .Add(0, node->HeadPtr())
                .Lambda(1)
                    .Param("list")
                    .Callable(node->Content())
                        .Arg(0, "list")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (ui32 pos = 1U; pos < node->ChildrenSize(); ++pos) {
                                parent.Add(pos, node->ChildPtr(pos));
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
                .Callable(2, "EmptyIterator")
                    .Add(0, ExpandType(node->Pos(), *node->GetTypeAnn(), ctx))
                .Seal()
            .Seal().Build();
    }

    return node;
}

TExprNode::TPtr MapForOptionalContainer(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (node->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Map " << node->Content() << " for optional container.";
        return ctx.Builder(node->Pos())
            .Callable("Map")
                .Add(0, node->HeadPtr())
                .Lambda(1)
                    .Param("cont")
                    .Callable(node->Content())
                        .Arg(0, "cont")
                    .Seal()
                .Seal()
            .Seal().Build();
    }

    return node;
}

template <bool BoolResult, bool ByList = false>
TExprNode::TPtr RewriteSearchByKeyForTypesMismatch(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto type = node->Head().GetTypeAnn();
    if constexpr (BoolResult) {
        if (IsDataOrOptionalOfData(type)) {
            return node;
        }
    }

    if (type->GetKind() == ETypeAnnotationKind::Optional) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Wrap " << node->Content() << " for optional collection.";
        const auto unwrapType = type->Cast<TOptionalExprType>()->GetItemType();
        const auto payloadType = ByList ? unwrapType->Cast<TListExprType>()->GetItemType() : unwrapType->Cast<TDictExprType>()->GetPayloadType();
        return ctx.Builder(node->Pos())
            .Callable("IfPresent")
                .Add(0, node->HeadPtr())
                .Lambda(1)
                    .Param("dict")
                    .Callable(node->Content())
                        .Arg(0, "dict")
                        .Add(1, node->TailPtr())
                    .Seal()
                .Seal()
                .Add(2, BoolResult ? MakeBool<false>(node->Pos(), ctx) : MakeNothing(node->Pos(), *payloadType, ctx))
            .Seal().Build();
    }

    const auto keyType = ByList ? type->Cast<TListExprType>()->GetItemType() : type->Cast<TDictExprType>()->GetKeyType();

    if (!IsSameAnnotation(*keyType, *node->Tail().GetTypeAnn())) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Wrap " << node->Content() << " for key type mismatch.";
        const auto payloadType = ByList ? type->Cast<TListExprType>()->GetItemType() : type->Cast<TDictExprType>()->GetPayloadType();
        return ctx.Builder(node->Pos())
            .Callable("AlterTo")
                .Add(0, node->TailPtr())
                .Add(1, ExpandType(node->Pos(), *keyType, ctx))
                .Lambda(2)
                    .Param("converted")
                    .Callable(node->Content())
                        .Add(0, node->HeadPtr())
                        .Arg(1, "converted")
                    .Seal()
                .Seal()
                .Add(3, BoolResult ? MakeBool<false>(node->Pos(), ctx) : MakeNothing(node->Pos(), *payloadType, ctx))
            .Seal().Build();
    }

    return node;
}

TExprNode::TPtr ExpandListHas(const TExprNode::TPtr& input, TExprContext& ctx) {
    const auto type = input->Head().GetTypeAnn();
    if (ETypeAnnotationKind::List == type->GetKind() &&
        IsSameAnnotation(*type->Cast<TListExprType>()->GetItemType(), *input->Tail().GetTypeAnn())) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content();
        return ctx.Builder(input->Pos())
            .Callable("Exists")
                .Callable(0, "Head")
                    .Callable(0, "SkipWhile")
                        .Add(0, input->HeadPtr())
                        .Lambda(1)
                            .Param("item")
                            .Callable("AggrNotEquals")
                                .Arg(0, "item")
                                .Add(1, input->TailPtr())
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal().Build();
    }

    return RewriteSearchByKeyForTypesMismatch<true, true>(input, ctx);
}

TExprNode::TPtr ExpandPgArrayOp(const TExprNode::TPtr& input, TExprContext& ctx) {
    const bool all = input->Content() == "PgAllResolvedOp";
    auto list = ctx.Builder(input->Pos())
        .Callable("PgCall")
            .Atom(0, "unnest")
            .List(1)
            .Seal()
            .Add(2, input->ChildPtr(3))
        .Seal()
        .Build();

    auto value = ctx.Builder(input->Pos())
        .Callable("Fold")
            .Add(0, list)
            .Callable(1, "PgConst")
                .Atom(0, all ? "true" : "false")
                .Callable(1, "PgType")
                    .Atom(0, "bool")
                .Seal()
            .Seal()
            .Lambda(2)
                .Param("item")
                .Param("state")
                .Callable(all ? "PgAnd" : "PgOr")
                    .Arg(0, "state")
                    .Callable(1, "PgResolvedOp")
                        .Add(0, input->ChildPtr(0))
                        .Add(1, input->ChildPtr(1))
                        .Add(2, input->ChildPtr(2))
                        .Arg(3, "item")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    return ctx.Builder(input->Pos())
        .Callable("If")
            .Callable(0, "Exists")
                .Add(0, input->ChildPtr(3))
            .Seal()
            .Add(1, value)
            .Callable(2, "Null")
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr ExpandPgNullIf(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto pred = ctx.Builder(input->Pos())
        .Callable("Coalesce")
            .Callable(0, "FromPg")
                .Callable( 0, "PgOp")
                    .Atom(0, "=")
                    .Add(1, input->Child(0))
                    .Add(2, input->Child(1))
                .Seal()
            .Seal()
            .Callable( 1, "Bool")
                .Atom(0, "false")
            .Seal()
        .Seal().Build();
    return ctx.Builder(input->Pos())
        .Callable("If")
            .Add(0, pred)
            .Callable(1, "Null").Seal()
            .Add(2, input->Child(0))
        .Seal().Build();
}

template <bool Flat, bool List>
TExprNode::TPtr ExpandContainerIf(const TExprNode::TPtr& input, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << input->Content();
    auto item = Flat ? input->TailPtr() : ctx.NewCallable(input->Tail().Pos(), List ? "AsList" : "Just", {input->TailPtr()});
    auto none = ctx.NewCallable(input->Tail().Pos(), "EmptyFrom", {item});
    return ctx.NewCallable(input->Pos(), "If", {input->HeadPtr(), std::move(item), std::move(none)});
}

TExprNode::TPtr DropDependsOnFromEmptyIterator(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (input->ChildrenSize() > 1) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Drop DependsOn from " << input->Content();
        TExprNode::TListType newChildren;
        newChildren.push_back(input->Child(0));
        return ctx.ChangeChildren(*input, std::move(newChildren));
    }
    return input;
}

TExprNode::TPtr ExpandVersion(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand Version";
    const TString branch(GetBranch());
    TString result;
    auto pos = branch.rfind("/");
    if (pos != TString::npos) {
        result = branch.substr(branch.rfind("/") + 1);
    }
    if (result.empty()) {
        result = "unknown";
    }
    return ctx.Builder(node->Pos())
        .Callable("String")
            .Atom(0, result)
        .Seal().Build();
}

TExprNode::TPtr ExpandPartitionsByKeys(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();
    const bool isStream = node->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow ||
        node->Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream;
    TExprNode::TPtr sort;
    const auto keyExtractor = node->Child(TCoPartitionsByKeys::idx_KeySelectorLambda);
    const bool isConstKey = !IsDepended(keyExtractor->Tail(), keyExtractor->Head().Head());
    const bool haveSort = node->Child(TCoPartitionsByKeys::idx_SortKeySelectorLambda)->IsLambda();

    auto sortLambda =  ctx.Builder(node->Pos())
        .Lambda()
            .Param("x")
            .Callable("Sort")
                .Arg(0, "x")
                .Add(1, node->ChildPtr(TCoPartitionsByKeys::idx_SortDirections))
                .Add(2, node->ChildPtr(TCoPartitionsByKeys::idx_SortKeySelectorLambda))
            .Seal()
        .Seal()
        .Build();

    auto settings = ctx.Builder(node->Pos())
        .List()
            .Atom(0, "Auto", TNodeFlags::Default)
            .Atom(1, "Many", TNodeFlags::Default)
        .Seal()
        .Build();

    auto flatten = ctx.Builder(node->Pos())
        .Lambda()
            .Param("dict")
            .Callable("OrderedFlatMap")
                .Callable(0, "DictPayloads")
                    .Arg(0, "dict")
                .Seal()
                .Add(1, haveSort ? sortLambda : MakeIdentityLambda(node->Pos(), ctx))
            .Seal()
        .Seal()
        .Build();

    if (isConstKey) {
        if (haveSort) {
            sort = ctx.Builder(node->Pos())
                .Callable("Sort")
                    .Add(0, node->HeadPtr())
                    .Add(1, node->ChildPtr(TCoPartitionsByKeys::idx_SortDirections))
                    .Add(2, node->ChildPtr(TCoPartitionsByKeys::idx_SortKeySelectorLambda))
                .Seal()
                .Build();
        } else {
            sort = node->HeadPtr();
        }
    } else {
        if (isStream) {
            sort = ctx.Builder(node->Pos())
                .Callable("OrderedFlatMap")
                    .Callable(0, "SqueezeToDict")
                        .Add(0, node->HeadPtr())
                        .Add(1, node->ChildPtr(TCoPartitionsByKeys::idx_KeySelectorLambda))
                        .Add(2, MakeIdentityLambda(node->Pos(), ctx))
                        .Add(3, std::move(settings))
                    .Seal()
                    .Add(1, std::move(flatten))
                .Seal()
                .Build();
        } else {
            sort = ctx.Builder(node->Pos())
                .Apply(*flatten)
                    .With(0)
                        .Callable("ToDict")
                            .Add(0, node->HeadPtr())
                            .Add(1, node->ChildPtr(TCoPartitionsByKeys::idx_KeySelectorLambda))
                            .Add(2, MakeIdentityLambda(node->Pos(), ctx))
                            .Add(3, std::move(settings))
                        .Seal()
                    .Done()
                .Seal()
                .Build();
        }

        if (auto keys = GetPathsToKeys(keyExtractor->Tail(), keyExtractor->Head().Head()); !keys.empty()) {
            if (const auto sortKeySelector = node->Child(TCoPartitionsByKeys::idx_SortKeySelectorLambda); sortKeySelector->IsLambda()) {
                auto sortKeys = GetPathsToKeys(sortKeySelector->Tail(), sortKeySelector->Head().Head());
                std::move(sortKeys.begin(), sortKeys.end(), std::back_inserter(keys));
                std::sort(keys.begin(), keys.end());
            }

            TExprNode::TListType columns;
            columns.reserve(keys.size());
            for (const auto& path : keys) {
                if (1U == path.size())
                    columns.emplace_back(ctx.NewAtom(node->Pos(), path.front()));
                else {
                    TExprNode::TListType atoms(path.size());
                    std::transform(path.cbegin(), path.cend(), atoms.begin(), [&](const std::string_view& name) { return ctx.NewAtom(node->Pos(), name); });
                    columns.emplace_back(ctx.NewList(node->Pos(), std::move(atoms)));
                }
            }

            sort = ctx.Builder(node->Pos())
                .Callable("AssumeChopped")
                    .Add(0, std::move(sort))
                    .List(1).Add(std::move(columns)).Seal()
                .Seal()
                .Build();
        }
    }

    return ctx.ReplaceNode(node->Tail().TailPtr(), node->Tail().Head().Head(), std::move(sort));
}

TExprNode::TPtr ExpandIsKeySwitch(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();
    return ctx.Builder(node->Pos())
        .Callable("AggrNotEquals")
            .Add(0, ctx.ReplaceNode(node->Child(2)->TailPtr(), node->Child(2)->Head().Head(), node->ChildPtr(0)))
            .Add(1, ctx.ReplaceNode(node->Child(3)->TailPtr(), node->Child(3)->Head().Head(), node->ChildPtr(1)))
        .Seal().Build();
}

TExprNode::TPtr ExpandMux(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();
    const auto varType = ExpandType(node->Pos(), *GetSeqItemType(node->GetTypeAnn()), ctx);
    TExprNode::TListType lists;
    switch (node->Head().GetTypeAnn()->GetKind()) {
        case ETypeAnnotationKind::Tuple: {
            const auto type = node->Head().GetTypeAnn()->Cast<TTupleExprType>();
            lists.resize(type->GetSize());
            for (ui32 i = 0U; i < lists.size(); ++i) {
                lists[i] = ctx.Builder(node->Head().Pos())
                    .Callable("OrderedMap")
                        .Callable(0, "Nth")
                            .Add(0, node->HeadPtr())
                            .Atom(1, i)
                        .Seal()
                        .Lambda(1)
                            .Param("item")
                            .Callable("Variant")
                                .Arg(0, "item")
                                .Atom(1, i)
                                .Add(2, varType)
                            .Seal()
                        .Seal()
                    .Seal().Build();
            }
            return ctx.NewCallable(node->Pos(), "Extend", std::move(lists));
        }
        case ETypeAnnotationKind::Struct: {
            const auto type = node->Head().GetTypeAnn()->Cast<TStructExprType>();
            lists.reserve(type->GetSize());
            for (const auto& item : type->GetItems()) {
                lists.emplace_back(
                    ctx.Builder(node->Head().Pos())
                        .Callable("OrderedMap")
                            .Callable(0, "Member")
                                .Add(0, node->HeadPtr())
                                .Atom(1, item->GetName())
                            .Seal()
                            .Lambda(1)
                                .Param("item")
                                .Callable("Variant")
                                    .Arg(0, "item")
                                    .Atom(1, item->GetName())
                                    .Add(2, varType)
                                .Seal()
                            .Seal()
                        .Seal().Build()
                    );
            }
            return ctx.NewCallable(node->Pos(), "Extend", std::move(lists));
        }
        default: break;
    }

    return node;
}

TExprNode::TPtr ExpandLMapOrShuffleByKeys(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();
    return ctx.Builder(node->Pos())
        .Callable("Collect")
            .Apply(0, node->Tail())
                .With(0)
                    .Callable("Iterator")
                        .Add(0, node->HeadPtr())
                    .Seal()
                .Done()
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr ExpandDemux(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();
    TExprNode::TListType lists;
    switch (node->GetTypeAnn()->GetKind()) {
        case ETypeAnnotationKind::Tuple: {
            const auto type = node->GetTypeAnn()->Cast<TTupleExprType>();
            lists.resize(type->GetSize());
            for (ui32 i = 0U; i < lists.size(); ++i) {
                lists[i] = ctx.Builder(node->Head().Pos())
                    .Callable("OrderedFlatMap")
                        .Add(0, node->HeadPtr())
                        .Lambda(1)
                            .Param("item")
                            .Callable("Guess")
                                .Arg(0, "item")
                                .Atom(1, i)
                            .Seal()
                        .Seal()
                    .Seal().Build();
            }
            return ctx.NewList(node->Pos(), std::move(lists));
        }
        case ETypeAnnotationKind::Struct: {
            const auto type = node->GetTypeAnn()->Cast<TStructExprType>();
            lists.reserve(type->GetSize());
            for (const auto& item : type->GetItems()) {
                lists.emplace_back(
                    ctx.Builder(node->Head().Pos())
                        .List()
                            .Atom(0, item->GetName())
                            .Callable(1, "OrderedFlatMap")
                                .Add(0, node->HeadPtr())
                                .Lambda(1)
                                    .Param("item")
                                    .Callable("Guess")
                                        .Arg(0, "item")
                                        .Atom(1, item->GetName())
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal().Build()
                    );
            }
            return ctx.NewCallable(node->Pos(), "AsStruct", std::move(lists));
        }
        default: break;
    }

    return node;
}

TExprNode::TPtr ExpandOptionalReduce(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();
    return ctx.Builder(node->Pos())
        .Callable("IfPresent")
            .Add(0, node->ChildPtr(0))
            .Lambda(1)
                .Param("lhs")
                .Callable("IfPresent")
                    .Add(0, node->ChildPtr(1))
                    .Lambda(1)
                        .Param("rhs")
                        .Callable("Just")
                            .Apply(0, node->Tail())
                                .With(0, "lhs")
                                .With(1, "rhs")
                            .Seal()
                        .Seal()
                    .Seal()
                    .Add(2, node->ChildPtr(0))
                .Seal()
            .Seal()
            .Add(2, node->ChildPtr(1))
        .Seal().Build();
}

template <bool MinOrMax>
TExprNode::TPtr ExpandAggrMinMax(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (IsDataOrOptionalOfData(node->GetTypeAnn())) {
        return node;
    }

    if (ETypeAnnotationKind::Optional == node->GetTypeAnn()->GetKind()) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content() << " over Optional";
        return ctx.Builder(node->Pos())
            .Callable("OptionalReduce")
                .Add(0, node->HeadPtr())
                .Add(1, node->TailPtr())
                .Lambda(2)
                    .Param("lhs")
                    .Param("rhs")
                    .Callable(node->Content())
                        .Arg(0, "lhs")
                        .Arg(1, "rhs")
                    .Seal()
                .Seal()
            .Seal().Build();
    }

    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();
    return ctx.Builder(node->Pos())
        .Callable("If")
            .Callable(0, MinOrMax ? "AggrLessOrEqual" : "AggrGreaterOrEqual")
                .Add(0, node->HeadPtr())
                .Add(1, node->TailPtr())
            .Seal()
            .Add(1, node->HeadPtr())
            .Add(2, node->TailPtr())
        .Seal().Build();
}

template<typename T>
TExprNode::TPtr DoExpandMinMax(TStringBuf name, bool usePlainCompare, TPositionHandle pos, T argsBegin, T argsEnd, TExprContext& ctx) {
    const size_t size = argsEnd - argsBegin;
    YQL_ENSURE(size > 0);
    if (size == 1) {
        return *argsBegin;
    }

    TExprNode::TPtr left;
    TExprNode::TPtr right;

    if (size > 2) {
        const size_t half = size / 2;
        left = DoExpandMinMax(name, usePlainCompare, pos, argsBegin, argsBegin + half, ctx);
        right = DoExpandMinMax(name, usePlainCompare, pos, argsBegin + half, argsEnd, ctx);
    } else {
        left = *argsBegin;
        right = *(argsBegin + 1);
    }

    return ctx.Builder(pos)
        .Callable("If")
            .Callable(0, usePlainCompare ? (name == "Min" ? "<=" : ">=") :
                                           (name == "Min" ? "AggrLessOrEqual" : "AggrGreaterOrEqual"))
                .Add(0, left)
                .Add(1, right)
            .Seal()
            .Add(1, left)
            .Add(2, right)
        .Seal()
        .Build();
}

TExprNode::TPtr ExpandMinMax(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_ENSURE(node->ChildrenSize() >= 2);
    if (IsDataOrOptionalOfData(node->GetTypeAnn())) {
        return node;
    }

    const auto& children = node->ChildrenList();
    if (!node->GetTypeAnn()->IsOptionalOrNull()) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content() << " with complex non-nullable types";
        return DoExpandMinMax(node->Content(), true, node->Pos(), children.begin(), children.end(), ctx);
    }

    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content() << " with complex nullable types";
    TExprNodeList childrenWithPositions;
    for (ui32 i = 0; i < children.size(); ++i) {
        const auto& child = children[i];
        childrenWithPositions.push_back(ctx.Builder(child->Pos())
            .List()
                .Add(0, child)
                .Callable(1, "Uint32")
                    .Atom(0, i)
                .Seal()
            .Seal()
            .Build()
        );
    }
    auto candidateTuple = DoExpandMinMax(node->Content(), false, node->Pos(), childrenWithPositions.begin(), childrenWithPositions.end(), ctx);

    TExprNodeList compareWithCandidate;
    for (ui32 i = 0; i < children.size(); ++i) {
        const auto& child = children[i];
        compareWithCandidate.push_back(ctx.Builder(child->Pos())
            .Callable("If")
                .Callable(0, "==")
                    .Callable(0, "Nth")
                        .Add(0, candidateTuple)
                        .Atom(1, 1)
                    .Seal()
                    .Callable(1, "Uint32")
                        .Atom(0, i)
                    .Seal()
                .Seal()
                .Add(1, MakeBool<true>(child->Pos(), ctx))
                .Callable(2, node->IsCallable("Min") ? "<=" : ">=")
                    .Callable(0, "Nth")
                        .Add(0, candidateTuple)
                        .Atom(1, 0)
                    .Seal()
                    .Add(1, child)
                .Seal()
            .Seal()
            .Build()
        );
    }

    return ctx.Builder(node->Pos())
        .Callable("If")
            .Callable(0, "Coalesce")
                .Add(0, ctx.NewCallable(node->Pos(), "And", std::move(compareWithCandidate)))
                .Add(1, MakeBool<false>(node->Pos(), ctx))
            .Seal()
            .Callable(1, "Nth")
                .Add(0, candidateTuple)
                .Atom(1, 0)
            .Seal()
            .Callable(2, "Nothing")
                .Add(0, ExpandType(node->Pos(), *node->GetTypeAnn(), ctx))
            .Seal()
        .Seal()
        .Build();
}

template <bool Ordered>
TExprNode::TPtr OptimizeMap(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto& arg = node->Tail().Head().Head();
    if (!arg.IsUsedInDependsOn() && ETypeAnnotationKind::Optional == node->GetTypeAnn()->GetKind()) {
        YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over Optional";
        return ctx.Builder(node->Pos())
            .Callable("IfPresent")
                .Add(0, node->HeadPtr())
                .Lambda(1)
                    .Param("item")
                    .Callable("Just")
                        .Apply(0, node->TailPtr()).With(0, "item").Seal()
                    .Seal()
                .Seal()
                .Callable(2, "Nothing")
                    .Add(0, ExpandType(node->Pos(), *node->GetTypeAnn(), ctx))
                .Seal()
            .Seal()
            .Build();
    }

    if (const auto& input = node->Head(); !arg.IsUsedInDependsOn() && input.IsCallable("AsList")) {
        TNodeSet uniqueItems(input.ChildrenSize());
        input.ForEachChild([&uniqueItems](const TExprNode& item){ uniqueItems.emplace(&item); });
        if (uniqueItems.size() < 0x10U) {
            YQL_CLOG(DEBUG, CorePeepHole) << "Eliminate " << node->Content() << " over list of " << uniqueItems.size();
            auto list = input.ChildrenList();
            for (auto& item : list) {
                item = ctx.ReplaceNode(node->Tail().TailPtr(), arg, std::move(item));
            }
            return ctx.ChangeChildren(input, std::move(list));
        }
    }

    if (node->Head().IsCallable("NarrowMap")) {
        if (!arg.IsUsedInDependsOn()) {
            YQL_CLOG(DEBUG, CorePeepHole) << "Fuse " << node->Content() << " with " << node->Head().Content();
            const auto width = node->Head().Tail().Head().ChildrenSize();
            auto lambda = ctx.Builder(node->Pos())
                .Lambda()
                    .Params("items", width)
                    .Apply(node->Tail())
                        .With(0)
                            .Apply(node->Head().Tail())
                                .With("items")
                            .Seal()
                        .Done()
                    .Seal()
                .Seal().Build();
            return ctx.ChangeChild(node->Head(), 1U, std::move(lambda));
        }
    }

    if (1U == node->Head().UseCount() && !arg.IsUsedInDependsOn()) {
        if (node->Head().IsCallable({"Map", "OrderedMap"})) {
            YQL_CLOG(DEBUG, CorePeepHole) << "Fuse " << node->Content() << " over " << node->Head().Content();
            auto lambda = ctx.Builder(node->Pos())
                    .Lambda()
                        .Param("it")
                        .Apply(node->Tail())
                            .With(0)
                                .Apply(node->Head().Tail())
                                    .With(0, "it")
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal().Build();
            return ctx.ChangeChildren(Ordered ? node->Head() : *node, {node->Head().HeadPtr(), std::move(lambda)});
        }
    }
    return node;
}

TExprNode::TPtr OptimizeSkip(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (const auto& input = node->Head(); input.IsCallable({"Map", "OrderedMap", "ExpandMap", "WideMap", "NarrowMap"})) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << input.Content();
        return ctx.SwapWithHead(*node);
    }

    return node;
}

TExprNode::TPtr OptimizeTake(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (const auto& input = node->Head(); input.IsCallable({"Filter", "OrderedFilter", "WideFilter"})) {
        if (2U == input.ChildrenSize()) {
            YQL_CLOG(DEBUG, CorePeepHole) << "Inject " << node->Content() << " limit into " << input.Content();
            auto list = input.ChildrenList();
            list.emplace_back(node->TailPtr());
            return ctx.ChangeChildren(input, std::move(list));
        }

        auto childLimit = input.ChildPtr(2);
        auto myLimit = node->ChildPtr(1);
        YQL_CLOG(DEBUG, CorePeepHole) << "Merge " << node->Content() << " limit into " << input.Content();
        return ctx.ChangeChild(input, 2, ctx.NewCallable(node->Pos(), "Min", { myLimit, childLimit }));
    }

    if (const auto& input = node->Head(); 1U == input.UseCount()) {
        if (input.IsCallable("Sort")) {
            YQL_CLOG(DEBUG, CorePeepHole) << "Fuse " << node->Content() << " over " << input.Content();
            auto children = input.ChildrenList();
            auto it = children.cbegin();
            children.emplace(++it, node->TailPtr());
            return ctx.NewCallable(node->Pos(), "TopSort", std::move(children));
        } else if (input.IsCallable({"Top", "TopSort"})) {
            YQL_CLOG(DEBUG, CorePeepHole) << "Fuse " << node->Content() << " over " << input.Content();
            return ctx.ChangeChild(input, 1U, ctx.NewCallable(node->Pos(), "Min", {node->TailPtr(), input.ChildPtr(1)}));
        }
    }

    return OptimizeSkip(node, ctx);
}

TExprNode::TPtr ExpandAsSet(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();
    return ctx.Builder(node->Pos())
        .Callable("AsDict")
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                ui32 pos = 0;
                for (auto child : node->Children()) {
                    parent.List(pos++)
                       .Add(0, child)
                       .Callable(1, "Void").Seal()
                    .Seal();
                }
                return parent;
            })
        .Seal()
        .Build();
}

ui32 GetCommonPartWidth(const TExprNode& lhs, const TExprNode& rhs) {
    ui32 c = 0U;
    while (c < lhs.ChildrenSize() && c < rhs.ChildrenSize() && lhs.Child(c) == rhs.Child(c)) {
        ++c;
    }
    return c;
}

template<bool AndOr>
TExprNode::TPtr OptimizeLogicalDups(const TExprNode::TPtr& node, TExprContext& ctx) {
    auto children = node->ChildrenList();
    const auto opposite = AndOr ? "Or" : "And";
    for (auto curr = children.begin(); children.cend() != curr;) {
        if (const auto next = curr + 1U; children.cend() != next) {
            if ((*curr)->IsCallable(opposite) && (*next)->IsCallable(opposite)) {
                if (const auto common = GetCommonPartWidth(**curr, **next)) {
                    if ((*next)->ChildrenSize() == common) {
                        curr = children.erase(curr);
                    } else if ((*curr)->ChildrenSize() == common) {
                        children.erase(next);
                    } else {
                        auto childrenOne = (*curr)->ChildrenList();
                        auto childrenTwo = (*next)->ChildrenList();

                        TExprNode::TListType newChildren(common + 1U);
                        std::move(childrenOne.begin(), childrenOne.begin() + common, newChildren.begin());

                        childrenOne.erase(childrenOne.cbegin(), childrenOne.cbegin() + common);
                        childrenTwo.erase(childrenTwo.cbegin(), childrenTwo.cbegin() + common);

                        auto one = 1U == childrenOne.size() ? std::move(childrenOne.front()) : ctx.ChangeChildren(**curr, std::move(childrenOne));
                        auto two = 1U == childrenTwo.size() ? std::move(childrenTwo.front()) : ctx.ChangeChildren(**next, std::move(childrenTwo));

                        newChildren.back() = ctx.ChangeChildren(*node, {std::move(one), std::move(two)});
                        *curr = ctx.ChangeChildren(**curr, std::move(newChildren));
                        children.erase(next);
                    }
                    continue;
                }
            } else if ((*curr)->IsCallable(opposite) && &(*curr)->Head() == next->Get()) {
                curr = children.erase(curr);
                continue;
            } else if ((*next)->IsCallable(opposite) && &(*next)->Head() == curr->Get()) {
                children.erase(next);
                continue;
            } else if ((*curr)->IsCallable() && (*next)->IsCallable() && (*curr)->Content() == (*next)->Content()
                && ((*next)->Content().ends_with("Map") || ((*curr)->IsCallable("IfPresent") && &(*curr)->Tail() == &(*next)->Tail()))
                && &(*curr)->Head() == &(*next)->Head()) {
                auto lambda = ctx.Builder(node->Pos())
                    .Lambda()
                        .Param("arg")
                        .Callable(node->Content())
                            .Apply(0, *(*curr)->Child(1U)).With(0, "arg").Seal()
                            .Apply(1, *(*next)->Child(1U)).With(0, "arg").Seal()
                        .Seal()
                    .Seal().Build();
                *curr = ctx.ChangeChild(**curr, 1U, std::move(lambda));
                children.erase(next);
                continue;
            }
        }
        ++curr;
    }

    if (children.size() < node->ChildrenSize()) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Dedup " << node->ChildrenSize() - children.size() << " common parts of " << opposite << "'s under " << node->Content();
        return 1U == children.size() ? children.front() : ctx.ChangeChildren(*node, std::move(children));
    }

    return node;
}

TExprNode::TPtr ExpandCombineByKey(const TExprNode::TPtr& node, TExprContext& ctx) {
    const bool isStreamOrFlow = node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream ||
        node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow;

    if (!isStreamOrFlow) {
        return node;
    }

    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content() << " over stream or flow";

    TCoCombineByKey combine(node);

    return Build<TCoCombineCore>(ctx, node->Pos())
        .Input<TCoFlatMap>()
            .Input(combine.Input())
            .Lambda()
                .Args({"arg"})
                .Body<TExprApplier>()
                    .Apply(combine.PreMapLambda())
                    .With(0, "arg")
                    .Build()
                .Build()
            .Build()
        .KeyExtractor(combine.KeySelectorLambda())
        .InitHandler(combine.InitHandlerLambda())
        .UpdateHandler(combine.UpdateHandlerLambda())
        .FinishHandler(combine.FinishHandlerLambda())
        .MemLimit()
            .Value("0")
            .Build()
        .Done()
        .Ptr();
}

template<typename TRowType>
TExprNode::TPtr MakeWideMapJoinCore(const TExprNode& mapjoin, TExprNode::TPtr&& input, TExprContext& ctx) {
    const auto inStructType = GetSeqItemType(mapjoin.Head().GetTypeAnn())->Cast<TRowType>();
    const auto outStructType = GetSeqItemType(mapjoin.GetTypeAnn())->Cast<TRowType>();

    TExprNode::TListType indexes;
    indexes.reserve(mapjoin.Child(3)->ChildrenSize());
    mapjoin.Child(3)->ForEachChild([&](const TExprNode& item){
        indexes.emplace_back(ctx.NewAtom(item.Pos(), *GetFieldPosition(*inStructType, item.Content())));
    });

    TExprNode::TListType leftRenames;
    leftRenames.reserve(mapjoin.Child(5)->ChildrenSize());
    bool split = false;
    mapjoin.Child(5)->ForEachChild([&](const TExprNode& item){
        leftRenames.emplace_back(ctx.NewAtom(item.Pos(), *GetFieldPosition(*((split = !split) ? inStructType : outStructType), item.Content())));
    });

    auto rightRenames = mapjoin.Child(6)->ChildrenList();

    for (auto i = 1U; i < rightRenames.size(); ++++i)
        rightRenames[i] = ctx.NewAtom(rightRenames[i]->Pos(), *GetFieldPosition(*outStructType, rightRenames[i]->Content()));

    auto children = mapjoin.ChildrenList();

    children.front() = std::move(input);
    children[3] = ctx.ChangeChildren(*children[3], std::move(indexes));
    children[5] = ctx.ChangeChildren(*children[5], std::move(leftRenames));
    children[6] = ctx.ChangeChildren(*children[6], std::move(rightRenames));

    return ctx.ChangeChildren(mapjoin, std::move(children));
}

template<typename TRowType>
std::pair<TExprNode::TPtr, TExprNode::TListType> MakeWideCommonJoinCore(const TExprNode& commonJoin, TExprNode::TPtr&& input, TExprContext& ctx) {
    const auto inStructType = GetSeqItemType(commonJoin.Head().GetTypeAnn())->Cast<TRowType>();

    TExprNode::TListType leftColumns, rightColumns, requred, keys, outputColumns;
    outputColumns.reserve(commonJoin.Child(2)->ChildrenSize() + commonJoin.Child(3)->ChildrenSize());

    leftColumns.reserve(commonJoin.Child(2)->ChildrenSize());
    for (auto& item :  commonJoin.Child(2)->ChildrenList()) {
        leftColumns.emplace_back(ctx.NewAtom(item->Pos(), *GetFieldPosition(*inStructType, item->Content())));
        outputColumns.emplace_back(std::move(item));
    }

    rightColumns.reserve(commonJoin.Child(3)->ChildrenSize());
    for (auto& item :  commonJoin.Child(3)->ChildrenList()) {
        rightColumns.emplace_back(ctx.NewAtom(item->Pos(),*GetFieldPosition(*inStructType, item->Content())));
        outputColumns.emplace_back(std::move(item));
    }

    requred.reserve(commonJoin.Child(4)->ChildrenSize());
    commonJoin.Child(4)->ForEachChild([&](const TExprNode& item){
        requred.emplace_back(ctx.NewAtom(item.Pos(), *GetFieldPosition(*inStructType, item.Content())));
    });

    keys.reserve(commonJoin.Child(5)->ChildrenSize());
    commonJoin.Child(5)->ForEachChild([&](const TExprNode& item){
        keys.emplace_back(ctx.NewAtom(item.Pos(), *GetFieldPosition(*inStructType, item.Content())));
    });

    auto children = commonJoin.ChildrenList();

    children.front() = std::move(input);
    children[2] = ctx.ChangeChildren(*children[2], std::move(leftColumns));
    children[3] = ctx.ChangeChildren(*children[3], std::move(rightColumns));
    children[4] = ctx.ChangeChildren(*children[4], std::move(requred));
    children[5] = ctx.ChangeChildren(*children[5], std::move(keys));
    children.back() = ctx.NewAtom(commonJoin.Tail().Pos(), *GetFieldPosition(*inStructType, commonJoin.Tail().Content()));

    return {ctx.ChangeChildren(commonJoin, std::move(children)), std::move(outputColumns)};
}

ui32 CollectStateNodes(const TExprNode& initLambda, const TExprNode& updateLambda, TExprNode::TListType& fields, TExprNode::TListType& init, TExprNode::TListType& update, TExprContext& ctx) {
    YQL_ENSURE(IsSameAnnotation(*initLambda.Tail().GetTypeAnn(), *updateLambda.Tail().GetTypeAnn()), "Must be same type.");

    if (ETypeAnnotationKind::Struct != initLambda.Tail().GetTypeAnn()->GetKind()) {
        fields.clear();
        init = TExprNode::TListType(1U, initLambda.TailPtr());
        update = TExprNode::TListType(1U, updateLambda.TailPtr());
        return 1U;
    }

    const auto structType = initLambda.Tail().GetTypeAnn()->Cast<TStructExprType>();
    const auto size = structType->GetSize();
    fields.reserve(size);
    init.reserve(size);
    update.reserve(size);
    fields.clear();
    init.clear();
    update.clear();

    if (initLambda.Tail().IsCallable("AsStruct"))
        initLambda.Tail().ForEachChild([&](const TExprNode& child) { fields.emplace_back(child.HeadPtr()); });
    else if (updateLambda.Tail().IsCallable("AsStruct"))
        updateLambda.Tail().ForEachChild([&](const TExprNode& child) { fields.emplace_back(child.HeadPtr()); });
    else
        for (const auto& item : structType->GetItems())
            fields.emplace_back(ctx.NewAtom(initLambda.Tail().Pos(), item->GetName()));

    if (initLambda.Tail().IsCallable("AsStruct"))
        initLambda.Tail().ForEachChild([&](const TExprNode& child) { init.emplace_back(child.TailPtr()); });
    else
        std::transform(fields.cbegin(), fields.cend(), std::back_inserter(init), [&](const TExprNode::TPtr& name) {
            return ctx.NewCallable(name->Pos(), "Member", {initLambda.TailPtr(), name});
        });

    if (updateLambda.Tail().IsCallable("AsStruct"))
        updateLambda.Tail().ForEachChild([&](const TExprNode& child) { update.emplace_back(child.TailPtr()); });
    else
        std::transform(fields.cbegin(), fields.cend(), std::back_inserter(update), [&](const TExprNode::TPtr& name) {
            return ctx.NewCallable(name->Pos(), "Member", {updateLambda.TailPtr(), name});
        });

    return size;
}

TExprNode::TPtr ExpandFinalizeByKey(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List) {
        return ctx.NewCallable(node->Pos(), "Collect",
            { ctx.ChangeChild(*node, 0, ctx.NewCallable(node->Pos(), "ToFlow", { node->HeadPtr() })) });
    }
    if (node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream) {
        return ctx.NewCallable(node->Pos(), "FromFlow",
            { ctx.ChangeChild(*node, 0, ctx.NewCallable(node->Pos(), "ToFlow", { node->HeadPtr() })) });
    }

    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content() << " over stream or flow";

    TCoFinalizeByKey combine(node);

    const auto inputStructType = GetSeqItemType(combine.PreMapLambda().Body().Ref().GetTypeAnn())->Cast<TStructExprType>();
    const auto inputWidth = inputStructType->GetSize();

    TExprNode::TListType inputFields;
    inputFields.reserve(inputWidth);
    for (const auto& item : inputStructType->GetItems()) {
        inputFields.emplace_back(ctx.NewAtom(combine.PreMapLambda().Pos(), item->GetName()));
    }

    TExprNode::TListType stateFields, init, update, outputFields;
    const auto stateWidth = CollectStateNodes(combine.InitHandlerLambda().Ref(), combine.UpdateHandlerLambda().Ref(), stateFields, init, update, ctx);

    auto output = combine.FinishHandlerLambda().Body().Ptr();
    const auto outputStructType = GetSeqItemType(node->GetTypeAnn())->Cast<TStructExprType>();
    const ui32 outputWidth = outputStructType ? outputStructType->GetSize() : 1;
    TExprNode::TListType finish;
    finish.reserve(outputWidth);
    if (output->IsCallable("AsStruct")) {
        output->ForEachChild([&](const TExprNode& child) {
            outputFields.emplace_back(child.HeadPtr());
            finish.emplace_back(child.TailPtr());
        });
    } else if (outputStructType) {
        for (const auto& item : outputStructType->GetItems()) {
            outputFields.emplace_back(ctx.NewAtom(output->Pos(), item->GetName()));
            finish.emplace_back(ctx.NewCallable(output->Pos(), "Member", { output, outputFields.back() }));
        }
    } else {
        finish.emplace_back(output);
    }

    const auto uniteToStructure = [&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
        for (ui32 i = 0U; i < inputWidth; ++i) {
            parent
                .List(i)
                    .Add(0, inputFields[i])
                    .Arg(1, "items", i)
                .Seal();
        }
        return parent;
    };

    return ctx.Builder(node->Pos())
        .Callable("NarrowMap")
            .Callable(0, "WideCombiner")
                .Callable(0, "ExpandMap")
                    .Callable(0, "FlatMap")
                        .Add(0, combine.Input().Ptr())
                        .Add(1, combine.PreMapLambda().Ptr())
                    .Seal()
                    .Lambda(1)
                        .Param("item")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (ui32 i = 0U; i < inputWidth; ++i) {
                                parent.Callable(i, "Member")
                                    .Arg(0, "item")
                                    .Add(1, inputFields[i])
                                .Seal();
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
                .Atom(1, "")
                .Lambda(2)
                    .Params("items", inputWidth)
                    .Apply(combine.KeySelectorLambda().Ref())
                        .With(0)
                            .Callable("AsStruct")
                                .Do(uniteToStructure)
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
                .Lambda(3)
                    .Param("key")
                    .Params("items", inputWidth)
                    .ApplyPartial(combine.InitHandlerLambda().Args().Ptr(), std::move(init))
                        .With(0, "key")
                        .With(1)
                            .Callable("AsStruct")
                                .Do(uniteToStructure)
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
                .Lambda(4)
                    .Param("key")
                    .Params("items", inputWidth)
                    .Params("state", stateWidth)
                    .ApplyPartial(combine.UpdateHandlerLambda().Args().Ptr(), std::move(update))
                        .With(0)
                            .Arg("key")
                        .Done()
                        .With(1)
                            .Callable("AsStruct")
                                .Do(uniteToStructure)
                            .Seal()
                        .Done()
                        .With(2)
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                if (stateFields.empty())
                                    parent.Arg("state", 0);
                                else {
                                    auto str = parent.Callable("AsStruct");
                                    for (ui32 i = 0U; i < stateWidth; ++i) {
                                        str.List(i)
                                            .Add(0, stateFields[i])
                                            .Arg(1, "state", i)
                                        .Seal();
                                    }
                                    str.Seal();
                                }
                                return parent;
                            })
                        .Done()
                    .Seal()
                .Seal()
                .Lambda(5)
                    .Param("key")
                    .Params("state", stateWidth)
                    .ApplyPartial(combine.FinishHandlerLambda().Args().Ptr(), std::move(finish))
                        .With(0, "key")
                        .With(1)
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                if (stateFields.empty())
                                    parent.Arg("state", 0);
                                else {
                                    auto str = parent.Callable("AsStruct");
                                    for (ui32 i = 0U; i < stateWidth; ++i) {
                                        str.List(i)
                                            .Add(0, std::move(stateFields[i]))
                                            .Arg(1, "state", i)
                                        .Seal();
                                    }
                                    str.Seal();
                                }
                                return parent;
                            })
                        .Done()
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(1)
                .Params("items", outputWidth)
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    if (outputFields.empty())
                        parent.Arg("items", 0);
                    else {
                        auto str = parent.Callable("AsStruct");
                        for (ui32 i = 0U; i < outputWidth; ++i) {
                            str.List(i)
                                .Add(0, std::move(outputFields[i]))
                                .Arg(1, "items", i)
                            .Seal();
                        }
                        str.Seal();
                    }
                    return parent;
                })
            .Seal()
        .Seal().Build();
}

// TODO: move in context
using TLiteralStructIndexMap = std::unordered_map<std::string_view, ui32>;
using TLieralStructsCacheMap = std::unordered_map<const TExprNode*, TLiteralStructIndexMap>;
const TLiteralStructIndexMap& GetLiteralStructIndexes(const TExprNode& literalStruct, TLieralStructsCacheMap& membersMap) {
    const auto search = membersMap.emplace(&literalStruct, literalStruct.ChildrenSize());
    if (search.second) {
        auto i = 0U;
        literalStruct.ForEachChild([&](const TExprNode& node) { search.first->second.emplace(node.Head().Content(), i++); });
    }
    return search.first->second;
}

std::array<TExprNode::TPtr, 2U> ApplyNarrowMap(const TExprNode& lambda, TExprContext& ctx) {
    const auto width =lambda.Head().ChildrenSize();
    TNodeOnNodeOwnedMap replaces(width);
    TExprNode::TListType args;
    args.reserve(width);
    for (auto i = 0U; i < width; ++i) {
        args.emplace_back(ctx.NewArgument(lambda.Head().Child(i)->Pos(), TString("fields") += ToString(i)));
        replaces.emplace(lambda.Head().Child(i), args.back());
    }
    return {{ ctx.NewArguments(lambda.Head().Pos(), std::move(args)),  ctx.ReplaceNodes(lambda.TailPtr(), replaces) }};
}

bool IsSimpleExpand(const TExprNode& out, const TExprNode& arg) {
    if (out.IsCallable({"Just", "Member", "Nth"}))
        return IsSimpleExpand(out.Head(), arg);
    return &out == &arg;
}

TExprNode::TPtr OptimizeExpandMap(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (const auto& input = node->Head(); input.IsCallable({"Map", "OrderedMap"})) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Fuse " << node->Content() << " with " << input.Content();
        auto lambda = ctx.Builder(node->Pos())
            .Lambda()
                .Param("item")
                .Apply(node->Tail())
                    .With(0)
                        .Apply(input.Tail())
                            .With(0, "item")
                        .Seal()
                    .Done()
                .Seal()
            .Seal().Build();
        return ctx.ChangeChildren(*node, {input.HeadPtr(), std::move(lambda)});
    }

    if (const auto& input = node->Head(); input.IsCallable({"Filter", "OrderedFilter"})) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << input.Content();
        auto outs = GetLambdaBody(node->Tail());

        TExprNode::TListType args, body;
        args.reserve(outs.size() + 2U);
        body.reserve(outs.size() + 2U);

        const auto& arg = node->Tail().Head().Head();
        args.emplace_back(ctx.NewArgument(arg.Pos(), "outer"));
        body.emplace_back(ctx.NewArgument(arg.Pos(), "inner"));

        TNodeOnNodeOwnedMap replaces(outs.size() + 1U);
        replaces.emplace(&arg, args.front());

        auto x = 0U;
        for (auto& out : outs) {
            if (IsSimpleExpand(*out, arg)) {
                args.emplace_back(ctx.NewArgument(out->Pos(), ToString(x++)));
                replaces.emplace(&*out, args.back());
                body.emplace_back(ctx.ReplaceNode(std::move(out), arg, body.front()));
                out = args.back();
            }
        }

        args.emplace_back(ctx.NewArgument(node->Pos(), "_"));
        body.emplace_back(ctx.ReplaceNode(input.Child(1)->TailPtr(), input.Child(1)->Head().Head(), body.front()));
        outs = ctx.ReplaceNodes(std::move(outs), replaces);
        auto row = body.front();
        if (std::none_of(outs.cbegin(), outs.cend(), std::bind(&IsDepended, std::bind(&TExprNode::TPtr::operator*, std::placeholders::_1), std::cref(*args.front())))) {
            args.erase(args.cbegin());
            body.erase(body.cbegin());
        }
        const auto width = body.size();
        auto expand = ctx.NewLambda(node->Tail().Pos(), ctx.NewArguments(node->Tail().Head().Pos(), {std::move(row)}), std::move(body));
        auto filter = ctx.Builder(input.Pos())
            .Callable("WideFilter")
                .Callable(0, node->Content())
                    .Add(0, input.HeadPtr())
                    .Add(1, std::move(expand))
                .Seal()
                .Lambda(1)
                    .Params("items", width)
                    .Arg("items", width - 1U)
                .Seal()
            .Seal().Build();

        if (3U == input.ChildrenSize()) {
            auto children = filter->ChildrenList();
            children.emplace_back(input.TailPtr());
            filter = ctx.ChangeChildren(*filter, std::move(children));
        }

        auto wide = ctx.NewLambda(node->Tail().Pos(), ctx.NewArguments(node->Tail().Head().Pos(), std::move(args)), std::move(outs));
        return ctx.NewCallable(node->Pos(), "WideMap", {std::move(filter), std::move(wide)});
    }

    if (const auto& input = node->Head(); input.IsCallable("NarrowMap")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Fuse " << node->Content() << " with " << input.Content();

        if (input.Tail().Tail().IsCallable("AsStruct")) {
            auto apply = ApplyNarrowMap(input.Tail(), ctx);
            TLieralStructsCacheMap membersMap; // TODO: move to context.
            const auto& members = GetLiteralStructIndexes(*apply.back(), membersMap);
            const auto& oldLambda = node->Tail();
            auto body = GetLambdaBody(oldLambda);
            std::for_each(body.begin(), body.end(), [&](TExprNode::TPtr& item) {
                item = item->IsCallable("Member") && &item->Head() == &oldLambda.Head().Head() ?
                    apply.back()->Child(members.find(item->Tail().Content())->second)->TailPtr():
                    ctx.ReplaceNode(std::move(item), oldLambda.Head().Head(), apply.back());
            });

            auto newLambda = ctx.NewLambda(oldLambda.Pos(), std::move(apply.front()), std::move(body));

            return ctx.Builder(node->Pos())
                .Callable("WideMap")
                    .Add(0, input.HeadPtr())
                    .Add(1, std::move(newLambda))
                .Seal().Build();
        } else {
            return ctx.Builder(node->Pos())
                .Callable("WideMap")
                    .Add(0, input.HeadPtr())
                    .Lambda(1)
                        .Params("items", input.Tail().Head().ChildrenSize())
                        .Apply(node->Tail())
                            .With(0)
                                .Apply(input.Tail())
                                    .With("items")
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                .Seal().Build();
        }
    }

    if (const auto& input = node->Head(); input.IsCallable("MapJoinCore") && !input.Head().IsArgument()) {
        if (const auto inItemType = GetSeqItemType(input.Head().GetTypeAnn()), outItemType = GetSeqItemType(input.GetTypeAnn());
            ETypeAnnotationKind::Struct == inItemType->GetKind() && ETypeAnnotationKind::Struct == outItemType->GetKind()) {
            YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << input.Content();

            auto expand = ctx.Builder(node->Pos())
                .Callable(node->Content())
                    .Add(0, input.HeadPtr())
                    .Lambda(1)
                        .Param("item")
                        .Do([&inItemType](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            ui32 i = 0U;
                            for (const auto& item : inItemType->Cast<TStructExprType>()->GetItems()) {
                                parent.Callable(i++, "Member")
                                    .Arg(0, "item")
                                    .Atom(1, item->GetName())
                                .Seal();
                            }
                            return parent;
                        })
                    .Seal()
                .Seal().Build();

            const auto structType = outItemType->Cast<TStructExprType>();
            return ctx.Builder(node->Pos())
                .Callable("WideMap")
                    .Add(0, MakeWideMapJoinCore<TStructExprType>(input, std::move(expand), ctx))
                    .Lambda(1)
                        .Params("fields", structType->GetSize())
                        .Apply(node->Tail())
                            .With(0)
                                .Callable("AsStruct")
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                        ui32 i = 0U;
                                        for (const auto& item : structType->GetItems()) {
                                            parent.List(i)
                                                .Atom(0, item->GetName())
                                                .Arg(1, "fields", i)
                                            .Seal();
                                            ++i;
                                        }
                                        return parent;
                                    })
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                .Seal().Build();
        }
    }

    if (const auto& input = node->Head(); input.IsCallable("CommonJoinCore") &&  !input.Head().IsArgument()) {
        if (const auto inItemType = GetSeqItemType(input.Head().GetTypeAnn()); ETypeAnnotationKind::Struct == inItemType->GetKind()) {
            YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << input.Content();

            auto expand = ctx.Builder(node->Pos())
                .Callable(node->Content())
                    .Add(0, input.HeadPtr())
                    .Lambda(1)
                        .Param("item")
                        .Do([&inItemType](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            ui32 i = 0U;
                            for (const auto& item : inItemType->Cast<TStructExprType>()->GetItems()) {
                                parent.Callable(i++, "Member")
                                    .Arg(0, "item")
                                    .Atom(1, item->GetName())
                                .Seal();
                            }
                            return parent;
                        })
                    .Seal()
                .Seal().Build();

            auto wide = MakeWideCommonJoinCore<TStructExprType>(input, std::move(expand), ctx);
            return ctx.Builder(node->Pos())
                .Callable("WideMap")
                    .Add(0, std::move(wide.first))
                    .Lambda(1)
                        .Params("fields", wide.second.size())
                        .Apply(node->Tail())
                            .With(0)
                                .Callable("AsStruct")
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                        ui32 i = 0U;
                                        for (auto& item : wide.second) {
                                            parent.List(i)
                                                .Add(0, std::move(item))
                                                .Arg(1, "fields", i)
                                            .Seal();
                                            ++i;
                                        }
                                        return parent;
                                    })
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                .Seal().Build();
        }
    }

    if (const auto& input = node->Head(); input.IsCallable("CombineCore") && !input.Head().IsArgument() &&
        (input.Child(2U)->Tail().IsCallable("AsStruct") || input.Child(3U)->Tail().IsCallable("AsStruct")) &&
        input.Child(4U)->Tail().IsCallable("Just") && ETypeAnnotationKind::Struct == input.Child(4U)->Tail().Head().GetTypeAnn()->GetKind()) {
        if (const auto inItemType = GetSeqItemType(input.Head().GetTypeAnn()); ETypeAnnotationKind::Struct == inItemType->GetKind()) {
            if (const auto inStructType = inItemType->Cast<TStructExprType>(); inStructType->GetSize() > 0U) {
                YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << input.Content();

                const auto& output = input.Child(4U)->Tail().Head();
                const auto structType = output.GetTypeAnn()->Cast<TStructExprType>();

                const auto outputWidth = structType->GetSize();
                const auto inputWidth = inStructType->GetSize();

                TExprNode::TListType inputFilelds, stateFields, outputFields, init, update, finish;
                inputFilelds.reserve(inputWidth);
                for (const auto& item : inStructType->GetItems()) {
                    inputFilelds.emplace_back(ctx.NewAtom(input.Pos(), item->GetName()));
                }

                const auto stateWidth = CollectStateNodes(*input.Child(2U), *input.Child(3U), stateFields, init, update, ctx);

                outputFields.reserve(outputWidth);
                finish.reserve(outputWidth);
                if (output.IsCallable("AsStruct")) {
                    input.Child(4U)->Tail().Head().ForEachChild([&](const TExprNode& child) {
                        outputFields.emplace_back(child.HeadPtr());
                        finish.emplace_back(child.TailPtr());
                    });
                } else {
                    for (const auto& item : structType->GetItems()) {
                        outputFields.emplace_back(ctx.NewAtom(output.Pos(), item->GetName()));
                        finish.emplace_back(ctx.NewCallable(output.Pos(), "Member", {input.Child(4U)->Tail().HeadPtr(), outputFields.back()}));
                    }
                }

                auto limit = input.ChildrenSize() > TCoCombineCore::idx_MemLimit ? input.TailPtr() : ctx.NewAtom(input.Pos(), "");
                return ctx.Builder(node->Pos())
                    .Callable("WideMap")
                        .Callable(0, "WideCombiner")
                            .Callable(0, node->Content())
                                .Add(0, input.HeadPtr())
                                .Lambda(1)
                                    .Param("item")
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                        ui32 i = 0U;
                                        for (const auto& member : inputFilelds) {
                                            parent.Callable(i++, "Member")
                                                .Arg(0, "item")
                                                .Add(1, member)
                                            .Seal();
                                        }
                                        return parent;
                                    })
                                .Seal()
                            .Seal()
                            .Add(1, std::move(limit))
                            .Lambda(2)
                                .Params("items", inputWidth)
                                .Apply(*input.Child(1U))
                                    .With(0)
                                        .Callable("AsStruct")
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                ui32 i = 0U;
                                                for (const auto& member : inputFilelds) {
                                                    parent.List(i)
                                                        .Add(0, member)
                                                        .Arg(1, "items", i)
                                                    .Seal();
                                                    ++i;
                                                }
                                                return parent;
                                            })
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal()
                            .Lambda(3)
                                .Param("key")
                                .Params("items", inputWidth)
                                .ApplyPartial(input.Child(2U)->HeadPtr(), std::move(init))
                                    .With(0, "key")
                                    .With(1)
                                        .Callable("AsStruct")
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                ui32 i = 0U;
                                                for (const auto& member : inputFilelds) {
                                                    parent.List(i)
                                                        .Add(0, member)
                                                        .Arg(1, "items", i)
                                                    .Seal();
                                                    ++i;
                                                }
                                                return parent;
                                            })
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal()
                            .Lambda(4)
                                .Param("key")
                                .Params("items", inputWidth)
                                .Params("state", stateWidth)
                                .ApplyPartial(input.Child(3U)->HeadPtr(), std::move(update))
                                    .With(0, "key")
                                    .With(1)
                                        .Callable("AsStruct")
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                ui32 i = 0U;
                                                for (const auto& member : inputFilelds) {
                                                    parent.List(i)
                                                        .Add(0, member)
                                                        .Arg(1, "items", i)
                                                    .Seal();
                                                    ++i;
                                                }
                                                return parent;
                                            })
                                        .Seal()
                                    .Done()
                                    .With(2)
                                        .Callable("AsStruct")
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                for (ui32 i = 0U; i < stateWidth; ++i) {
                                                    parent.List(i)
                                                        .Add(0, stateFields[i])
                                                        .Arg(1, "state", i)
                                                    .Seal();
                                                }
                                                return parent;
                                            })
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal()
                            .Lambda(5)
                                .Param("key")
                                .Params("state", stateWidth)
                                .ApplyPartial(input.Child(4U)->HeadPtr(), std::move(finish))
                                    .With(0, "key")
                                    .With(1)
                                        .Callable("AsStruct")
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                for (ui32 i = 0U; i < stateWidth; ++i) {
                                                    parent.List(i)
                                                        .Add(0, std::move(stateFields[i]))
                                                        .Arg(1, "state", i)
                                                    .Seal();
                                                }
                                                return parent;
                                            })
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal()
                        .Seal()
                        .Lambda(1)
                            .Params("items", outputWidth)
                            .Apply(node->Tail())
                                .With(0)
                                    .Callable("AsStruct")
                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                            for (ui32 i = 0U; i < outputWidth; ++i) {
                                                parent.List(i)
                                                    .Add(0, std::move(outputFields[i]))
                                                    .Arg(1, "items", i)
                                                .Seal();
                                            }
                                            return parent;
                                        })
                                    .Seal()
                                .Done()
                            .Seal()
                        .Seal()
                    .Seal().Build();
            }
        }
    }

    if (const auto& input = node->Head(); input.IsCallable("Condense1") && !input.Head().IsArgument() &&
        (input.Child(1U)->Tail().IsCallable("AsStruct") || input.Tail().Tail().IsCallable("AsStruct"))) {
        if (const auto inItemType = GetSeqItemType(input.Head().GetTypeAnn()); ETypeAnnotationKind::Struct == inItemType->GetKind()) {
            if (const auto inStructType = inItemType->Cast<TStructExprType>(); inStructType->GetSize() > 0U) {
                YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << input.Content();

                const auto inputWidth = inStructType->GetSize();
                TExprNode::TListType inputFilelds, stateFields, init, update;
                inputFilelds.reserve(inputWidth);
                for (const auto& item : inStructType->GetItems()) {
                    inputFilelds.emplace_back(ctx.NewAtom(input.Pos(), item->GetName()));
                }

                const auto stateWidth = CollectStateNodes(*input.Child(1U), input.Tail(), stateFields, init, update, ctx);

                return ctx.Builder(node->Pos())
                    .Callable("WideMap")
                        .Callable(0, "WideCondense1")
                            .Callable(0, node->Content())
                                .Add(0, input.HeadPtr())
                                .Lambda(1)
                                    .Param("item")
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                        ui32 i = 0U;
                                        for (const auto& member : inputFilelds) {
                                            parent.Callable(i++, "Member")
                                                .Arg(0, "item")
                                                .Add(1, member)
                                            .Seal();
                                        }
                                        return parent;
                                    })
                                .Seal()
                            .Seal()
                            .Lambda(1)
                                .Params("items", inputWidth)
                                .ApplyPartial(input.Child(1U)->HeadPtr(), std::move(init))
                                    .With(0)
                                        .Callable("AsStruct")
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                ui32 i = 0U;
                                                for (const auto& member : inputFilelds) {
                                                    parent.List(i)
                                                        .Add(0, member)
                                                        .Arg(1, "items", i)
                                                    .Seal();
                                                    ++i;
                                                }
                                                return parent;
                                            })
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal()
                            .Lambda(2)
                                .Params("items", inputWidth)
                                .Params("state", stateWidth)
                                .Apply(*input.Child(2U))
                                    .With(0)
                                        .Callable("AsStruct")
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                ui32 i = 0U;
                                                for (const auto& member : inputFilelds) {
                                                    parent.List(i)
                                                        .Add(0, member)
                                                        .Arg(1, "items", i)
                                                    .Seal();
                                                    ++i;
                                                }
                                                return parent;
                                            })
                                        .Seal()
                                    .Done()
                                    .With(1)
                                        .Callable("AsStruct")
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                for (ui32 i = 0U; i < stateWidth; ++i) {
                                                    parent.List(i)
                                                        .Add(0, stateFields[i])
                                                        .Arg(1, "state", i)
                                                    .Seal();
                                                }
                                                return parent;
                                            })
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal()
                            .Lambda(3)
                                .Params("items", inputWidth)
                                .Params("state", stateWidth)
                                .ApplyPartial(input.Tail().HeadPtr(), std::move(update))
                                    .With(0)
                                        .Callable("AsStruct")
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                ui32 i = 0U;
                                                for (const auto& member : inputFilelds) {
                                                    parent.List(i)
                                                        .Add(0, member)
                                                        .Arg(1, "items", i)
                                                    .Seal();
                                                    ++i;
                                                }
                                                return parent;
                                            })
                                        .Seal()
                                    .Done()
                                    .With(1)
                                        .Callable("AsStruct")
                                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                                for (ui32 i = 0U; i < stateWidth; ++i) {
                                                    parent.List(i)
                                                        .Add(0, stateFields[i])
                                                        .Arg(1, "state", i)
                                                    .Seal();
                                                }
                                                return parent;
                                            })
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal()
                        .Seal()
                        .Lambda(1)
                            .Params("items", stateWidth)
                            .Apply(node->Tail())
                                .With(0)
                                    .Callable("AsStruct")
                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                            for (ui32 i = 0U; i < stateWidth; ++i) {
                                                parent.List(i)
                                                    .Add(0, std::move(stateFields[i]))
                                                    .Arg(1, "items", i)
                                                .Seal();
                                            }
                                            return parent;
                                        })
                                    .Seal()
                                .Done()
                            .Seal()
                        .Seal()
                    .Seal().Build();
            }
        }
    }

    if (const auto& input = node->Head(); input.IsCallable({"Extend", "OrderedExtend"})) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << input.Content();
        bool isWideBlockFlow = AllOf(node->GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetItems(),
            [](const auto& itemType) { return itemType->IsBlockOrScalar(); });
        TString newName = ToString(input.Content());
        if (isWideBlockFlow) {
            newName = "Block" + newName;
        }
        TExprNodeList newChildren;
        newChildren.reserve(input.ChildrenSize());
        for (const auto& child : input.ChildrenList()) {
            newChildren.emplace_back(ctx.ChangeChildren(*node, { child, ctx.DeepCopyLambda(node->Tail())}));
        }
        return ctx.NewCallable(input.Pos(), newName, std::move(newChildren));
    }

/* TODO
    if (const auto& input = node->Head(); input.IsCallable("WithContext")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << input.Content();
        return ctx.ChangeChild(input, 0, ctx.ChangeChild(*node, 0, input.HeadPtr()));
    }
*/
    return node;
}

TExprNode::TPtr JustIf(bool optional, TExprNode::TPtr&& node, TExprContext& ctx) {
    return ctx.WrapByCallableIf(optional, "Just", std::move(node));
}

template<bool TupleOrStruct>
TExprNode::TPtr AutoMapGetElementhOfOptionalArray(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (node->Head().IsCallable("Nothing")) {
        YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over " << node->Head().Content();
        return ctx.ChangeChild(node->Head(), 0U, ExpandType(node->Pos(), *node->GetTypeAnn(), ctx));
    }

    if (const auto headType = node->Head().GetTypeAnn(); ETypeAnnotationKind::Optional == headType->GetKind()) {
        const auto arrayType = headType->Cast<TOptionalExprType>()->GetItemType();
        const auto itemType = TupleOrStruct ?
            arrayType->Cast<TTupleExprType>()->GetItems()[FromString<ui32>(node->Tail().Content())]:
            arrayType->Cast<TStructExprType>()->GetItems()[*arrayType->Cast<TStructExprType>()->FindItem(node->Tail().Content())]->GetItemType();

        if (node->Head().IsCallable("Just")) {
            YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over " << node->Head().Content();
            auto ret = ctx.ChangeChild(*node, 0U, node->Head().HeadPtr());
            return JustIf(!itemType->IsOptionalOrNull(), std::move(ret), ctx);
        }
    }

    return node;
}

TExprNode::TPtr OptimizeNth(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (node->Head().Type() == TExprNode::List) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Drop " << node->Content() << " over tuple literal";
        const auto index = FromString<ui32>(node->Tail().Content());
        return node->Head().ChildPtr(index);
    }

    return AutoMapGetElementhOfOptionalArray<true>(node, ctx);
}

TExprNode::TPtr OptimizeMember(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (node->Head().IsCallable("AsStruct")) {
        for (ui32 index = 0U; index < node->Head().ChildrenSize(); ++index) {
            if (const auto tuple = node->Head().Child(index); tuple->Head().Content() == node->Tail().Content()) {
                YQL_CLOG(DEBUG, CorePeepHole) << "Drop "<< node->Content() << " over " << node->Head().Content();
                return tuple->TailPtr();
            }
        }
    }

    return AutoMapGetElementhOfOptionalArray<false>(node, ctx);
}

TExprNode::TPtr OptimizeCondense1(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (node->Head().IsCallable("NarrowMap") &&
        ETypeAnnotationKind::Struct == node->Tail().Tail().GetTypeAnn()->GetKind()) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << node->Head().Content();

        const auto inputWidth = node->Head().Tail().Head().ChildrenSize();
        TExprNode::TListType fields, init, update;
        const auto outputWidth = CollectStateNodes(*node->Child(1U), node->Tail(), fields, init, update, ctx);

        return ctx.Builder(node->Pos())
            .Callable("NarrowMap")
                .Callable(0, "WideCondense1")
                    .Add(0, node->Head().HeadPtr())
                    .Lambda(1)
                        .Params("items", inputWidth)
                        .ApplyPartial(node->Child(1U)->HeadPtr(), std::move(init))
                            .With(0)
                                .Apply(node->Head().Tail())
                                    .With("items")
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                    .Lambda(2)
                        .Params("items", inputWidth)
                        .Params("state", outputWidth)
                        .Apply(*node->Child(2U))
                            .With(0)
                                .Apply(node->Head().Tail())
                                    .With("items")
                                .Seal()
                            .Done()
                            .With(1)
                                .Callable("AsStruct")
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                        for (ui32 i = 0U; i < outputWidth; ++i) {
                                            parent.List(i)
                                                .Add(0, fields[i])
                                                .Arg(1, "state", i)
                                            .Seal();
                                        }
                                        return parent;
                                    })
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                    .Lambda(3)
                        .Params("items", inputWidth)
                        .Params("state", outputWidth)
                        .ApplyPartial(node->Tail().HeadPtr(), std::move(update))
                            .With(0)
                                .Apply(node->Head().Tail())
                                    .With("items")
                                .Seal()
                            .Done()
                            .With(1)
                                .Callable("AsStruct")
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                        for (ui32 i = 0U; i < outputWidth; ++i) {
                                            parent.List(i)
                                                .Add(0, fields[i])
                                                .Arg(1, "state", i)
                                            .Seal();
                                        }
                                        return parent;
                                    })
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                .Seal()
                .Lambda(1)
                    .Params("items", outputWidth)
                    .Callable("AsStruct")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (ui32 i = 0U; i < outputWidth; ++i) {
                                parent.List(i)
                                    .Add(0, std::move(fields[i]))
                                    .Arg(1, "items", i)
                                .Seal();
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal().Build();
    }

    return node;
}

TExprNode::TPtr OptimizeCombineCore(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (node->Head().IsCallable("NarrowMap") && node->Child(4U)->Tail().IsCallable("Just")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << node->Head().Content();

        const auto& output = node->Child(4U)->Tail().Head();
        const auto inputWidth = node->Head().Tail().Head().ChildrenSize();

        const auto structType = ETypeAnnotationKind::Struct == output.GetTypeAnn()->GetKind() ? output.GetTypeAnn()->Cast<TStructExprType>() : nullptr;
        const auto outputWidth = structType ? structType->GetSize() : 1U;

        TExprNode::TListType stateFields, outputFields, init, update, finish;
        outputFields.reserve(outputWidth);
        finish.reserve(outputWidth);

        const auto stateWidth = CollectStateNodes(*node->Child(2U), *node->Child(3U), stateFields, init, update, ctx);

        if (output.IsCallable("AsStruct")) {
            node->Child(4U)->Tail().Head().ForEachChild([&](const TExprNode& child) {
                outputFields.emplace_back(child.HeadPtr());
                finish.emplace_back(child.TailPtr());
            });
        } else if (structType) {
            for (const auto& item : structType->GetItems()) {
                outputFields.emplace_back(ctx.NewAtom(output.Pos(), item->GetName()));
                finish.emplace_back(ctx.NewCallable(output.Pos(), "Member", {node->Child(4U)->Tail().HeadPtr(), outputFields.back()}));
            }
        } else {
            finish.emplace_back(node->Child(4U)->Tail().HeadPtr());
        }

        auto limit = node->ChildrenSize() > TCoCombineCore::idx_MemLimit ? node->TailPtr() : ctx.NewAtom(node->Pos(), "");
        return ctx.Builder(node->Pos())
            .Callable("NarrowMap")
                .Callable(0, "WideCombiner")
                    .Add(0, node->Head().HeadPtr())
                    .Add(1, std::move(limit))
                    .Lambda(2)
                        .Params("items", inputWidth)
                        .Apply(*node->Child(1U))
                            .With(0)
                                .Apply(node->Head().Tail())
                                    .With("items")
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                    .Lambda(3)
                        .Param("key")
                        .Params("items", inputWidth)
                        .ApplyPartial(node->Child(2U)->HeadPtr(), std::move(init))
                            .With(0, "key")
                            .With(1)
                                .Apply(node->Head().Tail())
                                    .With("items")
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                    .Lambda(4)
                        .Param("key")
                        .Params("items", inputWidth)
                        .Params("state", stateWidth)
                        .ApplyPartial(node->Child(3U)->HeadPtr(), std::move(update))
                            .With(0, "key")
                            .With(1)
                                .Apply(node->Head().Tail())
                                    .With("items")
                                .Seal()
                            .Done()
                            .With(2)
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    if (stateFields.empty())
                                        parent.Arg("state", 0);
                                    else {
                                        auto str = parent.Callable("AsStruct");
                                        for (ui32 i = 0U; i < stateWidth; ++i) {
                                            str.List(i)
                                                .Add(0, stateFields[i])
                                                .Arg(1, "state", i)
                                            .Seal();
                                        }
                                        str.Seal();
                                    }
                                    return parent;
                                })
                            .Done()
                        .Seal()
                    .Seal()
                    .Lambda(5)
                        .Param("key")
                        .Params("state", stateWidth)
                        .ApplyPartial(node->Child(4U)->HeadPtr(), std::move(finish))
                            .With(0, "key")
                            .With(1)
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    if (stateFields.empty())
                                        parent.Arg("state", 0);
                                    else {
                                        auto str = parent.Callable("AsStruct");
                                        for (ui32 i = 0U; i < stateWidth; ++i) {
                                            str.List(i)
                                                .Add(0, std::move(stateFields[i]))
                                                .Arg(1, "state", i)
                                            .Seal();
                                        }
                                        str.Seal();
                                    }
                                    return parent;
                                })
                            .Done()
                        .Seal()
                    .Seal()
                .Seal()
                .Lambda(1)
                    .Params("items", outputWidth)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        if (outputFields.empty())
                            parent.Arg("items", 0);
                        else {
                            auto str = parent.Callable("AsStruct");
                            for (ui32 i = 0U; i < outputWidth; ++i) {
                                str.List(i)
                                    .Add(0, std::move(outputFields[i]))
                                    .Arg(1, "items", i)
                                .Seal();
                            }
                            str.Seal();
                        }
                        return parent;
                    })
                .Seal()
            .Seal().Build();
    }

    return node;
}

bool IsExpression(const TExprNode& root, const TExprNode& arg) {
    if (&root == &arg)
        return false;

    if (root.IsCallable({"Member", "Nth"}))
        return IsExpression(root.Head(), arg);

    return true;
}

template<bool Sort, bool HasCount>
TExprNode::TPtr OptimizeTopOrSort(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (ETypeAnnotationKind::Struct == GetSeqItemType(*node->Head().GetTypeAnn()).GetKind()) {
        std::set<ui32> indexes;
        if (node->Tail().Tail().IsList())
            for (auto i = 0U; i < node->Tail().Tail().ChildrenSize(); ++i)
                if (IsExpression(*node->Tail().Tail().Child(i), node->Tail().Head().Head()))
                    indexes.emplace(i);

        if (!indexes.empty() || (!node->Tail().Tail().IsList() && IsExpression(node->Tail().Tail(), node->Tail().Head().Head()))) {
            YQL_CLOG(DEBUG, Core) << "Make system columns for " << node->Content() << " keys.";

            auto argIn = ctx.NewArgument(node->Tail().Pos(), "row");
            auto argOut = ctx.NewArgument(node->Tail().Pos(), "row");
            auto bodyIn = argIn;
            auto bodyOut = argOut;

            auto selector = node->TailPtr();
            if (indexes.empty()) {
                auto column = ctx.NewAtom(selector->Pos(), "_yql_sys_order_by", TNodeFlags::Default);
                bodyIn = ctx.Builder(bodyIn->Pos())
                    .Callable("AddMember")
                        .Add(0, std::move(bodyIn))
                        .Add(1, column)
                        .Apply(2, node->Tail())
                            .With(0, argIn)
                        .Seal()
                    .Seal().Build();
                bodyOut = ctx.Builder(bodyOut->Pos())
                    .Callable("RemoveMember")
                        .Add(0, std::move(bodyOut))
                        .Add(1, column)
                    .Seal().Build();
                selector = ctx.Builder(selector->Pos())
                    .Lambda()
                        .Param("row")
                        .Callable("Member")
                            .Arg(0, "row")
                            .Add(1, std::move(column))
                        .Seal()
                    .Seal().Build();
            } else {
                auto items = selector->Tail().ChildrenList();
                for (const auto index : indexes) {
                    auto column = ctx.NewAtom(items[index]->Pos(), TString("_yql_sys_order_by_") += ToString(index), TNodeFlags::Default);
                    bodyIn = ctx.Builder(bodyIn->Pos())
                        .Callable("AddMember")
                            .Add(0, std::move(bodyIn))
                            .Add(1, column)
                            .ApplyPartial(2, node->Tail().HeadPtr(), std::move(items[index]))
                                .With(0, argIn)
                            .Seal()
                        .Seal().Build();
                    bodyOut = ctx.Builder(bodyOut->Pos())
                        .Callable("RemoveMember")
                            .Add(0, std::move(bodyOut))
                            .Add(1, column)
                        .Seal().Build();
                    items[index] = ctx.Builder(selector->Pos())
                        .Callable("Member")
                            .Add(0, selector->Head().HeadPtr())
                            .Add(1, std::move(column))
                        .Seal().Build();
                }
                selector = ctx.DeepCopyLambda(*selector, ctx.NewList(selector->Tail().Pos(), std::move(items)));
            }

            auto children = node->ChildrenList();
            children.back() = std::move(selector);
            children.front() = ctx.Builder(node->Pos())
                    .Callable("OrderedMap")
                        .Add(0, std::move(children.front()))
                        .Add(1, ctx.NewLambda(node->Tail().Pos(), ctx.NewArguments(node->Tail().Head().Pos(), {std::move(argIn)}), std::move(bodyIn)))
                    .Seal().Build();
            return ctx.Builder(node->Pos())
                    .Callable("OrderedMap")
                        .Add(0, ctx.ChangeChildren(*node, std::move(children)))
                        .Add(1, ctx.NewLambda(node->Tail().Pos(), ctx.NewArguments(node->Tail().Head().Pos(), {std::move(argOut)}), std::move(bodyOut)))
                    .Seal().Build();
        }
    }

    if (const auto& input = node->Head(); input.IsCallable("NarrowMap") && input.Tail().Tail().IsCallable("AsStruct")) {
        TNodeMap<size_t> indexes(input.Tail().Tail().ChildrenSize());
        input.Tail().Tail().ForEachChild([&](const TExprNode& field) {
            if (field.Tail().IsArgument()) {
                const auto& arguments = input.Tail().Head().Children();
                if (const auto find = std::find(arguments.cbegin(), arguments.cend(), field.TailPtr()); arguments.cend() != find)
                    indexes.emplace(&field.Head(), std::distance(arguments.cbegin(), find));
            }
        });

        std::unordered_set<size_t> unique;
        std::vector<size_t> sorted;
        if (node->Tail().Tail().IsCallable("Member") && &node->Tail().Tail().Head() == &node->Tail().Head().Head()) {
            if (const auto it = indexes.find(&node->Tail().Tail().Tail()); indexes.cend() == it) {
                return node;
            } else {
                sorted.emplace_back(it->second);
            }
        } else if (node->Tail().Tail().IsList()) {
            for (const auto& field : node->Tail().Tail().Children()) {
                if (field->IsCallable("Member") && &field->Head() == &node->Tail().Head().Head())
                    if (const auto it = indexes.find(&field->Tail()); indexes.cend() == it) {
                        return node;
                    } else {
                        sorted.emplace_back(it->second);
                    }
                else
                    return node;
            }
        }

        unique.insert(sorted.begin(), sorted.end());
        if (sorted.empty() || unique.size() != sorted.size())
            return node;

        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << input.Content();

        TExprNode::TListType directions(sorted.size());
        auto dirIndex = HasCount ? 2U : 1U;
        for (auto i = 0U; i < sorted.size(); ++i) {
            auto dir = node->Child(dirIndex)->IsList() ? node->Child(dirIndex)->ChildPtr(i) : node->ChildPtr(dirIndex);
            directions[i] = ctx.Builder(dir->Pos())
                .List()
                    .Atom(0, sorted[i])
                    .Add(1, std::move(dir))
                .Seal().Build();
        }

        if constexpr (HasCount) {
            return Build<TCoNarrowMap>(ctx, node->Pos())
                .template Input<std::conditional_t<Sort, TCoWideTopSort, TCoWideTop>>()
                    .Input(input.HeadPtr())
                    .Count(node->ChildPtr(1))
                    .template Keys<TCoSortKeys>()
                        .Add(std::move(directions))
                        .Build()
                    .Build()
                .Lambda(input.TailPtr())
                .Done().Ptr();
        } else {
            return Build<TCoNarrowMap>(ctx, node->Pos())
                .template Input<TCoWideSort>()
                    .Input(input.HeadPtr())
                    .template Keys<TCoSortKeys>()
                        .Add(std::move(directions))
                        .Build()
                    .Build()
                .Lambda(input.TailPtr())
                .Done().Ptr();
        }
    }

    return node;
}

TExprNode::TPtr OptimizeChopper(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (node->Head().IsCallable("NarrowMap") &&
        node->Tail().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow &&
        node->Tail().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->GetKind() == ETypeAnnotationKind::Struct &&
        node->Tail().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TStructExprType>()->GetSize() > 0U) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << node->Head().Content();

        const auto inputWidth = node->Head().Tail().Head().ChildrenSize();
        const auto structType = node->Tail().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TStructExprType>();
        const auto outputWidth = structType->GetSize();

        TExprNode::TListType fields;
        fields.reserve(outputWidth);
        for (const auto& item : structType->GetItems())
            fields.emplace_back(ctx.NewAtom(node->Tail().Pos(), item->GetName()));

        return ctx.Builder(node->Pos())
            .Callable("NarrowMap")
                .Callable(0, "WideChopper")
                    .Add(0, node->Head().HeadPtr())
                    .Lambda(1)
                        .Params("items", inputWidth)
                        .Apply(*node->Child(1U))
                            .With(0)
                                .Apply(node->Head().Tail())
                                    .With("items")
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                    .Lambda(2)
                        .Param("key")
                        .Params("items", inputWidth)
                        .Apply(*node->Child(2U))
                            .With(0, "key")
                            .With(1)
                                .Apply(node->Head().Tail())
                                    .With("items")
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                    .Lambda(3)
                        .Param("key")
                        .Param("flow")
                        .Callable("ExpandMap")
                            .Apply(0, node->Tail())
                                .With(0, "key")
                                .With(1)
                                    .Callable("NarrowMap")
                                        .Arg(0, "flow")
                                        .Lambda(1)
                                            .Params("items", inputWidth)
                                            .Apply(node->Head().Tail())
                                                .With("items")
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                .Done()
                            .Seal()
                            .Lambda(1)
                                .Param("item")
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    for (ui32 i = 0U; i < outputWidth; ++i) {
                                        parent.Callable(i, "Member")
                                            .Arg(0, "item")
                                            .Add(1, fields[i])
                                        .Seal();
                                    }
                                    return parent;
                                })
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Lambda(1)
                    .Params("items", outputWidth)
                    .Callable("AsStruct")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (ui32 i = 0U; i < outputWidth; ++i) {
                                parent.List(i)
                                    .Add(0, std::move(fields[i]))
                                    .Arg(1, "items", i)
                                .Seal();
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal().Build();
    }

    return node;
}

using TTupleExpandMap = std::vector<std::optional<ui32>>;
using TStructExpandMap = std::vector<std::optional<std::vector<std::string_view>>>;

using TListExpandMap = std::map<ui32, const TTypeAnnotationNode*>;

template<ui32 WidthLimit = 0U>
std::array<std::optional<ui32>, 2U> GetExpandMapsForLambda(const TExprNode& lambda, TTupleExpandMap& tupleExpndMap, TStructExpandMap& structExpndMap, TListExpandMap* listExpndMap = nullptr) {
    const auto original = lambda.ChildrenSize() - 1U;
    tupleExpndMap.resize(original);

    bool hasTuple = false, hasStruct = false;
    ui32 flatByTuple = 0U, flatByStruct = 0U;

    for (ui32 i = 0U; i < original; ++i) {
        switch (const auto child = lambda.Child(i + 1U); child->GetTypeAnn()->GetKind()) {
            case ETypeAnnotationKind::Dict:
            case ETypeAnnotationKind::List:
            case ETypeAnnotationKind::Pg:
                if (listExpndMap) {
                    listExpndMap->emplace(i, child->GetTypeAnn());
                }
                break;
            case ETypeAnnotationKind::Tuple:
                if (const auto size = child->GetTypeAnn()->Cast<TTupleExprType>()->GetSize();
                    !WidthLimit || child->IsList() || WidthLimit >= size) {
                    ++flatByStruct;
                    hasTuple = true;
                    flatByTuple += size;
                    tupleExpndMap[i].emplace(size);
                    continue;
                }
                break;
            case ETypeAnnotationKind::Struct:
                if (const auto structType = child->GetTypeAnn()->Cast<TStructExprType>();
                    !WidthLimit || child->IsCallable("AsStruct") || WidthLimit >= structType->GetSize()) {
                    ++flatByTuple;
                    hasStruct = true;
                    flatByStruct += structType->GetSize();
                    structExpndMap[i].emplace(structType->GetSize());
                    const auto& items = structType->GetItems();
                    std::transform(items.cbegin(), items.cend(), structExpndMap[i]->begin(), std::bind(&TItemExprType::GetName, std::placeholders::_1));
                    continue;
                }
                break;
            default:
                break;
        }

        ++flatByTuple;
        ++flatByStruct;
    }

    return {hasTuple ? std::optional<ui32>(flatByTuple) : std::nullopt, hasStruct ? std::optional<ui32>(flatByStruct) : std::nullopt};
}

TExprNode::TPtr MakeEmptyWideLambda(TPositionHandle pos, ui32 width, TExprContext& ctx) {
    TExprNode::TListType args(width);
    std::generate_n(args.begin(), width, [&](){ return ctx.NewArgument(pos, "arg"); });
    return ctx.NewLambda(pos, ctx.NewArguments(pos, TExprNode::TListType(args)), std::move(args));
}

TExprNode::TPtr NarrowToWide(const TExprNode& map, TExprContext& ctx) {
    const auto& root = map.Tail().Tail();
    const auto inStructType = root.GetTypeAnn()->Cast<TStructExprType>();
    if (root.IsCallable("AsStruct")) {
        TLieralStructsCacheMap membersMap; // TODO: move to context.
        const auto& members = GetLiteralStructIndexes(root, membersMap);
        return ctx.Builder(map.Pos())
            .Callable("WideMap")
                .Add(0, map.HeadPtr())
                .Lambda(1)
                    .Params("fields", map.Tail().Head().ChildrenSize())
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 i = 0U;
                        for (const auto& item : inStructType->GetItems()) {
                            parent.ApplyPartial(i++, map.Tail().HeadPtr(), root.Child(members.find(item->GetName())->second)->TailPtr())
                                .With("fields")
                            .Seal();
                        }
                        return parent;
                    })
                .Seal()
            .Seal().Build();
    } else {
        auto apply = ApplyNarrowMap(map.Tail(), ctx);
        TExprNode::TListType body;
        body.reserve(inStructType->GetSize());
        const auto pos = map.Tail().Pos();
        const auto& items = inStructType->GetItems();
        std::transform(items.cbegin(), items.cend(), std::back_inserter(body),
            [&](const TItemExprType* item) { return ctx.NewCallable(pos, "Member", {apply.back(), ctx.NewAtom(pos, item->GetName())}); });
        auto lambda = ctx.NewLambda(pos, std::move(apply.front()), std::move(body));

        return ctx.Builder(map.Pos())
            .Callable("WideMap")
                .Add(0, map.HeadPtr())
                .Add(1, std::move(lambda))
            .Seal().Build();
    }
}

void FlattenLambdaBody(TExprNode::TPtr& lambda, const TTupleExpandMap& expandMap, ui32 oldWidth, ui32 newWidth, TExprContext& ctx) {
    TExprNode::TListType flatten;
    flatten.reserve(lambda->Head().ChildrenSize() + newWidth - oldWidth);
    for (ui32 i = 0U; i < oldWidth; ++i) {
        const auto child = lambda->Child(i + 1U);
        if (const auto expand = expandMap.size() > i ? expandMap[i] : std::nullopt) {
            for (ui32 j = 0U; j < *expand; ++j)
                flatten.emplace_back(child->IsList() ? child->ChildPtr(j) : ctx.NewCallable(child->Pos(), "Nth",
                    {lambda->ChildPtr(i + 1U), ctx.NewAtom(child->Pos(), j)}
                ));
        } else {
            flatten.emplace_back(lambda->ChildPtr(i + 1U));
        }
    }

    lambda = ctx.DeepCopyLambda(*lambda, std::move(flatten));
}

void FlattenLambdaBody(TExprNode::TPtr& lambda, const TStructExpandMap& expandMap, ui32 oldWidth, ui32 newWidth, TLieralStructsCacheMap& membersMap, TExprContext& ctx) {
    TExprNode::TListType flatten;
    flatten.reserve(lambda->Head().ChildrenSize() + newWidth - oldWidth);
    for (ui32 i = 0U; i < oldWidth; ++i) {
        if (const auto expand = expandMap.size() > i ? expandMap[i] : std::nullopt) {
            if (const auto child = lambda->Child(i + 1U); child->IsCallable("AsStruct")) {
                const auto& members = GetLiteralStructIndexes(*child, membersMap);
                for (const auto& member : *expand)
                    flatten.emplace_back(child->Child(members.find(member)->second)->TailPtr());
            } else {
                for (const auto& member : *expand)
                    flatten.emplace_back(ctx.NewCallable(child->Pos(), "Member", {lambda->ChildPtr(i + 1U), ctx.NewAtom(child->Pos(), member)}));
            }
        } else {
            flatten.emplace_back(lambda->ChildPtr(i + 1U));
        }
    }

    lambda = ctx.DeepCopyLambda(*lambda, std::move(flatten));
}

void FlattenLambdaArgs(TExprNode::TPtr& lambda, const TTupleExpandMap& expandMap, ui32 oldWidth, ui32 newWidth, TExprContext& ctx, ui32 skip = 0U) {
    const auto inputWidth = lambda->Head().ChildrenSize() + newWidth - oldWidth;
    lambda = ctx.Builder(lambda->Pos())
        .Lambda()
            .Params("items", inputWidth)
            .Apply(*lambda)
                .Do([&](TExprNodeReplaceBuilder& parent) -> TExprNodeReplaceBuilder& {
                    for (ui32 i = 0U, k = 0U; i < lambda->Head().ChildrenSize(); ++i) {
                        if (const auto expand = i >= skip && expandMap.size() + skip > i ? expandMap[i - skip] : std::nullopt) {
                            parent.With(i).List().Do([&](TExprNodeBuilder& list) -> TExprNodeBuilder& {
                                for (ui32 j = 0U; j < *expand; ++j) {
                                    list.Arg(j, "items", k++);
                                }
                                return list;
                            }).Seal().Done();
                        } else {
                            parent.With(i, "items", k++);
                        }
                    }
                    return parent;
                })
            .Seal()
        .Seal().Build();
}

void FlattenLambdaArgs(TExprNode::TPtr& lambda, const TStructExpandMap& expandMap, ui32 oldWidth, ui32 newWidth, TExprContext& ctx, ui32 skip = 0U) {
    const auto inputWidth = lambda->Head().ChildrenSize() + newWidth - oldWidth;
    lambda = ctx.Builder(lambda->Pos())
        .Lambda()
            .Params("items", inputWidth)
            .Apply(*lambda)
                .Do([&](TExprNodeReplaceBuilder& parent) -> TExprNodeReplaceBuilder& {
                    for (ui32 i = 0U, k = 0U; i < lambda->Head().ChildrenSize(); ++i) {
                        if (const auto expand = i >= skip && expandMap.size() + skip > i ? expandMap[i - skip] : std::nullopt) {
                            parent.With(i).Callable("AsStruct").Do([&](TExprNodeBuilder& list) -> TExprNodeBuilder& {
                                ui32 j = 0U;
                                for (const auto& field : *expand) {
                                    list.List(j++)
                                        .Atom(0, field)
                                        .Arg(1, "items", k++)
                                    .Seal();
                                }
                                return list;
                            }).Seal().Done();
                        } else {
                            parent.With(i, "items", k++);
                        }
                    }
                    return parent;
                })
            .Seal()
        .Seal().Build();
}

using TDedupMap = std::vector<std::pair<ui32, ui32>>;
TDedupMap DedupState(const TExprNode& init, const TExprNode& update) {
    YQL_ENSURE(init.ChildrenSize() == update.ChildrenSize(), "Must be same size.");
    const auto skip = init.Head().ChildrenSize();
    YQL_ENSURE(update.Head().ChildrenSize() + 1U == init.ChildrenSize() + skip, "Wrong update args count.");

    TNodeMap<ui32> map;
    map.reserve(update.Head().ChildrenSize());

    std::vector<ui32> pos;
    pos.reserve(init.ChildrenSize() - 1U);

    for (ui32 i = 1U; i < init.ChildrenSize(); ++i)
        pos.emplace_back(map.emplace(init.Child(i), i - 1U).first->second);

    map.clear();
    ui32 i = 0U;
    update.Head().ForEachChild([&](const TExprNode& arg) { map.emplace(&arg, i < skip ? i++ : skip + pos[i++ - skip]); });

    TDedupMap dedups;
    for (ui32 j = 1U; j <= pos.size(); ++j) {
        if (const auto i = pos[j - 1U] + 1U; i < j) {
            if (const auto one = update.Child(i), two = update.Child(j); one != two && CompareExprTreeParts(*one, *two, map)) {
                dedups.emplace_back(i - 1U, j - 1U);
            }
        }
    }

    return dedups;
}

using TDedupRealMap = std::map<ui32, ui32>;
TDedupRealMap DedupAggregationKeysFromState(const TExprNode& extract, const TExprNode& init, const TExprNode& update) {
    YQL_ENSURE(init.ChildrenSize() == update.ChildrenSize(), "Must be same size.");
    const ui32 keyWidth = extract.ChildrenSize() - 1;
    const ui32 itemsWidth = extract.Head().ChildrenSize();

    TNodeMap<ui32> map;
    map.reserve(keyWidth);

    {
        ui32 i = 0U;
        extract.Head().ForEachChild([&](const TExprNode& arg) { map.emplace(&arg, i++); });
        for (i = keyWidth; i < init.Head().ChildrenSize(); ++i) {
            map.emplace(init.Head().Child(i), i - keyWidth);
        }
    }

    TDedupRealMap dedups;
    for (ui32 stateIdx = 0; stateIdx + 1 < init.ChildrenSize(); ++stateIdx) {
        for (ui32 keyIdx = 0; keyIdx + 1 < extract.ChildrenSize(); ++keyIdx) {
            if (!CompareExprTreeParts(*extract.Child(keyIdx + 1), *init.Child(stateIdx + 1), map)) {
                continue;
            }
            if (update.Head().Child(keyWidth + itemsWidth + stateIdx) == update.Child(stateIdx + 1)) {
                dedups.emplace(stateIdx, keyIdx);
                break;
            }
        }
    }
    return dedups;
}

TExprNode::TPtr DedupLambdaBody(const TExprNode& lambda, const TDedupMap& dedups, TExprContext& ctx) {
    auto state = GetLambdaBody(lambda);
    std::for_each(dedups.cbegin(), dedups.cend(), [&](const std::pair<ui32, ui32>& pair) { state[pair.second] = state[pair.first]; });
    return ctx.DeepCopyLambda(lambda, std::move(state));
}

template<size_t Consumers>
std::vector<ui32> UnusedState(const TExprNode& init, const TExprNode& update, const std::array<const TExprNode*, Consumers>& consumers) {
    YQL_ENSURE(init.ChildrenSize() == update.ChildrenSize(), "Must be same size.");

    const auto size = init.ChildrenSize() - 1U;
    const auto skipU = update.Head().ChildrenSize() - size;

    std::vector<ui32> unused;
    unused.reserve(size);
    for (ui32 j = 0U; j < size; ++j) {
        if (std::all_of(consumers.cbegin(), consumers.cend(), [size, j](const TExprNode* lambda) { return lambda->Head().Child(j + lambda->Head().ChildrenSize() - size)->Unique(); }) &&
            (update.Head().Child(j + skipU)->Unique() || 2U == update.Head().Child(j + skipU)->UseCount() && update.Head().Child(j + skipU) == update.Child(j + 1U))) {
            unused.emplace_back(j);
        }
    }
    return unused;
}

template<size_t Consumers>
std::vector<ui32> UnusedArgs(const std::array<TExprNode::TChildrenType, Consumers>& consumers) {
    const auto size = (*std::min_element(consumers.cbegin(), consumers.cend(), [](const TExprNode::TChildrenType& l, const TExprNode::TChildrenType& r) { return l.size() < r.size(); })).size();
    std::vector<ui32> unused;
    unused.reserve(size);
    for (auto j = 0U; j < size; ++j) {
        if (std::all_of(consumers.cbegin(), consumers.cend(), [j](const TExprNode::TChildrenType& args) { return args[j]->Unique(); })) {
            unused.emplace_back(j);
        }
    }
    return unused;
}

template<size_t Consumers>
std::vector<ui32> UnusedArgs(const std::array<const TExprNode*, Consumers>& consumers) {
    std::array<TExprNode::TChildrenType, Consumers> consumersArgs;
    std::transform(consumers.cbegin(), consumers.cend(), consumersArgs.begin(), [](const TExprNode* lambda) { return lambda->Head().Children(); });
    return UnusedArgs(consumersArgs);
}

TExprNode::TListType&& DropUnused(TExprNode::TListType&& list, const std::vector<ui32>& unused, ui32 skip = 0U) {
    std::for_each(unused.cbegin(), unused.cend(), [&](const ui32 index) { list[index + skip] = TExprNode::TPtr(); });
    list.erase(std::remove_if(list.begin(), list.end(), std::logical_not<TExprNode::TPtr>()), list.cend());
    return std::move(list);
}

std::set<ui32> FillAllIndexes(const TTypeAnnotationNode& rowType) {
    std::set<ui32> set;
    for (ui32 i = 0U; i < rowType.Cast<TMultiExprType>()->GetSize(); ++i)
        set.emplace(i);
    return set;
}

template<bool EvenOnly>
void RemoveUsedIndexes(const TExprNode& list, std::set<ui32>& unused) {
    for (auto i = 0U; i < list.ChildrenSize() >> (EvenOnly ? 1U : 0U); ++i)
        unused.erase(FromString<ui32>(list.Child(EvenOnly ? i << 1U : i)->Content()));
}

TExprNode::TPtr DropUnusedArgs(const TExprNode& lambda, const std::vector<ui32>& unused, TExprContext& ctx, ui32 skip = 0U) {
    const auto& copy = ctx.DeepCopyLambda(lambda);
    return ctx.ChangeChild(*copy, 0U, ctx.NewArguments(copy->Head().Pos(), DropUnused(copy->Head().ChildrenList(), unused, skip)));
}

void DropUnusedRenames(TExprNode::TPtr& renames, const std::vector<ui32>& unused, TExprContext& ctx) {
    TExprNode::TListType children;
    children.reserve(renames->ChildrenSize());
    for (auto i = 0U; i < renames->ChildrenSize() >> 1U; ++i) {
        const auto idx = i << 1U;
        const auto outIndex = renames->Child(1U + idx);
        const auto oldOutPos = FromString<ui32>(outIndex->Content());
        if (const auto iter = std::lower_bound(unused.cbegin(), unused.cend(), oldOutPos); unused.cend() == iter || *iter != oldOutPos) {
            children.emplace_back(renames->ChildPtr(idx));
            const auto newOutPos = oldOutPos - ui32(std::distance(unused.cbegin(), iter));
            children.emplace_back(ctx.NewAtom(outIndex->Pos(), newOutPos));
        }
    }

    renames = ctx.ChangeChildren(*renames, std::move(children));
}

template<bool EvenOnly>
void UpdateInputIndexes(TExprNode::TPtr& indexes, const std::vector<ui32>& unused, TExprContext& ctx) {
    TExprNode::TListType children;
    children.reserve(indexes->ChildrenSize());
    for (auto i = 0U; i < indexes->ChildrenSize() >> (EvenOnly ? 1U : 0U); ++i) {
        const auto idx = i << (EvenOnly ? 1U : 0U);
        const auto outIndex = indexes->Child(idx);
        const auto oldValue = FromString<ui32>(outIndex->Content());
        const auto newValue = oldValue - ui32(std::distance(unused.cbegin(), std::lower_bound(unused.cbegin(), unused.cend(), oldValue)));
        if (oldValue == newValue) {
            children.emplace_back(indexes->ChildPtr(idx));
        } else {
            children.emplace_back(ctx.NewAtom(indexes->Child(idx)->Pos(), newValue));
        }
        if constexpr (EvenOnly) {
            children.emplace_back(indexes->ChildPtr(1U + idx));
        }
    }

    indexes = ctx.ChangeChildren(*indexes, std::move(children));
}

TExprNode::TPtr DropUnusedStateFromUpdate(const TExprNode& lambda, const std::vector<ui32>& unused, TExprContext& ctx) {
    const auto& copy = ctx.DeepCopyLambda(lambda, DropUnused(GetLambdaBody(lambda), unused));
    return ctx.ChangeChild(*copy, 0U, ctx.NewArguments(copy->Head().Pos(), DropUnused(copy->Head().ChildrenList(), unused, lambda.Head().ChildrenSize() - lambda.ChildrenSize() + 1U)));
}

TExprNode::TPtr MakeWideMapForDropUnused(TExprNode::TPtr&& input, const std::vector<ui32>& unused, TExprContext& ctx) {
    const auto width = input->GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetSize();
    return ctx.Builder(input->Pos())
        .Callable("WideMap")
            .Add(0, std::move(input))
            .Lambda(1)
                .Params("items", width)
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (auto i = 0U, j = 0U; i < width; ++i) {
                        if (unused.cend() == std::find(unused.cbegin(), unused.cend(), i))
                            parent.Arg(j++, "items", i);
                    }
                    return parent;
                })
            .Seal()
        .Seal().Build();
}

TExprNode::TPtr UnpickleInput(TExprNode::TPtr originalLambda, TListExpandMap& listExpandMap, TExprContext& ctx) {
        auto size = originalLambda->Head().ChildrenSize();
        return ctx.Builder(originalLambda->Pos())
            .Lambda()
                .Params("out", size)
                .Apply(originalLambda)
                    .Do([&](TExprNodeReplaceBuilder& inner) -> TExprNodeReplaceBuilder& {
                        for (ui32 j = 0U; j < size; ++j) {
                            auto it = listExpandMap.find(j);
                            if (it == listExpandMap.end()) {
                                inner.With(j, "out", j);
                                continue;
                            } else {
                                inner
                                    .With(j)
                                        .Callable("Unpickle")
                                            .Add(0, ExpandType(originalLambda->Pos(), *it->second, ctx))
                                            .Arg(1, "out", j)
                                        .Seal()
                                    .Done()
                                    ;
                            }
                        }
                        return inner;
                    })
                .Seal()
            .Seal().Build();
}

TExprNode::TPtr OptimizeWideCombiner(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto originalKeySize = node->Child(2U)->ChildrenSize() - 1U;
    if (const auto unused = UnusedArgs<3U>({node->Child(2)->Head().Children(), node->Child(3)->Head().Children().subspan(originalKeySize), node->Child(4)->Head().Children().subspan(originalKeySize)}); !unused.empty()) {
        YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " with " << unused.size() << " unused arguments.";
        return ctx.Builder(node->Pos())
            .Callable(node->Content())
                .Add(0, MakeWideMapForDropUnused(node->HeadPtr(), unused, ctx))
                .Add(1, node->ChildPtr(1))
                .Add(2, DropUnusedArgs(*node->Child(2), unused, ctx))
                .Add(3, DropUnusedArgs(*node->Child(3), unused, ctx, originalKeySize))
                .Add(4, DropUnusedArgs(*node->Child(4), unused, ctx, originalKeySize))
                .Add(5, node->ChildPtr(5))
            .Seal().Build();
    }

    const auto originalStateSize = node->Child(3U)->ChildrenSize() - 1U;
    const auto originalItemSize = node->Child(2U)->Head().ChildrenSize();
    TTupleExpandMap tupleExpandMap(originalKeySize);
    TStructExpandMap structExpandMap(originalKeySize);

    TListExpandMap listExpandMap;
    const auto needKeyFlatten = GetExpandMapsForLambda(*node->Child(2U), tupleExpandMap, structExpandMap, &listExpandMap);

    if (const auto selector = node->Child(2); selector != selector->Tail().GetDependencyScope()->second && originalKeySize == 1) {
        YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " by constant key.";
        return ctx.Builder(node->Pos())
            .Callable("WideMap")
                .Callable(0, "WideCondense1")
                    .Add(0, node->HeadPtr())
                    .Lambda(1)
                        .Params("item", selector->Head().ChildrenSize())
                        .Apply(*node->Child(3))
                            .With(0, selector->TailPtr())
                            .Do([&](TExprNodeReplaceBuilder& parent) -> TExprNodeReplaceBuilder& {
                                for (size_t i = 0; i < selector->Head().ChildrenSize(); ++i) {
                                    parent.With(i + 1, "item", i);
                                }
                                return parent;
                            })
                        .Seal()
                    .Seal()
                    .Lambda(2)
                        .Params("item", originalItemSize + originalStateSize)
                        .Callable("Bool")
                            .Atom(0, "false", TNodeFlags::Default)
                        .Seal()
                    .Seal()
                    .Lambda(3)
                        .Params("item", originalItemSize + originalStateSize)
                        .Apply(*node->Child(4))
                            .With(0, selector->TailPtr())
                            .Do([&](TExprNodeReplaceBuilder& parent) -> TExprNodeReplaceBuilder& {
                                for (size_t i = 0; i < originalItemSize + originalStateSize; ++i) {
                                    parent.With(i + 1, "item", i);
                                }
                                return parent;
                            })
                        .Seal()
                    .Seal()
                .Seal()
                .Lambda(1)
                    .Params("state", originalStateSize)
                    .Apply(*node->Child(5))
                        .With(0, selector->TailPtr())
                        .Do([&](TExprNodeReplaceBuilder& parent) -> TExprNodeReplaceBuilder& {
                            for (size_t i = 0; i < originalStateSize; ++i) {
                                parent.With(i + 1, "state", i);
                            }
                            return parent;
                        })
                    .Seal()
                .Seal()
            .Seal()
        .Build();
    }


    if (!listExpandMap.empty()) {
        auto children = node->ChildrenList();
        YQL_CLOG(DEBUG, CorePeepHole) << "Pickle " << listExpandMap.size() << " keys for " << node->Content();
        TExprNode::TListType extractorKeys = NYql::GetLambdaBody(*node->Child(2U));
        for (auto&& i : listExpandMap) {
            extractorKeys[i.first] = ctx.NewCallable(node->Pos(), "StablePickle", { extractorKeys[i.first] });
        }
        children[2U] = ctx.DeepCopyLambda(*children[2U], std::move(extractorKeys));

        children[3U] = UnpickleInput(children[3U], listExpandMap, ctx);
        children[4U] = UnpickleInput(children[4U], listExpandMap, ctx);
        children[5U] = UnpickleInput(children[5U], listExpandMap, ctx);

        return ctx.ChangeChildren(*node, std::move(children));
    }

    if (needKeyFlatten.front()) {
        const auto flattenSize = *needKeyFlatten.front();
        YQL_CLOG(DEBUG, CorePeepHole) << "Flatten key by tuple for " << node->Content() << " from " << originalKeySize << " to " << flattenSize;
        auto children = node->ChildrenList();

        FlattenLambdaBody(children[2U], tupleExpandMap, originalKeySize, flattenSize, ctx);
        FlattenLambdaArgs(children[3U], tupleExpandMap, originalKeySize, flattenSize, ctx);
        FlattenLambdaArgs(children[4U], tupleExpandMap, originalKeySize, flattenSize, ctx);
        FlattenLambdaArgs(children[5U], tupleExpandMap, originalKeySize, flattenSize, ctx);

        return ctx.ChangeChildren(*node, std::move(children));
    }

    if (needKeyFlatten.back()) {
        const auto flattenSize = *needKeyFlatten.back();
        YQL_CLOG(DEBUG, CorePeepHole) << "Flatten key by struct for " << node->Content() << " from " << originalKeySize << " to " << flattenSize;
        auto children = node->ChildrenList();

        TLieralStructsCacheMap membersMap;
        FlattenLambdaBody(children[2U], structExpandMap, originalKeySize, flattenSize, membersMap, ctx);
        FlattenLambdaArgs(children[3U], structExpandMap, originalKeySize, flattenSize, ctx);
        FlattenLambdaArgs(children[4U], structExpandMap, originalKeySize, flattenSize, ctx);
        FlattenLambdaArgs(children[5U], structExpandMap, originalKeySize, flattenSize, ctx);

        return ctx.ChangeChildren(*node, std::move(children));
    }

    tupleExpandMap.clear();
    structExpandMap.clear();
    tupleExpandMap.resize(originalStateSize);
    structExpandMap.resize(originalStateSize);

    const auto needStateFlatten = GetExpandMapsForLambda<3U>(*node->Child(3U), tupleExpandMap, structExpandMap);

    if (needStateFlatten.front()) {
        const auto flattenSize = *needStateFlatten.front();
        YQL_CLOG(DEBUG, CorePeepHole) << "Flatten state by tuple for " << node->Content() << " from " << originalStateSize << " to " << flattenSize;
        auto children = node->ChildrenList();

        FlattenLambdaBody(children[3U], tupleExpandMap, originalStateSize, flattenSize, ctx);
        FlattenLambdaBody(children[4U], tupleExpandMap, originalStateSize, flattenSize, ctx);
        FlattenLambdaArgs(children[4U], tupleExpandMap, originalStateSize, flattenSize, ctx, node->Child(3U)->Head().ChildrenSize());
        FlattenLambdaArgs(children[5U], tupleExpandMap, originalStateSize, flattenSize, ctx, node->Child(2U)->ChildrenSize() - 1U);

        return ctx.ChangeChildren(*node, std::move(children));
    }

    if (needStateFlatten.back()) {
        const auto flattenSize = *needStateFlatten.back();
        YQL_CLOG(DEBUG, CorePeepHole) << "Flatten state by struct for " << node->Content() << " from " << originalStateSize << " to " << flattenSize;
        auto children = node->ChildrenList();

        TLieralStructsCacheMap membersMap;
        FlattenLambdaBody(children[3U], structExpandMap, originalStateSize, flattenSize, membersMap, ctx);
        FlattenLambdaBody(children[4U], structExpandMap, originalStateSize, flattenSize, membersMap, ctx);
        FlattenLambdaArgs(children[4U], structExpandMap, originalStateSize, flattenSize, ctx, node->Child(3U)->Head().ChildrenSize());
        FlattenLambdaArgs(children[5U], structExpandMap, originalStateSize, flattenSize, ctx, node->Child(2U)->ChildrenSize() - 1U);

        return ctx.ChangeChildren(*node, std::move(children));
    }

    if (const auto dedups = DedupState(*node->Child(3), *node->Child(4)); !dedups.empty()) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Dedup " << node->Content() << ' ' << dedups.size() << " states.";

        auto children = node->ChildrenList();
        children[4U] = DedupLambdaBody(*children[4U], dedups, ctx);

        const auto& finish = node->Tail();
        const auto size = finish.Head().ChildrenSize();
        const auto skip = children[2U]->ChildrenSize() - 1U;

        std::vector<ui32> map(size);
        std::iota(map.begin(), map.end(), 0U);
        std::for_each(dedups.cbegin(), dedups.cend(), [&](const std::pair<ui32, ui32>& it){ map[it.second + skip] = it.first + skip; } );

        children.back() = ctx.Builder(finish.Pos())
            .Lambda()
                .Params("out", finish.Head().ChildrenSize())
                .Apply(finish)
                    .Do([&](TExprNodeReplaceBuilder& inner) -> TExprNodeReplaceBuilder& {
                        for (ui32 j = 0U; j < finish.Head().ChildrenSize(); ++j) {
                            inner.With(j, "out", map[j]);
                        }
                        return inner;
                    })
                .Seal()
            .Seal().Build();

        return ctx.ChangeChildren(*node, std::move(children));
    }

    if (const auto unused = UnusedState<1U>(*node->Child(3), *node->Child(4), {&node->Tail()}); !unused.empty()) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Unused " << node->Content() << ' ' << unused.size() << " states.";

        auto children = node->ChildrenList();
        const auto size = children[3]->ChildrenSize() - 1U;
        children[3] = ctx.DeepCopyLambda(*children[3], DropUnused(GetLambdaBody(*children[3]), unused));
        children[4] = ctx.DeepCopyLambda(*children[4], DropUnused(GetLambdaBody(*children[4]), unused));
        children[4] = ctx.ChangeChild(*children[4], 0U, ctx.NewArguments(children[4]->Head().Pos(), DropUnused(children[4]->Head().ChildrenList(), unused, children[4]->Head().ChildrenSize() - size)));
        children[5] = ctx.DeepCopyLambda(*children[5]);
        children[5] = ctx.ChangeChild(*children[5], 0U, ctx.NewArguments(children[5]->Head().Pos(), DropUnused(children[5]->Head().ChildrenList(), unused, children[5]->Head().ChildrenSize() - size)));

        return ctx.ChangeChildren(*node, std::move(children));
    }

    if (const auto stateToKeyIdxs = DedupAggregationKeysFromState(*node->Child(2), *node->Child(3), *node->Child(4)); !stateToKeyIdxs.empty()) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Dedup keys from state " << node->Content() << ' ' << stateToKeyIdxs.size() << " states.";

        auto children = node->ChildrenList();
        const auto itemsWidth = children[2]->Head().ChildrenSize();
        const auto keyWidth = children[2]->ChildrenSize() - 1;
        const auto statesWidth = children[3]->ChildrenSize() - 1;
        const auto buildRemappedLambda = [&](TExprNode& lambda, const ui32 statesShift, const ui32 keysShift) {
            return ctx.Builder(lambda.Pos())
                .Lambda()
                .Params("out", lambda.Head().ChildrenSize())
                    .Apply(lambda)
                        .Do([&](TExprNodeReplaceBuilder& inner) -> TExprNodeReplaceBuilder& {
                            for (ui32 j = 0; j < lambda.Head().ChildrenSize(); ++j) {
                                if (j < statesShift || j >= statesShift + statesWidth) {
                                    inner.With(j, "out", j);
                                } else {
                                    auto it = stateToKeyIdxs.find(j - statesShift);
                                    if (it == stateToKeyIdxs.end()) {
                                        inner.With(j, "out", j);
                                    } else {
                                        inner.With(j, "out", keysShift + it->second);
                                    }
                                }
                            }
                            return inner;
                        })
                    .Seal()
                .Seal().Build();
        };
        children[4] = buildRemappedLambda(*children[4], keyWidth + itemsWidth, 0);
        children[5] = buildRemappedLambda(*children[5], keyWidth, 0);

        auto body = GetLambdaBody(*children[3]);
        for (auto&& i : stateToKeyIdxs) {
            body[i.first] = children[3]->Head().Child(i.second);
        }
        children[3] = ctx.DeepCopyLambda(*children[3], std::move(body));

        return ctx.ChangeChildren(*node, std::move(children));
    }

    return node;
}

TExprNode::TPtr OptimizeWideCondense1(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (const auto unused = UnusedArgs<3U>({node->Child(1), node->Child(2), node->Child(3)}); !unused.empty()) {
        YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " with " << unused.size() << " unused arguments.";
        return ctx.Builder(node->Pos())
            .Callable(node->Content())
                .Add(0, MakeWideMapForDropUnused(node->HeadPtr(), unused, ctx))
                .Add(1, DropUnusedArgs(*node->Child(1), unused, ctx))
                .Add(2, DropUnusedArgs(*node->Child(2), unused, ctx))
                .Add(3, DropUnusedArgs(*node->Child(3), unused, ctx))
            .Seal().Build();
    }

    const auto originalSize = node->Tail().ChildrenSize() - 1U;
    TTupleExpandMap tupleExpandMap(originalSize);
    TStructExpandMap structExpandMap(originalSize);

    const auto inputWidth = node->Child(1U)->Head().ChildrenSize();
    const auto needFlatten = GetExpandMapsForLambda<3U>(node->Tail(), tupleExpandMap, structExpandMap);

    if (needFlatten.front()) {
        const auto flattenSize = *needFlatten.front();
        YQL_CLOG(DEBUG, CorePeepHole) << "Flatten " << node->Content() << " tuple outputs from " << originalSize << " to " << flattenSize;
        auto children = node->ChildrenList();

        FlattenLambdaBody(children[1U], tupleExpandMap, originalSize, flattenSize, ctx);
        FlattenLambdaBody(children[3U], tupleExpandMap, originalSize, flattenSize, ctx);
        FlattenLambdaArgs(children[2U], tupleExpandMap, originalSize, flattenSize, ctx, inputWidth);
        FlattenLambdaArgs(children[3U], tupleExpandMap, originalSize, flattenSize, ctx, inputWidth);

        auto mapper = MakeEmptyWideLambda(node->Pos(), originalSize, ctx);
        FlattenLambdaArgs(mapper, tupleExpandMap, originalSize, flattenSize, ctx);
        return ctx.Builder(node->Pos())
            .Callable("WideMap")
                .Add(0, ctx.ChangeChildren(*node, std::move(children)))
                .Add(1, std::move(mapper))
            .Seal().Build();
    }

    if (needFlatten.back()) {
        const auto flattenSize = *needFlatten.back();
        YQL_CLOG(DEBUG, CorePeepHole) << "Flatten " << node->Content() << " struct outputs from " << originalSize << " to " << flattenSize;
        auto children = node->ChildrenList();

        TLieralStructsCacheMap membersMap;
        FlattenLambdaBody(children[1U], structExpandMap, originalSize, flattenSize, membersMap, ctx);
        FlattenLambdaBody(children[3U], structExpandMap, originalSize, flattenSize, membersMap, ctx);
        FlattenLambdaArgs(children[2U], structExpandMap, originalSize, flattenSize, ctx, inputWidth);
        FlattenLambdaArgs(children[3U], structExpandMap, originalSize, flattenSize, ctx, inputWidth);

        auto mapper = MakeEmptyWideLambda(node->Pos(), originalSize, ctx);
        FlattenLambdaArgs(mapper, structExpandMap, originalSize, flattenSize, ctx);
        return ctx.Builder(node->Pos())
            .Callable("WideMap")
                .Add(0, ctx.ChangeChildren(*node, std::move(children)))
                .Add(1, std::move(mapper))
            .Seal().Build();
    }

    if (const auto dedups = DedupState(*node->Child(1), node->Tail()); !dedups.empty()) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Dedup " << node->Content() << ' ' << dedups.size() << " states.";
        return ctx.ChangeChild(*node, 3, DedupLambdaBody(node->Tail(), dedups, ctx));
    }

    return node;
}

TExprNode::TPtr OptimizeWideChopper(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto originalSize = node->Child(1U)->ChildrenSize() - 1U;
    TTupleExpandMap tupleExpandMap(originalSize);
    TStructExpandMap structExpandMap(originalSize);

    const auto needFlatten = GetExpandMapsForLambda(*node->Child(1U), tupleExpandMap, structExpandMap);

    if (needFlatten.front()) {
        const auto flattenSize = *needFlatten.front();
        YQL_CLOG(DEBUG, CorePeepHole) << "Flatten key by tuple for " << node->Content() << " from " << originalSize << " to " << flattenSize;
        auto children = node->ChildrenList();

        FlattenLambdaBody(children[1U], tupleExpandMap, originalSize, flattenSize, ctx);
        FlattenLambdaArgs(children[2U], tupleExpandMap, originalSize, flattenSize, ctx);
        FlattenLambdaArgs(children[3U], tupleExpandMap, originalSize, flattenSize, ctx);

        return ctx.ChangeChildren(*node, std::move(children));
    }

    if (needFlatten.back()) {
        const auto flattenSize = *needFlatten.back();
        YQL_CLOG(DEBUG, CorePeepHole) << "Flatten key by struct for " << node->Content() << " from " << originalSize << " to " << flattenSize;
        auto children = node->ChildrenList();

        TLieralStructsCacheMap membersMap;
        FlattenLambdaBody(children[1U], structExpandMap, originalSize, flattenSize, membersMap, ctx);
        FlattenLambdaArgs(children[2U], structExpandMap, originalSize, flattenSize, ctx);
        FlattenLambdaArgs(children[3U], structExpandMap, originalSize, flattenSize, ctx);

        return ctx.ChangeChildren(*node, std::move(children));
    }

    return node;
}


struct TBlockRules {

    // all kernels whose name begins with capital letter are YQL kernel
    static constexpr std::initializer_list<TBlockFuncMap::value_type> FuncsInit = {
        {"Abs", { "Abs" } },
        {"Minus", { "Minus" } },

        {"+", { "Add" } },
        {"-", { "Sub" } },
        {"*", { "Mul" } },
        {"/", { "Div" } },
        {"%", { "Mod" } },

        // comparison kernels
        {"==", { "Equals" } },
        {"!=", { "NotEquals" } },
        {"<",  { "Less" } },
        {"<=", { "LessOrEqual" } },
        {">",  { "Greater" } },
        {">=", { "GreaterOrEqual" } },

        // string kernels
        {"Size", { "Size" } },
        {"StartsWith", { "StartsWith" } },
        {"EndsWith", { "EndsWith" } },
        {"StringContains", { "StringContains" } },
    };

    TBlockRules()
        : Funcs(FuncsInit)
    {}

    static const TBlockRules& Instance() {
        return *Singleton<TBlockRules>();
    }

    const TBlockFuncMap Funcs;
};

TExprNode::TPtr SplitByPairs(TPositionHandle pos, const TStringBuf& funcName, const TExprNode::TListType& funcArgs,
    size_t begin, size_t end, TExprContext& ctx)
{
    YQL_ENSURE(end >= begin + 2);
    const size_t len = end - begin;
    if (len < 4) {
        auto result = ctx.NewCallable(pos, funcName, { funcArgs[begin], funcArgs[begin + 1] });
        if (len == 3) {
            result = ctx.NewCallable(pos, funcName, { result, funcArgs[begin + 2] });
        }
        return result;
    }

    auto left = SplitByPairs(pos, funcName, funcArgs, begin, begin + len / 2, ctx);
    auto right = SplitByPairs(pos, funcName, funcArgs, begin + len / 2, end, ctx);
    return ctx.NewCallable(pos, funcName, { left, right });
}

using TExprNodePtrPred = std::function<bool(const TExprNode::TPtr&)>;

TExprNodePtrPred MakeBlockRewriteStopPredicate(const TExprNode::TPtr& lambda) {
    return [lambda](const TExprNode::TPtr& node) {
        return node->IsArguments() || (node->IsLambda() && node != lambda);
    };
}

void DoMarkLazy(const TExprNode::TPtr& node, TNodeSet& lazyNodes, const TExprNodePtrPred& needStop, TNodeSet& visited, bool markAll) {
    if (!visited.insert(node.Get()).second) {
        return;
    }

    if (needStop(node)) {
        return;
    }

    if (markAll) {
        lazyNodes.insert(node.Get());
    }

    const bool isLazyNode = node->IsCallable({"And", "Or", "If", "Coalesce"});
    for (ui32 i = 0; i < node->ChildrenSize(); ++i) {
        DoMarkLazy(node->ChildPtr(i), lazyNodes, needStop, visited, markAll || (isLazyNode && i > 0));
    }
}

void MarkLazy(const TExprNode::TPtr& node, TNodeSet& lazyNodes, const TExprNodePtrPred& needStop) {
    TNodeSet visited;
    DoMarkLazy(node, lazyNodes, needStop, visited, false);
}

void DoMarkNonLazy(const TExprNode::TPtr& node, TNodeSet& lazyNodes, const TExprNodePtrPred& needStop, TNodeSet& visited) {
    if (!visited.insert(node.Get()).second) {
        return;
    }

    if (needStop(node)) {
        return;
    }

    lazyNodes.erase(node.Get());
    ui32 endIndex = node->IsCallable({"And", "Or", "If", "Coalesce"}) ? 1 : node->ChildrenSize();
    for (ui32 i = 0; i < endIndex; ++i) {
        DoMarkNonLazy(node->ChildPtr(i), lazyNodes, needStop, visited);
    }
}

void MarkNonLazy(const TExprNode::TPtr& node, TNodeSet& lazyNodes, const TExprNodePtrPred& needStop) {
    TNodeSet visited;
    DoMarkNonLazy(node, lazyNodes, needStop, visited);
}

TNodeSet CollectLazyNonStrictNodes(const TExprNode::TPtr& lambda) {
    TNodeSet nonStrictNodes;
    VisitExpr(lambda, [&](const TExprNode::TPtr& node) {
        if (node->IsArguments() || node->IsArgument()) {
            return false;
        }

        auto type = node->GetTypeAnn();
        YQL_ENSURE(type);

        // avoid visiting any possible scalar context
        return type->IsComposable();
    }, [&](const TExprNode::TPtr& node) {
        YQL_ENSURE(!nonStrictNodes.contains(node.Get()));
        if (node->IsCallable("AssumeStrict")) {
            return true;
        }
        if (node->IsCallable("AssumeNonStrict")) {
            nonStrictNodes.insert(node.Get());
            return true;
        }
        if (AnyOf(node->ChildrenList(), [&](const auto& child) { return nonStrictNodes.contains(child.Get()); }))
        {
            nonStrictNodes.insert(node.Get());
            return true;
        }

        if (auto maybeStrict = IsStrictNoRecurse(*node); maybeStrict.Defined() && !*maybeStrict) {
            nonStrictNodes.insert(node.Get());
            return true;
        }

        return true;
    });

    auto needStop = MakeBlockRewriteStopPredicate(lambda);

    TNodeSet lazyNodes;
    MarkLazy(lambda, lazyNodes, needStop);
    MarkNonLazy(lambda, lazyNodes, needStop);

    TNodeSet lazyNonStrict;
    for (auto& node : lazyNodes) {
        if (nonStrictNodes.contains(node)) {
            lazyNonStrict.insert(node);
        }
    }
    return lazyNonStrict;
}

bool CollectBlockRewrites(const TMultiExprType* multiInputType, bool keepInputColumns, const TExprNode::TPtr& lambda,
    ui32& newNodes, TNodeMap<size_t>& rewritePositions,
    TExprNode::TPtr& blockLambda, TExprNode::TPtr& restLambda,
    TExprContext& ctx, TTypeAnnotationContext& types)
{
    YQL_ENSURE(lambda && lambda->IsLambda());
    const auto& funcs = TBlockRules::Instance().Funcs;
    if (!types.ArrowResolver) {
        return false;
    }

    TVector<const TTypeAnnotationNode*> allInputTypes;
    for (const auto& i : multiInputType->GetItems()) {
        if (i->IsBlockOrScalar()) {
            return false;
        }

        allInputTypes.push_back(i);
    }

    auto resolveStatus = types.ArrowResolver->AreTypesSupported(ctx.GetPosition(lambda->Pos()), allInputTypes, ctx);
    YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
    if (resolveStatus != IArrowResolver::OK) {
        return false;
    }

    TExprNode::TListType blockArgs;
    for (ui32 i = 0; i < multiInputType->GetSize() + 1; ++i) { // last argument is used for length of blocks
        blockArgs.push_back(ctx.NewArgument(lambda->Pos(), "arg" + ToString(i)));
    }

    TNodeOnNodeOwnedMap rewrites;
    YQL_ENSURE(multiInputType->GetSize() == lambda->Head().ChildrenSize());
    for (ui32 i = 0; i < multiInputType->GetSize(); ++i) {
        rewrites[lambda->Head().Child(i)] = blockArgs[i];
    }

    const TNodeSet lazyNonStrict = CollectLazyNonStrictNodes(lambda);
    auto needStop = MakeBlockRewriteStopPredicate(lambda);

    newNodes = 0;
    VisitExpr(lambda, [&](const TExprNode::TPtr& node) {
        return !needStop(node);
    }, [&](const TExprNode::TPtr& node) {
        if (rewrites.contains(node.Get())) {
            return true;
        }

        if (node->IsArguments() || node->IsLambda()) {
            return true;
        }

        if (node->IsComplete()) {
            return true;
        }

        if (!node->IsList() && !node->IsCallable()) {
            return true;
        }

        if (node->IsList() && (!node->GetTypeAnn()->IsComputable() || node->IsLiteralList())) {
            return true;
        }

        if (lazyNonStrict.contains(node.Get())) {
            return true;
        }

        TExprNode::TListType funcArgs;
        std::string_view arrowFunctionName;
        const bool rewriteAsIs = node->IsCallable({"AssumeStrict", "AssumeNonStrict", "Likely"});
        if (node->IsList() || rewriteAsIs ||
            node->IsCallable({"And", "Or", "Xor", "Not", "Coalesce", "Exists", "If", "Just", "AsStruct", "Member", "Nth", "ToPg", "FromPg", "PgResolvedCall", "PgResolvedOp"}))
        {
            if (node->IsCallable() && !IsSupportedAsBlockType(node->Pos(), *node->GetTypeAnn(), ctx, types)) {
                return true;
            }

            ui32 startIndex = 0;
            if (node->IsCallable("PgResolvedCall")) {
                if (node->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Pg) {
                    return true;
                }

                startIndex = 3;
            } else if (node->IsCallable("PgResolvedOp")) {
                if (node->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Pg) {
                    return true;
                }

                startIndex = 2;
            }

            for (ui32 index = 0; index < startIndex; ++index) {
                auto child = node->ChildPtr(index);
                funcArgs.push_back(child);
            }

            for (ui32 index = startIndex; index < node->ChildrenSize(); ++index) {
                auto child = node->ChildPtr(index);
                if (!child->GetTypeAnn()->IsComputable()) {
                    funcArgs.push_back(child);
                } else if (child->IsComplete() && IsSupportedAsBlockType(child->Pos(), *child->GetTypeAnn(), ctx, types)) {
                    funcArgs.push_back(ctx.NewCallable(node->Pos(), "AsScalar", { child }));
                } else if (auto rit = rewrites.find(child.Get()); rit != rewrites.end()) {
                    funcArgs.push_back(rit->second);
                } else {
                    return true;
                }
            }

            // <AsStruct> arguments (i.e. members of the resulting structure)
            // are literal tuples, that don't propagate their child rewrites.
            // Hence, process these rewrites the following way: wrap the
            // complete expressions, supported by the block engine, with
            // <AsScalar> callable or apply the rewrite of one is found.
            // Otherwise, abort this <AsStruct> rewrite, since one of its
            // arguments is neither block nor scalar.
            if (node->IsCallable("AsStruct")) {
                for (ui32 index = 0; index < node->ChildrenSize(); index++) {
                    auto member = funcArgs[index];
                    auto child = member->TailPtr();
                    TExprNodePtr rewrite;
                    if (child->IsComplete() && IsSupportedAsBlockType(child->Pos(), *child->GetTypeAnn(), ctx, types)) {
                        rewrite = ctx.NewCallable(child->Pos(), "AsScalar", { child });
                    } else if (auto rit = rewrites.find(child.Get()); rit != rewrites.end()) {
                        rewrite = rit->second;
                    } else {
                        return true;
                    }
                    funcArgs[index] = ctx.NewList(member->Pos(), {member->HeadPtr(), rewrite});
                }
            }

            const TString blockFuncName = rewriteAsIs ? ToString(node->Content()) :
                (TString("Block") + (node->IsList() ? "AsTuple" : node->Content()));
            if (node->IsCallable({"And", "Or", "Xor"}) && funcArgs.size() > 2) {
                // Split original argument list by pairs (since the order is not important balanced tree is used)
                rewrites[node.Get()] = SplitByPairs(node->Pos(), blockFuncName, funcArgs, 0, funcArgs.size(), ctx);
            } else {
                rewrites[node.Get()] = ctx.NewCallable(node->Pos(), blockFuncName, std::move(funcArgs));
            }
            ++newNodes;
            return true;
        }
        const bool isUdf = node->IsCallable("Apply") && node->Head().IsCallable("Udf");
        if (isUdf) {
            if (!GetSetting(*node->Head().Child(7), "blocks")) {
                return true;
            }
        }

        {
            TVector<const TTypeAnnotationNode*> allTypes;
            allTypes.push_back(node->GetTypeAnn());
            for (ui32 i = isUdf ? 1 : 0; i < node->ChildrenSize(); ++i) {
                allTypes.push_back(node->Child(i)->GetTypeAnn());
            }

            auto resolveStatus = types.ArrowResolver->AreTypesSupported(ctx.GetPosition(node->Pos()), allTypes, ctx);
            YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
            if (resolveStatus != IArrowResolver::OK) {
                return true;
            }
        }

        TVector<const TTypeAnnotationNode*> argTypes;
        bool hasBlockArg = false;
        for (ui32 i = isUdf ? 1 : 0; i < node->ChildrenSize(); ++i) {
            auto child = node->Child(i);
            if (child->IsComplete()) {
                argTypes.push_back(ctx.MakeType<TScalarExprType>(child->GetTypeAnn()));
            } else {
                hasBlockArg = true;
                argTypes.push_back(ctx.MakeType<TBlockExprType>(child->GetTypeAnn()));
            }
        }

        YQL_ENSURE(!node->IsComplete() && hasBlockArg);
        const TTypeAnnotationNode* outType = ctx.MakeType<TBlockExprType>(node->GetTypeAnn());
        if (isUdf) {
            TExprNode::TPtr extraTypes;
            bool renameFunc = false;
            if (node->Head().Child(2)->IsCallable("TupleType")) {
                extraTypes = node->Head().Child(2)->ChildPtr(2);
            } else {
                renameFunc = true;
                extraTypes = ctx.NewCallable(node->Head().Pos(), "TupleType", {});
            }

            funcArgs.push_back(ctx.Builder(node->Head().Pos())
                .Callable("Udf")
                    .Atom(0, TString(node->Head().Child(0)->Content()) + (renameFunc ? "_BlocksImpl" : ""))
                    .Add(1, node->Head().ChildPtr(1))
                    .Callable(2, "TupleType")
                        .Callable(0, "TupleType")
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    for (ui32 i = 1; i < node->ChildrenSize(); ++i) {
                                        auto type = argTypes[i - 1];
                                        parent.Add(i - 1, ExpandType(node->Head().Pos(), *type, ctx));
                                    }

                                    return parent;
                                })
                        .Seal()
                        .Callable(1, "StructType")
                        .Seal()
                        .Add(2, extraTypes)
                    .Seal()
                    .Add(3, node->Head().ChildPtr(3))
                .Seal()
                .Build());

            if (HasSetting(*node->Head().Child(7), "strict")) {
                auto newArg = ctx.Builder(node->Head().Pos())
                    .Callable("EnsureStrict")
                        .Add(0, funcArgs.back())
                        .Atom(1, TStringBuilder() << "Block version of " << node->Head().Child(0)->Content() << " is not marked as strict")
                    .Seal()
                    .Build();
                funcArgs.back() = std::move(newArg);
            }
        } else {
            auto fit = funcs.find(node->Content());
            if (fit == funcs.end()) {
                return true;
            }

            arrowFunctionName = fit->second.Name;
            funcArgs.push_back(ctx.NewAtom(node->Pos(), arrowFunctionName));

            auto resolveStatus = types.ArrowResolver->LoadFunctionMetadata(ctx.GetPosition(node->Pos()), arrowFunctionName, argTypes, outType, ctx);
            YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
            if (resolveStatus != IArrowResolver::OK) {
                return true;
            }
            funcArgs.push_back(ExpandType(node->Pos(), *outType, ctx));
        }

        for (ui32 i = isUdf ? 1 : 0; i < node->ChildrenSize(); ++i) {
            auto child = node->Child(i);
            if (child->IsComplete()) {
                funcArgs.push_back(ctx.NewCallable(node->Pos(), "AsScalar", { node->ChildPtr(i) }));
            } else {
                auto rit = rewrites.find(child);
                if (rit == rewrites.end()) {
                    return true;
                }

                funcArgs.push_back(rit->second);
            }
        }

        rewrites[node.Get()] = ctx.NewCallable(node->Pos(), isUdf ? "Apply" : "BlockFunc", std::move(funcArgs));
        ++newNodes;
        return true;
    });

    // calculate extra columns
    TNodeOnNodeOwnedMap replaces;
    TExprNode::TListType lambdaArgs, roots;
    if (keepInputColumns) {
        // put original columns first
        for (ui32 i = 0; i < lambda->Head().ChildrenSize(); ++i) {
            auto originalArg = lambda->Head().Child(i);
            auto it = rewrites.find(originalArg);
            YQL_ENSURE(it != rewrites.end());
            YQL_ENSURE(it->second == blockArgs[i]);
            auto arg = ctx.NewArgument(lambda->Pos(), "arg" + ToString(lambdaArgs.size()));
            lambdaArgs.push_back(arg);
            rewritePositions[originalArg] = roots.size();
            roots.push_back(it->second);
        }
    }

    for (ui32 i = 1; i < lambda->ChildrenSize(); ++i) {
        if (lambda->ChildPtr(i)->IsComplete()) {
            TVector<const TTypeAnnotationNode*> allTypes;
            allTypes.push_back(lambda->ChildPtr(i)->GetTypeAnn());
            auto resolveStatus = types.ArrowResolver->AreTypesSupported(ctx.GetPosition(lambda->Pos()), allTypes, ctx);
            YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
            if (resolveStatus == IArrowResolver::OK) {
                rewrites[lambda->Child(i)] = ctx.NewCallable(lambda->Pos(), "AsScalar", { lambda->ChildPtr(i) });
                ++newNodes;
            }
        }

        VisitExpr(lambda->ChildPtr(i), [&](const TExprNode::TPtr& node) {
            auto it = rewrites.find(node.Get());
            if (it != rewrites.end()) {
                if (!replaces.contains(node.Get())) {
                    auto arg = ctx.NewArgument(node->Pos(), "arg" + ToString(lambdaArgs.size()));
                    lambdaArgs.push_back(arg);
                    replaces[node.Get()] = arg;
                    rewritePositions[node.Get()] = roots.size();
                    roots.push_back(it->second);
                }

                return false;
            }

            return true;
        });
    }

    if (keepInputColumns) {
        // add original columns to replaces if not already added
        for (ui32 i = 0; i < lambda->Head().ChildrenSize(); ++i) {
            auto originalArg = lambda->Head().Child(i);
            if (!replaces.contains(originalArg)) {
                replaces[originalArg] = lambdaArgs[i];
            }
        }
    }

    YQL_ENSURE(lambdaArgs.size() == roots.size());
    roots.push_back(blockArgs.back());

    blockLambda = ctx.NewLambda(lambda->Pos(), ctx.NewArguments(lambda->Pos(), std::move(blockArgs)), std::move(roots));

    TExprNode::TListType restRoots;
    for (ui32 i = 1; i < lambda->ChildrenSize(); ++i) {
        TExprNode::TPtr newRoot;
        auto status = RemapExpr(lambda->ChildPtr(i), newRoot, replaces, ctx, TOptimizeExprSettings(&types));
        YQL_ENSURE(status != IGraphTransformer::TStatus::Error);
        restRoots.push_back(newRoot);
    }
    restLambda = ctx.NewLambda(lambda->Pos(), ctx.NewArguments(lambda->Pos(), std::move(lambdaArgs)), std::move(restRoots));

    return true;
}

bool CanRewriteToBlocksWithInput(const TExprNode& input, const TTypeAnnotationContext& types) {
    EBlockEngineMode effectiveMode = types.UseBlocks ? EBlockEngineMode::Force : types.BlockEngineMode;
    switch (effectiveMode) {
        case NYql::EBlockEngineMode::Disable:
            return false;
        case NYql::EBlockEngineMode::Auto:
            return input.IsCallable("WideFromBlocks");
        case NYql::EBlockEngineMode::Force:
            return true;
    }
    Y_UNREACHABLE();
}

TExprNode::TPtr OptimizeWideMapBlocks(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    Y_ENSURE(node->IsCallable("WideMap"));
    const auto lambda = node->TailPtr();
    // Swap trivial WideMap and WideFromBlocks.
    if (node->Head().IsCallable("WideFromBlocks")) {
        if (auto newLambda = RebuildArgumentsOnlyLambdaForBlocks(*lambda, ctx, types)) {
            YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Head().Content() << " with " << node->Content();
            return ctx.Builder(node->Pos())
                .Callable("WideFromBlocks")
                    .Callable(0, "WideMap")
                        .Add(0, node->Head().HeadPtr())
                        .Add(1, newLambda)
                    .Seal()
                .Seal()
                .Build();
        }
    }

    if (!CanRewriteToBlocksWithInput(node->Head(), types)) {
        return node;
    }

    auto multiInputType = node->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>();
    ui32 newNodes;
    TNodeMap<size_t> rewritePositions;
    TExprNode::TPtr blockLambda;
    TExprNode::TPtr restLambda;
    bool keepInputColumns = false;
    if (!CollectBlockRewrites(multiInputType, keepInputColumns, lambda, newNodes, rewritePositions, blockLambda, restLambda, ctx, types)) {
        return node;
    }

    if (!newNodes) {
        return node;
    }

    YQL_CLOG(DEBUG, CorePeepHole) << "Convert " << node->Content() << " to blocks, extra nodes: " << newNodes
                                  << ", extra columns: " << rewritePositions.size();

    auto ret = ctx.Builder(node->Pos())
        .Callable("WideFromBlocks")
            .Callable(0, "WideMap")
                .Callable(0, "WideToBlocks")
                    .Add(0, node->HeadPtr())
                .Seal()
                .Add(1, blockLambda)
            .Seal()
        .Seal()
        .Build();

    return ctx.Builder(node->Pos())
        .Callable(node->Content())
            .Add(0, ret)
            .Add(1, restLambda)
        .Seal()
        .Build();
}

TExprNode::TPtr OptimizeWideFilterBlocks(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    if (!CanRewriteToBlocksWithInput(node->Head(), types)) {
        return node;
    }

    auto multiInputType = node->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>();
    auto lambda = node->ChildPtr(1);
    YQL_ENSURE(lambda->ChildrenSize() == 2); // filter lambda should have single output

    ui32 newNodes;
    TNodeMap<size_t> rewritePositions;
    TExprNode::TPtr blockLambda;
    TExprNode::TPtr restLambda;
    bool keepInputColumns = true;
    if (!CollectBlockRewrites(multiInputType, keepInputColumns, lambda, newNodes, rewritePositions, blockLambda, restLambda, ctx, types)) {
        return node;
    }

    auto blockMapped = ctx.Builder(node->Pos())
        .Callable("WideMap")
            .Callable(0, "WideToBlocks")
                .Add(0, node->HeadPtr())
            .Seal()
            .Add(1, blockLambda)
        .Seal()
        .Build();

    if (auto it = rewritePositions.find(lambda->Child(1)); it != rewritePositions.end()) {
        // lambda is block-friendly
        YQL_ENSURE(it->second == multiInputType->GetSize(), "Block filter column must follow original input columns");
        auto result = ctx.Builder(node->Pos())
            .Callable("BlockCompress")
                .Add(0, blockMapped)
                .Atom(1, it->second)
            .Seal()
            .Build();

        if (node->ChildrenSize() == 3) {
            result = ctx.Builder(node->Pos())
                .Callable("WideTakeBlocks")
                    .Add(0, result)
                    .Add(1, node->ChildPtr(2))
                .Seal()
                .Build();
        }

        YQL_CLOG(DEBUG, CorePeepHole) << "Convert " << node->Content() << " to blocks, extra nodes: " << newNodes
                                      << ", extra columns: " << rewritePositions.size();
        return ctx.Builder(node->Pos())
            .Callable("WideFromBlocks")
                .Add(0, result)
            .Seal()
            .Build();
    }

    if (!newNodes) {
        return node;
    }

    auto filtered = ctx.Builder(node->Pos())
        .Callable("WideFilter")
            .Callable(0, "WideFromBlocks")
                .Add(0, blockMapped)
            .Seal()
            .Add(1, restLambda)
        .Seal()
        .Build();

    if (node->ChildrenSize() == 3) {
        filtered = ctx.Builder(node->Pos())
            .Callable("Take")
                .Add(0, filtered)
                .Add(1, node->ChildPtr(2))
            .Seal()
            .Build();
    }

    YQL_ENSURE(blockLambda->ChildrenSize() > 1);
    const size_t blockOutputs = blockLambda->ChildrenSize() - 2; // last column is a block count
    TExprNode::TListType allArgs;
    for (size_t i = 0; i < blockOutputs; ++i) {
        allArgs.push_back(ctx.NewArgument(node->Pos(), "arg" + ToString(i)));
    }

    YQL_ENSURE(allArgs.size() > multiInputType->GetSize());
    TExprNode::TListType outputs = allArgs;
    outputs.resize(multiInputType->GetSize());

    auto removeRootsLambda = ctx.NewLambda(node->Pos(), ctx.NewArguments(node->Pos(), std::move(allArgs)), std::move(outputs));
    YQL_CLOG(DEBUG, CorePeepHole) << "Convert " << node->Content() << " to (partial) blocks, extra nodes: " << newNodes
                                  << ", extra columns: " << rewritePositions.size();
    return ctx.Builder(node->Pos())
        .Callable("WideMap")
            .Add(0, filtered)
            .Add(1, removeRootsLambda)
        .Seal()
        .Build();
}

TExprNode::TPtr OptimizeSkipTakeToBlocks(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    if (!types.ArrowResolver) {
        return node;
    }

    if (node->Head().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Flow) {
        return node;
    }

    auto flowItemType = node->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();
    if (flowItemType->GetKind() != ETypeAnnotationKind::Multi) {
        return node;
    }

    const auto& allTypes = flowItemType->Cast<TMultiExprType>()->GetItems();
    if (AnyOf(allTypes, [](const TTypeAnnotationNode* type) { return type->IsBlockOrScalar(); })) {
        return node;
    }

    auto resolveStatus = types.ArrowResolver->AreTypesSupported(ctx.GetPosition(node->Head().Pos()),
        TVector<const TTypeAnnotationNode*>(allTypes.begin(), allTypes.end()), ctx);
    YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
    if (resolveStatus != IArrowResolver::OK) {
        return node;
    }

    if (!CanRewriteToBlocksWithInput(node->Head(), types)) {
        return node;
    }

    TStringBuf newName = node->Content() == "Skip" ? "WideSkipBlocks" : "WideTakeBlocks";
    YQL_CLOG(DEBUG, CorePeepHole) << "Convert " << node->Content() << " to " << newName;
    return ctx.Builder(node->Pos())
        .Callable("WideFromBlocks")
            .Callable(0, newName)
                .Callable(0, "WideToBlocks")
                    .Add(0, node->HeadPtr())
                .Seal()
                .Add(1, node->ChildPtr(1))
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr OptimizeTopOrSortBlocks(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    if (!types.ArrowResolver) {
        return node;
    }

    if (node->Head().GetTypeAnn()->GetKind() != ETypeAnnotationKind::Flow) {
        return node;
    }

    auto flowItemType = node->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();
    if (flowItemType->GetKind() != ETypeAnnotationKind::Multi) {
        return node;
    }

    const auto& allTypes = flowItemType->Cast<TMultiExprType>()->GetItems();
    if (AnyOf(allTypes, [](const TTypeAnnotationNode* type) { return type->IsBlockOrScalar(); })) {
        return node;
    }

    auto resolveStatus = types.ArrowResolver->AreTypesSupported(ctx.GetPosition(node->Head().Pos()),
        TVector<const TTypeAnnotationNode*>(allTypes.begin(), allTypes.end()), ctx);
    YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
    if (resolveStatus != IArrowResolver::OK) {
        return node;
    }

    if (!CanRewriteToBlocksWithInput(node->Head(), types)) {
        return node;
    }

    TString newName = node->Content() + TString("Blocks");
    YQL_CLOG(DEBUG, CorePeepHole) << "Convert " << node->Content() << " to " << newName;
    auto children = node->ChildrenList();
    children[0] = ctx.NewCallable(node->Pos(), "WideToBlocks", { children[0] });

    return ctx.Builder(node->Pos())
        .Callable("WideFromBlocks")
            .Add(0, ctx.NewCallable(node->Pos(), newName, std::move(children)))
        .Seal()
        .Build();
}

TExprNode::TPtr UpdateBlockCombineColumns(const TExprNode::TPtr& node, std::optional<ui32> filterColumn, const TVector<ui32>& argIndices, TExprContext& ctx) {
    auto combineChildren = node->ChildrenList();
    combineChildren[0] = node->Head().HeadPtr();
    if (filterColumn) {
        YQL_ENSURE(combineChildren[1]->IsCallable("Void"), "Filter column is already used");
        combineChildren[1] = ctx.NewAtom(node->Pos(), *filterColumn);
    } else {
        if (!combineChildren[1]->IsCallable("Void")) {
            combineChildren[1] = ctx.NewAtom(node->Pos(), argIndices[FromString<ui32>(combineChildren[1]->Content())]);
        }
    }

    const bool hashed = node->Content().EndsWith("Hashed");
    if (hashed) {
        auto keyNodes = combineChildren[2]->ChildrenList();
        for (auto& p : keyNodes) {
            p = ctx.NewAtom(node->Pos(), argIndices[FromString<ui32>(p->Content())]);
        }

        combineChildren[2] = ctx.ChangeChildren(*combineChildren[2], std::move(keyNodes));
    }

    auto payloadIndex = hashed ? 3 : 2;
    auto payloadNodes = combineChildren[payloadIndex]->ChildrenList();
    for (auto& p : payloadNodes) {
        YQL_ENSURE(p->IsList() && p->ChildrenSize() >= 1 && p->Head().IsCallable("AggBlockApply"), "Expected AggBlockApply");
        auto payloadArgs = p->ChildrenList();
        for (ui32 i = 1; i < payloadArgs.size(); ++i) {
            payloadArgs[i] = ctx.NewAtom(node->Pos(), argIndices[FromString<ui32>(payloadArgs[i]->Content())]);
        }

        p = ctx.ChangeChildren(*p, std::move(payloadArgs));
    }

    combineChildren[payloadIndex] = ctx.ChangeChildren(*combineChildren[payloadIndex], std::move(payloadNodes));
    return ctx.ChangeChildren(*node, std::move(combineChildren));
}

TExprNode::TPtr OptimizeBlockCombine(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    Y_UNUSED(types);
    if (node->Head().IsCallable("WideMap")) {
        const auto& lambda = node->Head().Tail();
        TVector<ui32> argIndices;
        bool onlyArguments = IsArgumentsOnlyLambda(lambda, argIndices);
        if (onlyArguments) {
            YQL_CLOG(DEBUG, CorePeepHole) << "Drop renaming WideMap under " << node->Content();
            return UpdateBlockCombineColumns(node, {}, argIndices, ctx);
        }
    }

    if (node->Head().IsCallable("BlockCompress") && node->Child(1)->IsCallable("Void")) {
        auto filterIndex = FromString<ui32>(node->Head().Child(1)->Content());
        TVector<ui32> argIndices;
        argIndices.resize(node->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetSize());
        for (ui32 i = 0; i < argIndices.size(); ++i) {
            argIndices[i] = (i < filterIndex) ? i : i + 1;
        }

        YQL_CLOG(DEBUG, CorePeepHole) << "Fuse " << node->Content() << " with " << node->Head().Content();
        return UpdateBlockCombineColumns(node, filterIndex, argIndices, ctx);
    }

    if (node->Head().IsCallable("ReplicateScalars")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Drop " << node->Head().Content() << " as input of " << node->Content();
        return ctx.ChangeChild(*node, 0, node->Head().HeadPtr());
    }

    return node;
}

TExprNode::TPtr OptimizeBlockMerge(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    Y_UNUSED(types);
    if (node->Head().IsCallable("ReplicateScalars")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Drop " << node->Head().Content() << " as input of " << node->Content();
        return ctx.ChangeChild(*node, 0, node->Head().HeadPtr());
    }

    return node;
}

TExprNode::TPtr SwapReplicateScalarsWithWideMap(const TExprNode::TPtr& wideMap, TExprContext& ctx) {
    YQL_ENSURE(wideMap->IsCallable("WideMap") && wideMap->Head().IsCallable("ReplicateScalars"));
    const auto& input = wideMap->Head();
    auto inputTypes = input.GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetItems();
    YQL_ENSURE(inputTypes.size() > 0);

    THashSet<ui32> replicatedInputIndexes;
    auto replicateScalarsInputTypes = input.Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetItems();
    YQL_ENSURE(replicateScalarsInputTypes.size() > 0);
    if (input.ChildrenSize() == 1) {
        for (ui32 i = 0; i + 1 < replicateScalarsInputTypes.size(); ++i) {
            if (replicateScalarsInputTypes[i]->IsScalar()) {
                replicatedInputIndexes.insert(i);
            }
        }
    } else {
        for (auto& indexNode : input.Child(1)->ChildrenList()) {
            YQL_ENSURE(indexNode->IsAtom());
            ui32 idx = FromString<ui32>(indexNode->Content());
            YQL_ENSURE(idx + 1 < replicatedInputIndexes.size() && replicateScalarsInputTypes[idx]->IsScalar());
            replicatedInputIndexes.insert(idx);
        }
    }

    YQL_ENSURE(!replicatedInputIndexes.empty(), "ReplicateScalars should be dropped earlier");

    const auto& lambda = wideMap->Tail();
    const auto& lambdaArgs = lambda.Head();

    YQL_ENSURE(lambdaArgs.ChildrenSize() == inputTypes.size());

    TNodeMap<ui32> arg2index;
    for (ui32 i = 0; i < lambdaArgs.ChildrenSize(); ++i) {
        YQL_ENSURE(arg2index.emplace(lambdaArgs.Child(i), i).second);
    }

    TVector<THashSet<ui32>> bodyItemDeps;
    for (ui32 i = 1; i + 1 < lambda.ChildrenSize(); ++i) {
        bodyItemDeps.emplace_back();
        VisitExpr(lambda.ChildPtr(i), [&](const TExprNode::TPtr& node) {
            if (node->IsArgument()) {
                auto it = arg2index.find(node.Get());
                if (it != arg2index.end()) {
                    bodyItemDeps.back().insert(it->second);
                }
            }
            return true;
        });
    }

    TExprNodeList replicatedOutputIndexes;
    for (ui32 i = 0; i < bodyItemDeps.size(); ++i) {
        const auto& currDeps = bodyItemDeps[i];
        if (!AnyOf(replicatedInputIndexes, [&](ui32 idx) { return currDeps.contains(idx); })) {
            continue;
        }

        // output body item depends on some of replicated scalars
        if (AllOf(currDeps, [&](ui32 idx) { return replicatedInputIndexes.contains(idx) || inputTypes[idx]->IsScalar(); })) {
            // output body items only depends on scalars or replicated scalars
            replicatedOutputIndexes.push_back(ctx.NewAtom(input.Pos(), i));
        }
    }

    return ctx.Builder(wideMap->Pos())
        .Callable("ReplicateScalars")
            .Callable(0, "WideMap")
                .Add(0, input.HeadPtr())
                .Add(1, ctx.DeepCopyLambda(lambda))
            .Seal()
            .Add(1, ctx.NewList(input.Pos(), std::move(replicatedOutputIndexes)))
        .Seal()
        .Build();
}

TExprNode::TPtr OptimizeWideMaps(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (const auto& input = node->Head(); input.IsCallable("ExpandMap")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Fuse " << node->Content() << " with " << input.Content();
        auto lambda = ctx.FuseLambdas(node->Tail(), input.Tail());
        return ctx.NewCallable(node->Pos(),
            node->Content().starts_with("Narrow") ? TString("Ordered") += node->Content().substr(6U, 16U) : input.Content(),
            {input.HeadPtr(), std::move(lambda)});
    } else if (input.IsCallable("WideMap") && !node->IsCallable("NarrowFlatMap")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Fuse " << node->Content() << " with " << input.Content();
        auto lambda = ctx.FuseLambdas(node->Tail(), input.Tail());
        return ctx.ChangeChildren(*node, {input.HeadPtr(), std::move(lambda)});
    }  else if (input.IsCallable("WithContext")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << input.Content();
        return ctx.ChangeChild(input, 0U, ctx.ChangeChild(*node, 0U, input.HeadPtr()));
    } else if (const auto unused = UnusedArgs<1U>({&node->Tail()}); !unused.empty()) {
        if (input.IsCallable({"WideFilter", "WideSkipWhile", "WideSkipWhileInclusive", "WideTakeWhile", "WideTakeWhileInclusive"})) {
            if (const auto actualUnused = UnusedArgs<2U>({&node->Tail(), input.Child(1U)}); !actualUnused.empty()) {
                YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over " << input.Content() << " with " << actualUnused.size() << " unused fields.";
                auto children = input.ChildrenList();
                children.front() = MakeWideMapForDropUnused(std::move(children.front()), actualUnused, ctx);
                children[1U] = DropUnusedArgs(*children[1U], actualUnused, ctx);
                return ctx.Builder(node->Pos())
                    .Callable(node->Content())
                        .Add(0, ctx.ChangeChildren(input, std::move(children)))
                        .Add(1, DropUnusedArgs(node->Tail(), actualUnused, ctx))
                    .Seal().Build();
            }
        } else if (input.IsCallable("WideCombiner")) {
            YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over " << input.Content() << " with " << unused.size() << " unused fields.";
            return ctx.Builder(node->Pos())
                .Callable(node->Content())
                    .Add(0, ctx.ChangeChild(input, 5U, ctx.DeepCopyLambda(input.Tail(), DropUnused(GetLambdaBody(input.Tail()), unused))))
                    .Add(1, DropUnusedArgs(node->Tail(), unused, ctx))
                .Seal().Build();
        } else if (input.IsCallable("BlockCompress")) {
            YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over " << input.Content() << " with " << unused.size() << " unused fields.";
            const auto index = FromString<ui32>(input.Tail().Content());
            const auto delta = std::distance(unused.cbegin(), std::find_if(unused.cbegin(), unused.cend(), std::bind(std::less<ui32>(), index, std::placeholders::_1)));
            return ctx.Builder(node->Pos())
                .Callable(node->Content())
                    .Callable(0, input.Content())
                        .Add(0, MakeWideMapForDropUnused(input.HeadPtr(), unused, ctx))
                        .Atom(1, index - delta)
                    .Seal()
                    .Add(1, DropUnusedArgs(node->Tail(), unused, ctx))
                .Seal().Build();
        } else if (input.IsCallable("WideToBlocks")) {
            auto actualUnused = unused;
            if (actualUnused.back() + 1U == node->Tail().Head().ChildrenSize())
                actualUnused.pop_back();
            if (!actualUnused.empty()) {
                YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over " << input.Content() << " with " << actualUnused.size() << " unused fields.";
                return ctx.Builder(node->Pos())
                    .Callable(node->Content())
                        .Callable(0, input.Content())
                            .Add(0, MakeWideMapForDropUnused(input.HeadPtr(), actualUnused, ctx))
                        .Seal()
                        .Add(1, DropUnusedArgs(node->Tail(), actualUnused, ctx))
                    .Seal().Build();
            }
        } else if (input.IsCallable({"WideFromBlocks", "WideTakeBlocks", "WideSkipBlocks", "BlockExpandChunked"})) {
            YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over " << input.Content() << " with " << unused.size() << " unused fields.";
            return ctx.Builder(node->Pos())
                .Callable(node->Content())
                    .Add(0, ctx.ChangeChild(input, 0U, MakeWideMapForDropUnused(input.HeadPtr(), unused, ctx)))
                    .Add(1, DropUnusedArgs(node->Tail(), unused, ctx))
                .Seal().Build();
        } else if (input.IsCallable("WideCondense1")) {
            if (const auto& unusedState = UnusedState<2U>(*input.Child(1), input.Tail(), {&node->Tail(), input.Child(2)}); !unusedState.empty()) {
                YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over " << input.Content() << " with " << unusedState.size() << " unused fields.";
                return ctx.Builder(node->Pos())
                    .Callable(node->Content())
                        .Callable(0, input.Content())
                            .Add(0, input.HeadPtr())
                            .Add(1, ctx.DeepCopyLambda(*input.Child(1), DropUnused(GetLambdaBody(*input.Child(1)), unusedState)))
                            .Add(2, DropUnusedArgs(*input.Child(2), unusedState, ctx, input.Child(1)->Head().ChildrenSize()))
                            .Add(3, DropUnusedStateFromUpdate(input.Tail(), unusedState, ctx))
                        .Seal()
                        .Add(1, DropUnusedArgs(node->Tail(), unusedState, ctx))
                    .Seal().Build();
            }
        } else if (input.IsCallable("WideChain1Map")) {
            if (const auto& unusedState = UnusedState<1U>(*input.Child(1), input.Tail(), {&node->Tail()}); !unusedState.empty()) {
                YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over " << input.Content() << " with " << unusedState.size() << " unused fields.";
                return ctx.Builder(node->Pos())
                    .Callable(node->Content())
                        .Callable(0, input.Content())
                            .Add(0, input.HeadPtr())
                            .Add(1, ctx.DeepCopyLambda(*input.Child(1), DropUnused(GetLambdaBody(*input.Child(1)), unusedState)))
                            .Add(2, DropUnusedStateFromUpdate(input.Tail(), unusedState, ctx))
                        .Seal()
                        .Add(1, DropUnusedArgs(node->Tail(), unusedState, ctx))
                    .Seal().Build();
            }
        } else if (input.IsCallable({"GraceJoinCore", "GraceSelfJoinCore", "MapJoinCore"})) {
            YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over " << input.Content() << " with " << unused.size() << " unused fields.";
            auto children = input.ChildrenList();
            const bool self = input.IsCallable("GraceSelfJoinCore");
            DropUnusedRenames(children[5U], unused, ctx);
            DropUnusedRenames(children[self ? 4U : 6U], unused, ctx);
            return ctx.Builder(node->Pos())
                .Callable(node->Content())
                    .Add(0, ctx.ChangeChildren(input, std::move(children)))
                    .Add(1, DropUnusedArgs(node->Tail(), unused, ctx))
                .Seal().Build();
        } else if (node->IsCallable("WideMap") && input.IsCallable("ReplicateScalars")) {
            YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over " << input.Content();
            return SwapReplicateScalarsWithWideMap(node, ctx);
        }
    }

    return node;
}

TExprNode::TPtr OptimizeNarrowFlatMap(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto& lambda = node->Tail();
    const auto& body = lambda.Tail();

    if (body.IsCallable("If") && 1U == body.Tail().ChildrenSize() && body.Tail().IsCallable({"List", "Nothing", "EmptyFrom"})) {
        const auto width = lambda.Head().ChildrenSize();
        if (auto shared = FindSharedNode(body.ChildPtr(1), body.HeadPtr(),
            [&lambda] (const TExprNode::TPtr& node) {
                if (!node->IsCallable())
                    return false;

                if (node->GetTypeAnn() && node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Type) {
                    return false;
                }

                return node->GetDependencyScope()->second == &lambda;
            }); shared.first && shared.second) {
            YQL_CLOG(DEBUG, CorePeepHole) << "Extract " << shared.first->Content() << " from " << node->Content() << " with " << body.Content();

            auto argNodes = lambda.Head().ChildrenList();
            argNodes.emplace_back(ctx.NewArgument(lambda.Head().Pos(), TString("extracted") += ToString(argNodes.size())));

            auto args = ctx.NewArguments(lambda.Head().Pos(), std::move(argNodes));
            auto body = ctx.ReplaceNode(lambda.TailPtr(), *shared.first, args->TailPtr());

            auto children = node->ChildrenList();
            children.back() = ctx.NewLambda(lambda.Pos(), std::move(args), std::move(body));
            children.front() = ctx.Builder(node->Pos())
                .Callable("WideMap")
                    .Add(0, std::move(children.front()))
                    .Lambda(1)
                        .Params("items", width)
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            for (ui32 i = 0U; i < width; ++i) {
                                parent.Arg(i, "items", i);
                            }
                            return parent;
                        })
                        .ApplyPartial(width, lambda.HeadPtr(), std::move(shared.first))
                            .With("items")
                        .Seal()
                    .Seal()
                .Seal().Build();

            return ctx.ChangeChildren(*node, std::move(children));
        } else if (!shared.first) {
            YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " with " << body.Content() << " to WideFilter";
            const bool just = 2U == body.Child(1)->ChildrenSize() && body.Child(1)->IsCallable("List")
                || 1U == body.Child(1)->ChildrenSize() && body.Child(1)->IsCallable({"AsList", "Just"});
            return ctx.Builder(node->Pos())
                .Callable(just ? "NarrowMap" : "NarrowFlatMap")
                    .Callable(0, "WideFilter")
                        .Add(0, node->HeadPtr())
                        .Lambda(1)
                            .Params("items", width)
                            .ApplyPartial(lambda.HeadPtr(), body.HeadPtr()).With("items").Seal()
                        .Seal()
                    .Seal()
                    .Lambda(1)
                        .Params("items", width)
                        .ApplyPartial(lambda.HeadPtr(), just ? body.Child(1)->TailPtr() : body.ChildPtr(1)).With("items").Seal()
                    .Seal()
                .Seal().Build();
        }
    }

    return OptimizeWideMaps(node, ctx);
}

TExprNode::TPtr  OptimizeSqueezeToDict(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (const auto& input = node->Head(); input.IsCallable("NarrowMap")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Fuse " << node->Content() << " with " << input.Content();
        return ctx.NewCallable(node->Pos(), "NarrowSqueezeToDict", {
            input.HeadPtr(),
            ctx.FuseLambdas(*node->Child(1U), input.Tail()),
            ctx.FuseLambdas(*node->Child(2U), input.Tail()),
            node->TailPtr()
        });
    }
    return node;
}

TExprNode::TPtr ExpandConstraintsOf(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();

    TString json;
    TStringOutput out(json);
    NJson::TJsonWriter jsonWriter(&out, true);
    node->Head().GetConstraintSet().ToJson(jsonWriter);
    jsonWriter.Flush();

    return ctx.Builder(node->Pos())
        .Callable("Json")
            .Atom(0, json, TNodeFlags::MultilineContent)
        .Seal()
        .Build();
}

TExprNode::TPtr ExpandCostsOf(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand " << node->Content();

    TNodeMap<TString> visitedNodes;
    std::function<TString(const TExprNode::TPtr& node)> printNode = [&](const TExprNode::TPtr& node) -> TString {
        auto [it, emplaced] = visitedNodes.emplace(node.Get(), "");
        if (!emplaced) {
            return it->second;
        }

        std::vector<TString> chInfo;
        for (const auto& child : node->ChildrenList()) {
            auto res = printNode(child);
            if (res) {
                chInfo.emplace_back(std::move(res));
            }
        }

        auto stat = typesCtx.GetStats(node.Get());
        if (!chInfo.empty() || stat) {
            TStringOutput out(it->second);
            NJson::TJsonWriter jsonWriter(&out, false);
            jsonWriter.OpenMap();
            if (node->Content()) {
                jsonWriter.Write("Name", node->Content());
            }
            if (stat) {
                if (stat->Cost) {
                    jsonWriter.Write("Cost", stat->Cost);
                }
                jsonWriter.Write("Cols", stat->Ncols);
                jsonWriter.Write("Rows", stat->Nrows);
            }
            if (!chInfo.empty()) {
                jsonWriter.WriteKey("Children");
                jsonWriter.OpenArray();
                for (const auto& info : chInfo) {
                    jsonWriter.UnsafeWrite(info);
                }
                jsonWriter.CloseArray();
            }
            jsonWriter.CloseMap();
            jsonWriter.Flush();
        }

        return it->second;
    };

    TString json = printNode(node);

    if (!json) {
        json = "{}";
    }

    return ctx.Builder(node->Pos())
        .Callable("Json")
            .Atom(0, json, TNodeFlags::MultilineContent)
        .Seal()
        .Build();
}

TExprNode::TPtr OptimizeMapJoinCore(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (const auto& input = node->Head(); input.IsCallable("NarrowMap") && input.Tail().Tail().IsCallable("AsStruct")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << input.Content();
        auto map = NarrowToWide(input, ctx);
        const auto outStructType = GetSeqItemType(node->GetTypeAnn())->Cast<TStructExprType>();
        return ctx.Builder(input.Pos())
            .Callable(input.Content())
                .Add(0, MakeWideMapJoinCore<TStructExprType>(*node, std::move(map), ctx))
                .Lambda(1)
                    .Params("fields", outStructType->GetSize())
                    .Callable("AsStruct")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            ui32 i = 0U;
                            for (const auto& item : outStructType->GetItems()) {
                                parent.List(i)
                                    .Atom(0, item->GetName())
                                    .Arg(1, "fields", i)
                                .Seal();
                                ++i;
                            }
                            return parent;
                        })
                        .Seal()
                .Seal()
            .Seal().Build();
    }

    return node;
}

TExprNode::TPtr OptimizeGraceJoinCore(const TExprNode::TPtr& node, TExprContext& ctx) {
    const TCoGraceJoinCore grace(node);
    auto leftUnused = FillAllIndexes(GetSeqItemType(*grace.LeftInput().Ref().GetTypeAnn()));
    auto rightUnused = FillAllIndexes(GetSeqItemType(*grace.RightInput().Ref().GetTypeAnn()));
    RemoveUsedIndexes<false>(grace.LeftKeysColumns().Ref(), leftUnused);
    RemoveUsedIndexes<false>(grace.RightKeysColumns().Ref(), rightUnused);
    RemoveUsedIndexes<true>(grace.LeftRenames().Ref(), leftUnused);
    RemoveUsedIndexes<true>(grace.RightRenames().Ref(), rightUnused);
    if (!(leftUnused.empty() && rightUnused.empty())) {
        YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " with " << leftUnused.size() << " left and " << rightUnused.size() << " right unused columns.";

        auto children = node->ChildrenList();
        if (!leftUnused.empty()) {
            const std::vector<ui32> unused(leftUnused.cbegin(), leftUnused.cend());
            children[TCoGraceJoinCore::idx_LeftInput] = MakeWideMapForDropUnused(std::move(children[TCoGraceJoinCore::idx_LeftInput]), unused, ctx);
            UpdateInputIndexes<false>(children[TCoGraceJoinCore::idx_LeftKeysColumns], unused, ctx);
            UpdateInputIndexes<true>(children[TCoGraceJoinCore::idx_LeftRenames], unused, ctx);
        }
        if (!rightUnused.empty()) {
            const std::vector<ui32> unused(rightUnused.cbegin(), rightUnused.cend());
            children[TCoGraceJoinCore::idx_RightInput] = MakeWideMapForDropUnused(std::move(children[TCoGraceJoinCore::idx_RightInput]), unused, ctx);
            UpdateInputIndexes<false>(children[TCoGraceJoinCore::idx_RightKeysColumns], unused, ctx);
            UpdateInputIndexes<true>(children[TCoGraceJoinCore::idx_RightRenames], unused, ctx);
        }
        return ctx.ChangeChildren(*node, std::move(children));
    }

    return node;
}

TExprNode::TPtr OptimizeGraceSelfJoinCore(const TExprNode::TPtr& node, TExprContext& ctx) {
    const TCoGraceSelfJoinCore grace(node);
    auto unused = FillAllIndexes(GetSeqItemType(*grace.Input().Ref().GetTypeAnn()));
    RemoveUsedIndexes<false>(grace.LeftKeysColumns().Ref(), unused);
    RemoveUsedIndexes<false>(grace.RightKeysColumns().Ref(), unused);
    RemoveUsedIndexes<true>(grace.LeftRenames().Ref(), unused);
    RemoveUsedIndexes<true>(grace.RightRenames().Ref(), unused);
    if (!unused.empty()) {
        YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " with " << unused.size() << " unused columns.";

        auto children = node->ChildrenList();
        const std::vector<ui32> vector(unused.cbegin(), unused.cend());
        children[TCoGraceSelfJoinCore::idx_Input] = MakeWideMapForDropUnused(std::move(children[TCoGraceSelfJoinCore::idx_Input]), vector, ctx);
        UpdateInputIndexes<false>(children[TCoGraceSelfJoinCore::idx_LeftKeysColumns], vector, ctx);
        UpdateInputIndexes<false>(children[TCoGraceSelfJoinCore::idx_RightKeysColumns], vector, ctx);
        UpdateInputIndexes<true>(children[TCoGraceSelfJoinCore::idx_LeftRenames], vector, ctx);
        UpdateInputIndexes<true>(children[TCoGraceSelfJoinCore::idx_RightRenames], vector, ctx);
        return ctx.ChangeChildren(*node, std::move(children));
    }

    return node;
}

TExprNode::TPtr OptimizeCommonJoinCore(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (const auto& input = node->Head(); input.IsCallable("NarrowMap") && input.Tail().Tail().IsCallable("AsStruct")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Swap " << node->Content() << " with " << input.Content();
        auto map = NarrowToWide(input, ctx);
        auto wide = MakeWideCommonJoinCore<TStructExprType>(*node, std::move(map), ctx);
        return ctx.Builder(input.Pos())
            .Callable(input.Content())
                .Add(0, std::move(wide.first))
                .Lambda(1)
                    .Params("fields", wide.second.size())
                    .Callable("AsStruct")
                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                            ui32 i = 0U;
                            for (auto& item : wide.second) {
                                parent.List(i)
                                    .Add(0, std::move(item))
                                    .Arg(1, "fields", i)
                                .Seal();
                                ++i;
                            }
                            return parent;
                        })
                        .Seal()
                .Seal()
            .Seal().Build();
    }

    return node;
}

TExprNode::TPtr DoBuildTablePath(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_ENSURE(node->Head().IsCallable("String"),
        "BuildTablePath: expecting string literal as first argument, got: " << node->Head().Content());
    YQL_ENSURE(node->Tail().IsCallable("String"),
               "BuildTablePath: expecting string literal as second argument, got: " << node->Tail().Content());

    return ctx.Builder(node->Pos())
        .Callable("String")
            .Atom(0, BuildTablePath(node->Head().Head().Content(), node->Tail().Head().Content()))
        .Seal()
        .Build();
}

template <bool Equality>
TExprNode::TPtr ReduceBothArgs(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' with both Optionals.";
    return ECompareOptions::Optional == CanCompare<Equality>(node.Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType(), node.Tail().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType()) ?
        ctx.Builder(node.Pos())
            .Callable("IfPresent")
                .Add(0, node.HeadPtr())
                .Lambda(1)
                    .Param("lhs")
                    .Callable("IfPresent")
                        .Add(0, node.TailPtr())
                        .Lambda(1)
                            .Param("rhs")
                            .Callable(node.Content())
                                .Arg(0, "lhs")
                                .Arg(1, "rhs")
                            .Seal()
                        .Seal()
                        .Add(2, MakeBoolNothing(node.Pos(), ctx))
                    .Seal()
                .Seal()
                .Add(2, MakeBoolNothing(node.Pos(), ctx))
            .Seal()
        .Build():
        ctx.Builder(node.Pos())
            .Callable("IfPresent")
                .Add(0, node.HeadPtr())
                .Lambda(1)
                    .Param("lhs")
                    .Callable("IfPresent")
                        .Add(0, node.TailPtr())
                        .Lambda(1)
                            .Param("rhs")
                            .Callable("Just")
                                .Callable(0, node.Content())
                                    .Arg(0, "lhs")
                                    .Arg(1, "rhs")
                                .Seal()
                            .Seal()
                        .Seal()
                        .Add(2, MakeBoolNothing(node.Pos(), ctx))
                    .Seal()
                .Seal()
                .Add(2, MakeBoolNothing(node.Pos(), ctx))
            .Seal()
        .Build();
}

template <bool Equality, bool Equals, bool IsDistinct>
TExprNode::TPtr ReduceLeftArg(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' with left Optional.";
    auto stub = IsDistinct ? ctx.WrapByCallableIf(Equals, "Not", ctx.NewCallable(node.Pos(), "Exists", {node.TailPtr()})) : MakeBoolNothing(node.Pos(), ctx);
    return IsDistinct || ECompareOptions::Optional == CanCompare<Equality>(node.Head().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType(), node.Tail().GetTypeAnn()) ?
        ctx.Builder(node.Pos())
            .Callable("IfPresent")
                .Add(0, node.HeadPtr())
                .Lambda(1)
                    .Param("lhs")
                    .Callable(node.Content())
                        .Arg(0, "lhs")
                        .Add(1, node.TailPtr())
                    .Seal()
                .Seal()
                .Add(2, std::move(stub))
            .Seal()
        .Build():
        ctx.Builder(node.Pos())
            .Callable("IfPresent")
                .Add(0, node.HeadPtr())
                .Lambda(1)
                    .Param("lhs")
                    .Callable("Just")
                        .Callable(0, node.Content())
                            .Arg(0, "lhs")
                            .Add(1, node.TailPtr())
                        .Seal()
                    .Seal()
                .Seal()
                .Add(2, std::move(stub))
            .Seal()
        .Build();
}

template <bool Equality, bool Equals, bool IsDistinct>
TExprNode::TPtr ReduceRightArg(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' with right Optional.";
    auto stub = IsDistinct ? ctx.WrapByCallableIf(Equals, "Not", ctx.NewCallable(node.Pos(), "Exists", {node.HeadPtr()})) : MakeBoolNothing(node.Pos(), ctx);
    return IsDistinct || ECompareOptions::Optional == CanCompare<Equality>(node.Head().GetTypeAnn(), node.Tail().GetTypeAnn()->Cast<TOptionalExprType>()->GetItemType()) ?
        ctx.Builder(node.Pos())
            .Callable("IfPresent")
                .Add(0, node.TailPtr())
                .Lambda(1)
                    .Param("rhs")
                    .Callable(node.Content())
                        .Add(0, node.HeadPtr())
                        .Arg(1, "rhs")
                    .Seal()
                .Seal()
                .Add(2, std::move(stub))
            .Seal()
        .Build():
        ctx.Builder(node.Pos())
            .Callable("IfPresent")
                .Add(0, node.TailPtr())
                .Lambda(1)
                    .Param("rhs")
                    .Callable("Just")
                        .Callable(0, node.Content())
                            .Add(0, node.HeadPtr())
                            .Arg(1, "rhs")
                        .Seal()
                    .Seal()
                .Seal()
                .Add(2, std::move(stub))
            .Seal()
        .Build();
}

template <bool Equals>
TExprNode::TPtr AggrEqualOpt(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' with optional args.";
    auto stub = ctx.WrapByCallableIf(Equals, "Not", ctx.NewCallable(node.Pos(), "Exists", {node.TailPtr()}));
    return ctx.Builder(node.Pos())
        .Callable("IfPresent")
            .Add(0, node.HeadPtr())
            .Lambda(1)
                .Param("lhs")
                .Callable("IfPresent")
                    .Add(0, node.TailPtr())
                    .Lambda(1)
                        .Param("rhs")
                        .Callable(node.Content())
                            .Arg(0, "lhs")
                            .Arg(1, "rhs")
                        .Seal()
                    .Seal()
                    .Add(2, MakeBool<!Equals>(node.Pos(), ctx))
                .Seal()
            .Seal()
            .Add(2, std::move(stub))
        .Seal().Build();
}

template <bool Asc, bool Equals>
TExprNode::TPtr AggrComparePg(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Pg.";
    auto op = Asc ? ( Equals ? "<=" : "<") : ( Equals ? ">=" : ">");
    auto finalPart = (Equals == Asc) ? MakeBool<true>(node.Pos(), ctx) : 
        ctx.NewCallable(node.Pos(), "Exists", { node.TailPtr() });
    if (!Asc) {
        finalPart = ctx.NewCallable(node.Pos(), "Not", { finalPart });
    }

    return ctx.Builder(node.Pos())
        .Callable("If")
            .Callable(0, "Exists")
                .Add(0, node.HeadPtr())
            .Seal()
            .Callable(1, "Coalesce")
                .Callable(0, "FromPg")
                    .Callable(0, "PgOp")
                        .Atom(0, op)
                        .Add(1, node.HeadPtr())
                        .Add(2, node.TailPtr())
                    .Seal()
                .Seal()
                .Add(1, MakeBool<!Asc>(node.Pos(), ctx))
            .Seal()
            .Add(2, finalPart)
        .Seal()
        .Build();
}

template <bool Asc, bool Equals>
TExprNode::TPtr AggrCompareOpt(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' with optional args.";
    constexpr bool order = Asc == Equals;
    return ctx.Builder(node.Pos())
        .Callable("IfPresent")
            .Add(0, order ? node.HeadPtr() : node.TailPtr())
            .Lambda(1)
                .Param(order ? "lhs" : "rhs")
                .Callable("IfPresent")
                    .Add(0, order ? node.TailPtr() : node.HeadPtr())
                    .Lambda(1)
                        .Param(order ? "rhs" : "lhs")
                        .Callable(node.Content())
                            .Arg(0, "lhs")
                            .Arg(1, "rhs")
                        .Seal()
                    .Seal()
                    .Add(2, MakeBool<!Equals>(node.Pos(), ctx))
                .Seal()
            .Seal()
            .Add(2, MakeBool<Equals>(node.Pos(), ctx))
        .Seal().Build();
}

template <bool Equals>
TExprNode::TPtr SqlEqualTuples(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Tuples.";

    const auto lSize = node.Head().GetTypeAnn()->Cast<TTupleExprType>()->GetSize();
    const auto rSize = node.Tail().GetTypeAnn()->Cast<TTupleExprType>()->GetSize();
    const auto size = std::max(lSize, rSize);

    TExprNode::TListType compares;
    compares.reserve(size);

    TExprNode::TPtr nullNode = MakeNull(node.Pos(), ctx);
    for (ui32 i = 0U; i < size; ++i) {
        TExprNode::TPtr left = (i >= lSize) ? nullNode : ctx.Builder(node.Pos())
            .Callable("Nth")
                .Add(0, node.HeadPtr())
                .Atom(1, i)
                .Seal()
            .Build();

        TExprNode::TPtr right = (i >= rSize) ? nullNode : ctx.Builder(node.Pos())
            .Callable("Nth")
                .Add(0, node.TailPtr())
                .Atom(1, i)
            .Seal()
            .Build();

        compares.emplace_back(ctx.Builder(node.Pos())
            .Callable(node.Content())
                .Add(0, left)
                .Add(1, right)
            .Seal()
        .Build());
    }

    if (compares.empty()) {
        return MakeBool<Equals>(node.Pos(), ctx);
    }

    return ctx.NewCallable(node.Pos(), Equals ? "And" : "Or", std::move(compares));
}

template <bool Equals>
TExprNode::TPtr AggrEqualPg(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Pg.";
    auto ret = ctx.Builder(node.Pos())
        .Callable("If")
            .Callable(0, "Exists")
                .Add(0, node.ChildPtr(0))
            .Seal()
            .Callable(1, "Coalesce")
                .Callable(0,"FromPg")
                    .Callable(0, "PgOp")
                        .Atom(0, "<>")
                        .Add(1, node.ChildPtr(0))
                        .Add(2, node.ChildPtr(1))
                    .Seal()
                .Seal()
                .Callable(1, "Bool")
                    .Atom(0, "true")
                .Seal()
            .Seal()
            .Callable(2, "Exists")
                .Add(0, node.ChildPtr(1))
            .Seal()
        .Seal()
        .Build();

    if (Equals) {
        ret = ctx.NewCallable(node.Pos(), "Not", { ret });
    }

    return ret;
}

template <bool Equals>
TExprNode::TPtr AggrEqualTuples(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Tuples.";

    const auto size = node.Head().GetTypeAnn()->Cast<TTupleExprType>()->GetSize();
    if (!size)
        return MakeBool<Equals>(node.Pos(), ctx);

    TExprNode::TListType compares;
    compares.reserve(size);
    for (ui32 i = 0U; i < size; ++i) {
        compares.emplace_back(ctx.Builder(node.Pos())
            .Callable(node.Content())
                .Callable(0, "Nth")
                    .Add(0, node.HeadPtr())
                    .Atom(1, i)
                .Seal()
                .Callable(1, "Nth")
                    .Add(0, node.TailPtr())
                    .Atom(1, i)
                .Seal()
            .Seal()
        .Build());
    }

    return ctx.NewCallable(node.Pos(), Equals ? "And" : "Or", std::move(compares));
}

TExprNode::TPtr SqlCompareTuplesImpl(ui32 index, ui32 min, ui32 max, const TExprNode& node, TExprContext& ctx) {
    if (const auto next = index + 1U; next < min) {
        return ctx.Builder(node.Pos())
            .Callable("If")
                .Callable(0, "Coalesce")
                    .Callable(0, "==")
                        .Callable(0, "Nth")
                            .Add(0, node.HeadPtr())
                            .Atom(1, index)
                        .Seal()
                        .Callable(1, "Nth")
                            .Add(0, node.TailPtr())
                            .Atom(1, index)
                        .Seal()
                    .Seal()
                    .Add(1, MakeBool<false>(node.Pos(), ctx))
                .Seal()
                .Add(1, SqlCompareTuplesImpl(next, min, max, node, ctx))
                .Callable(2, node.Content().SubString(0U, 1U))
                    .Callable(0, "Nth")
                        .Add(0, node.HeadPtr())
                        .Atom(1, index)
                    .Seal()
                    .Callable(1, "Nth")
                        .Add(0, node.TailPtr())
                        .Atom(1, index)
                    .Seal()
                .Seal()
            .Seal()
        .Build();
    } else if (next < max) {
        return ctx.Builder(node.Pos())
            .Callable("Or")
                .Callable(0, node.Content().SubString(0U, 1U))
                    .Callable(0, "Nth")
                        .Add(0, node.HeadPtr())
                        .Atom(1, index)
                    .Seal()
                    .Callable(1, "Nth")
                        .Add(0, node.TailPtr())
                        .Atom(1, index)
                    .Seal()
                .Seal()
                .Add(1, MakeBoolNothing(node.Pos(), ctx))
            .Seal()
        .Build();
    } else {
        return ctx.Builder(node.Pos())
            .Callable(node.Content())
                .Callable(0, "Nth")
                    .Add(0, node.HeadPtr())
                    .Atom(1, index)
                .Seal()
                .Callable(1, "Nth")
                    .Add(0, node.TailPtr())
                    .Atom(1, index)
                .Seal()
            .Seal()
        .Build();
    }
}

TExprNode::TPtr SqlCompareTuples(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Tuples.";
    const auto lSize = node.Head().GetTypeAnn()->Cast<TTupleExprType>()->GetSize();
    const auto rSize = node.Tail().GetTypeAnn()->Cast<TTupleExprType>()->GetSize();
    const auto sizes = std::minmax(lSize, rSize);
    return SqlCompareTuplesImpl(0U, sizes.first, sizes.second, node, ctx);
}

template<bool Asc>
TExprNode::TPtr AggrCompareTuplesImpl(ui32 index, ui32 count, const TExprNode& node, TExprContext& ctx) {
    if (const auto next = index + 1U; next < count) {
        return ctx.Builder(node.Pos())
            .Callable("If")
                .Callable(0, "AggrEquals")
                    .Callable(0, "Nth")
                        .Add(0, node.HeadPtr())
                        .Atom(1, index)
                    .Seal()
                    .Callable(1, "Nth")
                        .Add(0, node.TailPtr())
                        .Atom(1, index)
                    .Seal()
                .Seal()
                .Add(1, AggrCompareTuplesImpl<Asc>(next, count, node, ctx))
                .Callable(2, Asc ? "AggrLess" : "AggrGreater")
                    .Callable(0, "Nth")
                        .Add(0, node.HeadPtr())
                        .Atom(1, index)
                    .Seal()
                    .Callable(1, "Nth")
                        .Add(0, node.TailPtr())
                        .Atom(1, index)
                    .Seal()
                .Seal()
            .Seal()
        .Build();
    } else {
        return ctx.Builder(node.Pos())
            .Callable(node.Content())
                .Callable(0, "Nth")
                    .Add(0, node.HeadPtr())
                    .Atom(1, index)
                .Seal()
                .Callable(1, "Nth")
                    .Add(0, node.TailPtr())
                    .Atom(1, index)
                .Seal()
            .Seal()
        .Build();
    }
}

template<bool Asc>
TExprNode::TPtr AggrCompareTuples(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Tuples.";
    return AggrCompareTuplesImpl<Asc>(0U, node.Head().GetTypeAnn()->Cast<TTupleExprType>()->GetSize(), node, ctx);
}

template <bool Equals>
TExprNode::TPtr SqlEqualStructs(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Structs.";

    const auto lType = node.Head().GetTypeAnn()->Cast<TStructExprType>();
    const auto rType = node.Tail().GetTypeAnn()->Cast<TStructExprType>();

    TExprNode::TListType compares;
    compares.reserve(std::max(lType->GetSize(), rType->GetSize()));

    TExprNode::TPtr nullNode = MakeNull(node.Pos(), ctx);
    for (const auto& item : lType->GetItems()) {
        const auto& name = item->GetName();
        TExprNode::TPtr right = !rType->FindItem(name) ? nullNode : ctx.Builder(node.Pos())
            .Callable("Member")
                .Add(0, node.TailPtr())
                .Atom(1, name)
            .Seal()
            .Build();

        compares.emplace_back(ctx.Builder(node.Pos())
            .Callable(node.Content())
                .Callable(0, "Member")
                    .Add(0, node.HeadPtr())
                    .Atom(1, name)
                .Seal()
                .Add(1, right)
            .Seal()
            .Build());
    }

    for (const auto& item : rType->GetItems()) {
        const auto& name = item->GetName();
        if (!lType->FindItem(name)) {
            compares.emplace_back(ctx.Builder(node.Pos())
                .Callable(node.Content())
                    .Add(0, nullNode)
                    .Callable(1, "Member")
                        .Add(0, node.TailPtr())
                        .Atom(1, name)
                    .Seal()
                .Seal()
                .Build());
        }
    }

    if (compares.empty()) {
        return MakeBool<Equals>(node.Pos(), ctx);
    }

    return ctx.NewCallable(node.Pos(), Equals ? "And" : "Or", std::move(compares));
}

template <bool Equals>
TExprNode::TPtr AggrEqualStructs(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Structs.";

    const auto type = node.Head().GetTypeAnn()->Cast<TStructExprType>();
    if (!type->GetSize())
        return MakeBool<Equals>(node.Pos(), ctx);

    TExprNode::TListType compares;
    compares.reserve(type->GetSize());

    for (const auto& item : type->GetItems()) {
        compares.emplace_back(ctx.Builder(node.Pos())
            .Callable(node.Content())
                .Callable(0, "Member")
                    .Add(0, node.HeadPtr())
                    .Atom(1, item->GetName())
                .Seal()
                .Callable(1, "Member")
                    .Add(0, node.TailPtr())
                    .Atom(1, item->GetName())
                .Seal()
            .Seal()
        .Build());
    }

    return ctx.NewCallable(node.Pos(), Equals ? "And" : "Or", std::move(compares));
}

template <bool Equals>
TExprNode::TPtr MakeStopper(const TPositionHandle pos, TExprContext& ctx) {
    return Equals ?
        ctx.Builder(pos)
            .Lambda()
                .Param("result")
                .Callable("Coalesce")
                    .Arg(0, "result")
                    .Add(1, MakeBool<Equals>(pos, ctx))
                .Seal()
            .Seal()
        .Build():
        ctx.Builder(pos)
            .Lambda()
                .Param("result")
                .Callable("Not")
                    .Callable(0, "Coalesce")
                        .Arg(0, "result")
                        .Add(1, MakeBool<Equals>(pos, ctx))
                    .Seal()
                .Seal()
            .Seal()
        .Build();
}

template <bool Equals>
TExprNode::TPtr SqlEqualLists(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Lists.";

    return ctx.Builder(node.Pos())
        .Callable(Equals ? "And" : "Or")
            .Callable(0, Equals ? "AggrEquals" : "AggrNotEquals")
                .Callable(0, "Length")
                    .Add(0, node.HeadPtr())
                .Seal()
                .Callable(1, "Length")
                    .Add(0, node.TailPtr())
                .Seal()
            .Seal()
            .Callable(1, "Coalesce")
                .Callable(0, "Last")
                    .Callable(0, "TakeWhileInclusive")
                        .Callable(0, "Chain1Map")
                            .Callable(0, "Zip")
                                .Add(node.ChildrenList())
                            .Seal()
                            .Lambda(1)
                                .Param("item")
                                .Callable(node.Content())
                                    .Callable(0, "Nth")
                                        .Arg(0, "item")
                                        .Atom(1, 0)
                                    .Seal()
                                    .Callable(1, "Nth")
                                        .Arg(0, "item")
                                        .Atom(1, 1)
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Lambda(2)
                                .Param("item")
                                .Param("state")
                                .Callable(Equals ? "And" : "Or")
                                    .Arg(0, "state")
                                    .Callable(1, node.Content())
                                        .Callable(0, "Nth")
                                            .Arg(0, "item")
                                            .Atom(1, 0)
                                        .Seal()
                                        .Callable(1, "Nth")
                                            .Arg(0, "item")
                                            .Atom(1, 1)
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                        .Add(1, MakeStopper<Equals>(node.Pos(), ctx))
                    .Seal()
                .Seal()
                .Add(1, MakeBool<Equals>(node.Pos(), ctx))
            .Seal()
        .Seal().Build();
}

template <bool Equals>
TExprNode::TPtr AggrEqualLists(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Lists.";
    auto all = ctx.Builder(node.Pos())
        .Callable("Exists")
            .Callable(0, "Head")
                .Callable(0, "SkipWhile")
                    .Callable(0, "Zip")
                        .Add(node.ChildrenList())
                    .Seal()
                    .Lambda(1)
                        .Param("item")
                        .Callable("AggrEquals")
                            .Callable(0, "Nth")
                                .Arg(0, "item")
                                .Atom(1, 0)
                            .Seal()
                            .Callable(1, "Nth")
                                .Arg(0, "item")
                                .Atom(1, 1)
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();

    return ctx.Builder(node.Pos())
        .Callable(Equals ? "And" : "Or")
            .Callable(0, node.Content())
                .Callable(0, "Length")
                    .Add(0, node.HeadPtr())
                .Seal()
                .Callable(1, "Length")
                    .Add(0, node.TailPtr())
                .Seal()
            .Seal()
            .Add(1, ctx.WrapByCallableIf(Equals, "Not", std::move(all)))
        .Seal().Build();
}

template <bool Asc, bool Equals>
TExprNode::TPtr SqlComparePg(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Pg.";
    return ctx.Builder(node.Pos())
        .Callable("FromPg")
            .Callable(0, "PgOp")
                .Atom(0, Asc ? (Equals ? "<=" : "<") : (Equals ? ">=" : ">"))
                .Add(1, node.ChildPtr(0))
                .Add(2, node.ChildPtr(1))
            .Seal()
        .Seal()
        .Build();
}

template <bool Asc, bool Equals>
TExprNode::TPtr SqlCompareLists(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Lists.";
    auto lenCompare = JustIf(ETypeAnnotationKind::Optional == node.GetTypeAnn()->GetKind(),
        ctx.Builder(node.Pos())
            .Callable(Asc ? (Equals ? "AggrLessOrEqual" : "AggrLess") : (Equals ? "AggrGreaterOrEqual" : "AggrGreater"))
                .Callable(0, "Length")
                    .Add(0, node.HeadPtr())
                .Seal()
                .Callable(1, "Length")
                    .Add(0, node.TailPtr())
                .Seal()
            .Seal()
        .Build(), ctx);

    return ctx.Builder(node.Pos())
        .Callable("IfPresent")
            .Callable(0, "Head")
                .Callable(0, "SkipWhile")
                    .Callable(0, "Zip")
                        .Add(node.ChildrenList())
                    .Seal()
                    .Lambda(1)
                        .Param("item")
                        .Callable("Coalesce")
                            .Callable(0, "==")
                                .Callable(0, "Nth")
                                    .Arg(0, "item")
                                    .Atom(1, 0)
                                .Seal()
                                .Callable(1, "Nth")
                                    .Arg(0, "item")
                                    .Atom(1, 1)
                                .Seal()
                            .Seal()
                            .Add(1, MakeBool<false>(node.Pos(), ctx))
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(1)
                .Param("item")
                .Callable(node.Content().SubString(0U, 1U))
                    .Callable(0, "Nth")
                        .Arg(0, "item")
                        .Atom(1, 0)
                    .Seal()
                    .Callable(1, "Nth")
                        .Arg(0, "item")
                        .Atom(1, 1)
                    .Seal()
                .Seal()
            .Seal()
            .Add(2, std::move(lenCompare))
        .Seal().Build();
}

template <bool Equals>
TExprNode::TPtr AggrCompareLists(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Lists.";
    return ctx.Builder(node.Pos())
        .Callable("IfPresent")
            .Callable(0, "Head")
                .Callable(0, "SkipWhile")
                    .Callable(0, "ZipAll")
                        .Add(node.ChildrenList())
                    .Seal()
                    .Lambda(1)
                        .Param("item")
                        .Callable("AggrEquals")
                            .Callable(0, "Nth")
                                .Arg(0, "item")
                                .Atom(1, 0)
                            .Seal()
                            .Callable(1, "Nth")
                                .Arg(0, "item")
                                .Atom(1, 1)
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(1)
                .Param("item")
                .Callable(node.Content())
                    .Callable(0, "Nth")
                        .Arg(0, "item")
                        .Atom(1, 0)
                    .Seal()
                    .Callable(1, "Nth")
                        .Arg(0, "item")
                        .Atom(1, 1)
                    .Seal()
                .Seal()
            .Seal()
            .Add(2, MakeBool<Equals>(node.Pos(), ctx))
        .Seal().Build();
}

template <bool Equals>
TExprNode::TPtr CheckHasItems(const TExprNode::TPtr& node, TExprContext& ctx) {
    return ctx.WrapByCallableIf(Equals, "Not", ctx.NewCallable(node->Pos(), "HasItems", {node}));
}

template <bool Equals, bool IsDistinct>
TExprNode::TPtr SqlEqualPg(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Pg.";
    if constexpr (IsDistinct) {
        return ctx.Builder(node.Pos())
            .Callable("IfPresent")
                .Callable(0, "FromPg")
                    .Callable(0, "PgOp")
                        .Atom(0, Equals ? "=" : "<>")
                        .Add(1, node.ChildPtr(0))
                        .Add(2, node.ChildPtr(1))
                    .Seal()
                .Seal()
                .Lambda(1)
                    .Param("unpacked")
                    .Arg("unpacked")
                .Seal()
                .Callable(2, Equals ? "==" : "!=")
                    .Callable(0, "Exists")
                        .Add(0, node.ChildPtr(0))
                    .Seal()
                    .Callable(1, "Exists")
                        .Add(0, node.ChildPtr(1))
                    .Seal()
                .Seal()
            .Build();
    } else {
        return ctx.Builder(node.Pos())
            .Callable("FromPg")
                .Callable(0, "PgOp")
                    .Atom(0, Equals ? "=" : "<>")
                    .Add(1, node.ChildPtr(0))
                    .Add(2, node.ChildPtr(1))
                .Seal()
            .Seal()
            .Build();
    }
}

template <bool Equals, bool IsDistinct>
TExprNode::TPtr SqlEqualDicts(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Dicts.";

    const auto can = CanCompare<true>(node.Head().GetTypeAnn()->Cast<TDictExprType>()->GetPayloadType(), node.Tail().GetTypeAnn()->Cast<TDictExprType>()->GetPayloadType());
    const auto stub = JustIf(ECompareOptions::Optional == can && !IsDistinct, MakeBool<!Equals>(node.Pos(), ctx), ctx);
    return ctx.Builder(node.Pos())
        .Callable(Equals ? "And" : "Or")
            .Callable(0, Equals ? "AggrEquals" : "AggrNotEquals")
                .Callable(0, "Length")
                    .Add(0, node.HeadPtr())
                .Seal()
                .Callable(1, "Length")
                    .Add(0, node.TailPtr())
                .Seal()
            .Seal()
            .Callable(1, "Coalesce")
                .Callable(0, "Last")
                    .Callable(0, "TakeWhileInclusive")
                        .Callable(0, "Chain1Map")
                            .Callable(0, "DictItems")
                                .Add(0, node.HeadPtr())
                            .Seal()
                            .Lambda(1)
                                .Param("item")
                                .Callable("IfPresent")
                                    .Callable(0, "Lookup")
                                        .Add(0, node.TailPtr())
                                        .Callable(1, "Nth")
                                            .Arg(0, "item")
                                            .Atom(1, 0)
                                        .Seal()
                                    .Seal()
                                    .Lambda(1)
                                        .Param("payload")
                                        .Callable(node.Content())
                                            .Callable(0, "Nth")
                                                .Arg(0, "item")
                                                .Atom(1, 1)
                                            .Seal()
                                            .Arg(1, "payload")
                                        .Seal()
                                    .Seal()
                                    .Add(2, stub)
                                .Seal()
                            .Seal()
                            .Lambda(2)
                                .Param("item")
                                .Param("state")
                                .Callable(Equals ? "And" : "Or")
                                    .Arg(0, "state")
                                    .Callable(1, "IfPresent")
                                        .Callable(0, "Lookup")
                                            .Add(0, node.TailPtr())
                                            .Callable(1, "Nth")
                                                .Arg(0, "item")
                                                .Atom(1, 0)
                                            .Seal()
                                        .Seal()
                                        .Lambda(1)
                                            .Param("payload")
                                            .Callable(node.Content())
                                                .Callable(0, "Nth")
                                                    .Arg(0, "item")
                                                    .Atom(1, 1)
                                                .Seal()
                                                .Arg(1, "payload")
                                            .Seal()
                                        .Seal()
                                        .Add(2, stub)
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                        .Add(1, MakeStopper<Equals>(node.Pos(), ctx))
                    .Seal()
                .Seal()
                .Add(1, MakeBool<Equals>(node.Pos(), ctx))
            .Seal()
        .Seal().Build();
}

template <bool Equals>
TExprNode::TPtr AggrEqualDicts(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Dicts.";
    auto all = ctx.Builder(node.Pos())
        .Callable("Exists")
            .Callable(0, "Head")
                .Callable(0, "SkipWhile")
                    .Callable(0, "DictItems")
                        .Add(0, node.HeadPtr())
                    .Seal()
                    .Lambda(1)
                        .Param("item")
                        .Callable("IfPresent")
                            .Callable(0, "Lookup")
                                .Add(0, node.TailPtr())
                                .Callable(1, "Nth")
                                    .Arg(0, "item")
                                    .Atom(1, 0)
                                .Seal()
                            .Seal()
                            .Lambda(1)
                                .Param("payload")
                                .Callable("AggrEquals")
                                    .Callable(0, "Nth")
                                        .Arg(0, "item")
                                        .Atom(1, 1)
                                    .Seal()
                                    .Arg(1, "payload")
                                .Seal()
                            .Seal()
                            .Callable(2, "Bool")
                                .Atom(0, "false", TNodeFlags::Default)
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();

    return ctx.Builder(node.Pos())
        .Callable(Equals ? "And" : "Or")
            .Callable(0, node.Content())
                .Callable(0, "Length")
                    .Add(0, node.HeadPtr())
                .Seal()
                .Callable(1, "Length")
                    .Add(0, node.TailPtr())
                .Seal()
            .Seal()
            .Add(1, ctx.WrapByCallableIf(Equals, "Not", std::move(all)))
        .Seal().Build();
}

template <bool Equality, bool Order, bool IsDistinct>
TExprNode::TPtr SqlCompareVariants(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Variants.";

    auto lhs = node.HeadPtr();
    auto rhs = node.TailPtr();

    const auto lType = lhs->GetTypeAnn()->Cast<TVariantExprType>()->GetUnderlyingType();
    const auto rType = rhs->GetTypeAnn()->Cast<TVariantExprType>()->GetUnderlyingType();

    std::vector<std::pair<TExprNode::TPtr, std::optional<bool>>> variants;
    bool swap, byStruct;
    switch (rType->GetKind()) {
        case ETypeAnnotationKind::Tuple: {
            byStruct = false;
            const auto lTuple = lType->Cast<TTupleExprType>();
            const auto rTuple = rType->Cast<TTupleExprType>();

            if (swap = lTuple->GetSize() > rTuple->GetSize())
                std::swap(lhs, rhs);

            const auto tupleType = lTuple->GetSize() <= rTuple->GetSize() ? lTuple : rTuple;
            variants.reserve(tupleType->GetSize());

            const auto& lItems = lTuple->GetItems();
            const auto& rItems = rTuple->GetItems();
            for (ui32 i = 0U; i < tupleType->GetSize(); ++i) {
                variants.emplace_back(ctx.NewAtom(node.Pos(), i),
                    !IsDistinct && ECompareOptions::Optional == CanCompare<true>(lItems[i], rItems[i]));
            }

            break;
        }
        case ETypeAnnotationKind::Struct: {
            byStruct = true;
            const auto lStruct = lType->Cast<TStructExprType>();
            const auto rStruct = rType->Cast<TStructExprType>();

            if (swap = lStruct->GetSize() > rStruct->GetSize())
                std::swap(lhs, rhs);

            const auto structType = lStruct->GetSize() <= rStruct->GetSize() ? lStruct : rStruct;
            variants.reserve(structType->GetSize());
            const auto oppositeType = lStruct->GetSize() > rStruct->GetSize() ? lStruct : rStruct;

            const auto& items = oppositeType->GetItems();
            for (const auto& item : structType->GetItems()) {
                variants.emplace_back(ctx.NewAtom(node.Pos(), item->GetName()), std::nullopt);
                if (const auto idx = oppositeType->FindItem(item->GetName())) {
                    variants.back().second.emplace(!IsDistinct && ECompareOptions::Optional == CanCompare<true>(item->GetItemType(), items[*idx]->GetItemType()));
                }
            }

            break;
        }
        default:
            return {};
    }

    TExprNode::TListType children;
    children.reserve((variants.size() << 1U) + 1U);
    children.emplace_back(std::move(lhs));
    const bool optResult = ETypeAnnotationKind::Optional == node.GetTypeAnn()->GetKind();
    for (auto& variant : variants) {
        children.emplace_back(variant.first);
        auto stub = Equality ? MakeBool<Order>(node.Pos(), ctx) :
            ctx.Builder(node.Pos())
                .Callable(Order ^ swap ? "AggrLess" : "AggrGreater")
                    .Callable(0, byStruct ? "Utf8" : "Uint32")
                        .Add(0, variant.first)
                    .Seal()
                    .Callable(1, "Way")
                        .Add(0, rhs)
                    .Seal()
                .Seal()
            .Build();
        if (const auto opt = variant.second) {
            children.emplace_back(optResult && !*opt ?
                ctx.Builder(node.Pos())
                    .Lambda()
                        .Param("lhs")
                        .Callable("Just")
                            .Callable(0, "IfPresent")
                                .Callable(0, "Guess")
                                    .Add(0, rhs)
                                    .Add(1, std::move(variant.first))
                                .Seal()
                                .Lambda(1)
                                    .Param("rhs")
                                    .Callable(node.Content())
                                        .Arg(0, swap ? "rhs" : "lhs")
                                        .Arg(1, swap ? "lhs" : "rhs")
                                    .Seal()
                                .Seal()
                                .Add(2, JustIf(*opt, std::move(stub), ctx))
                            .Seal()
                        .Seal()
                    .Seal()
                .Build():
                ctx.Builder(node.Pos())
                    .Lambda()
                        .Param("lhs")
                        .Callable("IfPresent")
                            .Callable(0, "Guess")
                                .Add(0, rhs)
                                .Add(1, std::move(variant.first))
                            .Seal()
                            .Lambda(1)
                                .Param("rhs")
                                .Callable(node.Content())
                                    .Arg(0, swap ? "rhs" : "lhs")
                                    .Arg(1, swap ? "lhs" : "rhs")
                                .Seal()
                            .Seal()
                            .Add(2, JustIf(*opt, std::move(stub), ctx))
                        .Seal()
                    .Seal()
                .Build()
            );
        } else {
            children.emplace_back(ctx.NewLambda(node.Pos(), ctx.NewArguments(node.Pos(), {ctx.NewArgument(node.Pos(), "unused")}), JustIf(optResult, std::move(stub), ctx)));
        }
    }

    return ctx.NewCallable(node.Pos(), "Visit", std::move(children));
}

template <bool Equality, bool Order>
TExprNode::TPtr AggrCompareVariants(const TExprNode& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Expand '" << node.Content() << "' over Variants.";

    const auto type = node.Head().GetTypeAnn()->Cast<TVariantExprType>()->GetUnderlyingType();

    TExprNode::TListType variants;
    bool byStruct;
    switch (type->GetKind()) {
        case ETypeAnnotationKind::Tuple: {
            byStruct = false;
            const auto size = type->Cast<TTupleExprType>()->GetSize();
            variants.reserve(size);
            for (ui32 i = 0U; i < size; ++i) {
                variants.emplace_back(ctx.NewAtom(node.Pos(), i));
            }
            break;
        }
        case ETypeAnnotationKind::Struct: {
            byStruct = true;
            const auto structType = type->Cast<TStructExprType>();
            variants.reserve(structType->GetSize());
            for (const auto& item : structType->GetItems()) {
                variants.emplace_back(ctx.NewAtom(node.Pos(), item->GetName()));
            }
            break;
        }
        default:
            return {};
    }

    TExprNode::TListType children;
    children.reserve((variants.size() << 1U) + 1U);
    children.emplace_back(node.HeadPtr());
    for (auto& variant : variants) {
        children.emplace_back(variant);
        auto stub = Equality ? MakeBool<Order>(node.Pos(), ctx) :
            ctx.Builder(node.Pos())
                .Callable(Order ? "AggrLess" : "AggrGreater")
                    .Callable(0, byStruct ? "Utf8" : "Uint32")
                        .Add(0, variant)
                    .Seal()
                    .Callable(1, "Way")
                        .Add(0, node.TailPtr())
                    .Seal()
                .Seal()
            .Build();
        children.emplace_back(
            ctx.Builder(node.Pos())
                .Lambda()
                    .Param("lhs")
                    .Callable("IfPresent")
                        .Callable(0, "Guess")
                            .Add(0, node.TailPtr())
                            .Add(1, std::move(variant))
                        .Seal()
                        .Lambda(1)
                            .Param("rhs")
                            .Callable(node.Content())
                                .Arg(0, "lhs")
                                .Arg(1, "rhs")
                            .Seal()
                        .Seal()
                        .Add(2, std::move(stub))
                    .Seal()
                .Seal()
            .Build()
        );
    }

    return ctx.NewCallable(node.Pos(), "Visit", std::move(children));
}


TExprNode::TPtr CompareTagged(const TExprNode& node, TExprContext& ctx) {
    auto children = node.ChildrenList();
    std::for_each(children.begin(), children.end(), [&](TExprNode::TPtr& child) {
        if (const auto type = child->GetTypeAnn(); ETypeAnnotationKind::Tagged == type->GetKind()) {
            const auto pos = child->Pos();
            child = ctx.NewCallable(pos, "Untag", {std::move(child), ctx.NewAtom(pos, type->Cast<TTaggedExprType>()->GetTag())});
        }
    });
    return ctx.ChangeChildren(node, std::move(children));
}

template <bool Equals, bool IsDistinct>
TExprNode::TPtr ExpandSqlEqual(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto lType = node->Head().GetTypeAnn();
    const auto rType = node->Tail().GetTypeAnn();
    if constexpr (!IsDistinct) {
        if (IsDataOrOptionalOfData(lType) && IsDataOrOptionalOfData(rType)) {
            // this case is supported by runtime
            return node;
        }
    }
    if (const auto lKind = lType->GetKind(), rKind = rType->GetKind(); lKind == rKind) {
        switch (rKind) {
            case ETypeAnnotationKind::Void:
            case ETypeAnnotationKind::EmptyList:
            case ETypeAnnotationKind::EmptyDict:
                return MakeBool<Equals>(node->Pos(), ctx);
            case ETypeAnnotationKind::Null:
                return IsDistinct ? MakeBool<Equals>(node->Pos(), ctx) : MakeBoolNothing(node->Pos(), ctx);
            case ETypeAnnotationKind::Tuple:
                return SqlEqualTuples<Equals>(*node, ctx);
            case ETypeAnnotationKind::Struct:
                return SqlEqualStructs<Equals>(*node, ctx);
            case ETypeAnnotationKind::List:
                return SqlEqualLists<Equals>(*node, ctx);
            case ETypeAnnotationKind::Dict:
                return SqlEqualDicts<Equals, IsDistinct>(*node, ctx);
            case ETypeAnnotationKind::Pg:
                return SqlEqualPg<Equals, IsDistinct>(*node, ctx);
            case ETypeAnnotationKind::Variant:
                return SqlCompareVariants<true, !Equals, IsDistinct>(*node, ctx);
            case ETypeAnnotationKind::Tagged:
                return CompareTagged(*node, ctx);
            case ETypeAnnotationKind::Optional:
                if constexpr (IsDistinct)
                    return AggrEqualOpt<Equals>(*node, ctx);
                else
                    return ReduceBothArgs<true>(*node, ctx);
            default:
                break;
        }
    } else if (ETypeAnnotationKind::List == lKind && ETypeAnnotationKind::EmptyList == rKind
            || ETypeAnnotationKind::Dict == lKind && ETypeAnnotationKind::EmptyDict == rKind) {
        return JustIf(ETypeAnnotationKind::Optional == node->GetTypeAnn()->GetKind(), CheckHasItems<Equals>(node->HeadPtr(), ctx), ctx);
    } else if (ETypeAnnotationKind::EmptyList == lKind && ETypeAnnotationKind::List == rKind
            || ETypeAnnotationKind::EmptyDict == lKind && ETypeAnnotationKind::Dict == rKind) {
        return JustIf(ETypeAnnotationKind::Optional == node->GetTypeAnn()->GetKind(), CheckHasItems<Equals>(node->TailPtr(), ctx), ctx);
    } else if (ETypeAnnotationKind::Optional == lKind) {
        return ReduceLeftArg<true, Equals, IsDistinct>(*node, ctx);
    } else if (ETypeAnnotationKind::Optional == rKind) {
        return ReduceRightArg<true, Equals, IsDistinct>(*node, ctx);
    }

    return node;
}

template<bool Asc, bool Equals>
TExprNode::TPtr ExpandSqlCompare(const TExprNode::TPtr& node, TExprContext& ctx) {
    const auto lType = node->Head().GetTypeAnn();
    const auto rType = node->Tail().GetTypeAnn();
    if (IsDataOrOptionalOfData(lType) && IsDataOrOptionalOfData(rType)) {
        // this case is supported by runtime
        return node;
    }
    if (const auto lKind = lType->GetKind(), rKind = rType->GetKind(); lKind == rKind) {
        switch (rKind) {
            case ETypeAnnotationKind::Void:
            case ETypeAnnotationKind::EmptyList:
            case ETypeAnnotationKind::EmptyDict:
                return MakeBool<Equals>(node->Pos(), ctx);
            case ETypeAnnotationKind::Null:
                return MakeBoolNothing(node->Pos(), ctx);
            case ETypeAnnotationKind::Tuple:
                return SqlCompareTuples(*node, ctx);
            case ETypeAnnotationKind::List:
                return SqlCompareLists<Asc, Equals>(*node, ctx);
            case ETypeAnnotationKind::Variant:
                return SqlCompareVariants<false, Asc, false>(*node, ctx);
            case ETypeAnnotationKind::Tagged:
                return CompareTagged(*node, ctx);
            case ETypeAnnotationKind::Optional:
                return ReduceBothArgs<false>(*node, ctx);
            case ETypeAnnotationKind::Pg:
                return SqlComparePg<Asc, Equals>(*node, ctx);
            default:
                break;
        }
    } else if (ETypeAnnotationKind::List == lKind && ETypeAnnotationKind::EmptyList == rKind) {
        return JustIf(ETypeAnnotationKind::Optional == node->GetTypeAnn()->GetKind(), Asc != Equals ? MakeBool<!Asc>(node->Pos(), ctx) : CheckHasItems<Asc>(node->HeadPtr(), ctx), ctx);
    } else if (ETypeAnnotationKind::EmptyList == lKind && ETypeAnnotationKind::List == rKind) {
        return JustIf(ETypeAnnotationKind::Optional == node->GetTypeAnn()->GetKind(), Asc == Equals ? MakeBool<Asc>(node->Pos(), ctx) : CheckHasItems<!Asc>(node->TailPtr(), ctx), ctx);
    } else if (ETypeAnnotationKind::Optional == lKind) {
        return ReduceLeftArg<false, Equals, false>(*node, ctx);
    } else if (ETypeAnnotationKind::Optional == rKind) {
        return ReduceRightArg<false, Equals, false>(*node, ctx);
    }

    return node;
}

template <bool Equals>
TExprNode::TPtr ExpandAggrEqual(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (&node->Head() == &node->Tail()) {
        YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over same arguments.";
        return MakeBool<Equals>(node->Pos(), ctx);
    }

    const auto type = node->Head().GetTypeAnn();
    YQL_ENSURE(IsSameAnnotation(*type, *node->Tail().GetTypeAnn()), "Expected same type.");
    switch (type->GetKind()) {
        case ETypeAnnotationKind::Void:
        case ETypeAnnotationKind::Null:
        case ETypeAnnotationKind::EmptyList:
        case ETypeAnnotationKind::EmptyDict:
            return MakeBool<Equals>(node->Pos(), ctx);
        case ETypeAnnotationKind::Tuple:
            return AggrEqualTuples<Equals>(*node, ctx);
        case ETypeAnnotationKind::Struct:
            return AggrEqualStructs<Equals>(*node, ctx);
        case ETypeAnnotationKind::List:
            return AggrEqualLists<Equals>(*node, ctx);
        case ETypeAnnotationKind::Dict:
            return AggrEqualDicts<Equals>(*node, ctx);
        case ETypeAnnotationKind::Variant:
            return AggrCompareVariants<true, !Equals>(*node, ctx);
        case ETypeAnnotationKind::Tagged:
            return CompareTagged(*node, ctx);
        case ETypeAnnotationKind::Pg:
            return AggrEqualPg<Equals>(*node, ctx);
        case ETypeAnnotationKind::Optional:
            if (type->Cast<TOptionalExprType>()->GetItemType()->GetKind() != ETypeAnnotationKind::Data)
                return AggrEqualOpt<Equals>(*node, ctx);
            [[fallthrough]];
        default:
            break;
    }
    return node;
}

template<bool Asc, bool Equals>
TExprNode::TPtr ExpandAggrCompare(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (&node->Head() == &node->Tail()) {
        YQL_CLOG(DEBUG, CorePeepHole) << node->Content() << " over same arguments.";
        return MakeBool<Equals>(node->Pos(), ctx);
    }

    const auto type = node->Head().GetTypeAnn();
    YQL_ENSURE(IsSameAnnotation(*type, *node->Tail().GetTypeAnn()), "Expected same type.");
    switch (type->GetKind()) {
        case ETypeAnnotationKind::Void:
        case ETypeAnnotationKind::Null:
        case ETypeAnnotationKind::EmptyList:
        case ETypeAnnotationKind::EmptyDict:
            return MakeBool<Equals>(node->Pos(), ctx);
        case ETypeAnnotationKind::Tuple:
            return AggrCompareTuples<Asc>(*node, ctx);
        case ETypeAnnotationKind::List:
            return AggrCompareLists<Equals>(*node, ctx);
        case ETypeAnnotationKind::Variant:
            return AggrCompareVariants<false, Asc>(*node, ctx);
        case ETypeAnnotationKind::Tagged:
            return CompareTagged(*node, ctx);
        case ETypeAnnotationKind::Pg:
            return AggrComparePg<Asc, Equals>(*node, ctx);
        case ETypeAnnotationKind::Optional:
            if (type->Cast<TOptionalExprType>()->GetItemType()->GetKind() != ETypeAnnotationKind::Data)
                return AggrCompareOpt<Asc, Equals>(*node, ctx);
            [[fallthrough]];
        default:
            break;
    }

    return node;
}

TExprNode::TPtr DropToFlowDeps(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (node->ChildrenSize() == 1) {
        return node;
    }
    YQL_CLOG(DEBUG, CorePeepHole) << __FUNCTION__;
    auto children = node->ChildrenList();
    children.resize(1);
    return ctx.ChangeChildren(*node, std::move(children));
}

TExprNode::TPtr OptimizeToFlow(const TExprNode::TPtr& node, TExprContext&) {
    if (node->ChildrenSize() == 1 && node->Head().IsCallable("FromFlow")) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Drop ToFlow over FromFlow";
        return node->Head().HeadPtr();
    }
    return node;
}

TExprNode::TPtr BuildCheckedBinaryOpOverDecimal(TPositionHandle pos, TStringBuf op, const TExprNode::TPtr& lhs, const TExprNode::TPtr& rhs, const TTypeAnnotationNode& resultType, TExprContext& ctx) {
    auto typeNode = ExpandType(pos, resultType, ctx);
    return ctx.Builder(pos)
        .Callable("SafeCast")
            .Callable(0, op)
                .Callable(0, "SafeCast")
                    .Callable(0, "SafeCast")
                        .Add(0, lhs)
                        .Add(1, typeNode)
                    .Seal()
                    .Callable(1, "DataType")
                        .Atom(0, "Decimal")
                        .Atom(1, 20)
                        .Atom(2, 0)
                    .Seal()
                .Seal()
                .Callable(1, "SafeCast")
                    .Callable(0, "SafeCast")
                        .Add(0, rhs)
                        .Add(1, typeNode)
                    .Seal()
                    .Callable(1, "DataType")
                        .Atom(0, "Decimal")
                        .Atom(1, 20)
                        .Atom(2, 0)
                    .Seal()
                .Seal()
            .Seal()
            .Add(1, typeNode)
        .Seal()
        .Build();
}

TExprNode::TPtr BuildCheckedBinaryOpOverSafeCast(TPositionHandle pos, TStringBuf op, const TExprNode::TPtr& lhs, const TExprNode::TPtr& rhs, const TTypeAnnotationNode& resultType, TExprContext& ctx) {
    auto typeNode = ExpandType(pos, resultType, ctx);
    return ctx.Builder(pos)
        .Callable(op)
            .Callable(0, "SafeCast")
                .Add(0, lhs)
                .Add(1, typeNode)
            .Seal()
            .Callable(1, "SafeCast")
                .Add(0, rhs)
                .Add(1, typeNode)
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr ExpandCheckedAdd(const TExprNode::TPtr& node, TExprContext& ctx) {
    return BuildCheckedBinaryOpOverDecimal(node->Pos(), "+", node->ChildPtr(0), node->ChildPtr(1), *node->GetTypeAnn(), ctx);
}

TExprNode::TPtr ExpandCheckedSub(const TExprNode::TPtr& node, TExprContext& ctx) {
    return BuildCheckedBinaryOpOverDecimal(node->Pos(), "-", node->ChildPtr(0), node->ChildPtr(1), *node->GetTypeAnn(), ctx);
}

TExprNode::TPtr ExpandCheckedMul(const TExprNode::TPtr& node, TExprContext& ctx) {
    return BuildCheckedBinaryOpOverDecimal(node->Pos(), "*", node->ChildPtr(0), node->ChildPtr(1), *node->GetTypeAnn(), ctx);
}

TExprNode::TPtr ExpandCheckedDiv(const TExprNode::TPtr& node, TExprContext& ctx) {
    return BuildCheckedBinaryOpOverSafeCast(node->Pos(), "/", node->ChildPtr(0), node->ChildPtr(1), *node->GetTypeAnn(), ctx);
}

TExprNode::TPtr ExpandCheckedMod(const TExprNode::TPtr& node, TExprContext& ctx) {
    return BuildCheckedBinaryOpOverSafeCast(node->Pos(), "%", node->ChildPtr(0), node->ChildPtr(1), *node->GetTypeAnn(), ctx);
}

TExprNode::TPtr ExpandCheckedMinus(const TExprNode::TPtr& node, TExprContext& ctx) {
    return ctx.Builder(node->Pos())
        .Callable("SafeCast")
            .Callable(0, "Minus")
                .Callable(0, "SafeCast")
                    .Add(0, node->HeadPtr())
                    .Callable(1, "DataType")
                        .Atom(0, "Decimal")
                        .Atom(1, 20)
                        .Atom(2, 0)
                    .Seal()
                .Seal()
            .Seal()
            .Callable(1, "TypeOf")
                .Add(0, node->HeadPtr())
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr DropAssume(const TExprNode::TPtr& node, TExprContext&) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Drop " << node->Content();
    return node->HeadPtr();
}

TExprNode::TPtr DropEmptyFrom(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_CLOG(DEBUG, CorePeepHole) << "Drop " << node->Content();
    return ctx.NewCallable(node->Pos(), GetEmptyCollectionName(node->GetTypeAnn()), {ExpandType(node->Pos(), *node->GetTypeAnn(), ctx)});
}

TExprNode::TPtr OptimizeCoalesce(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (const auto& input = node->Head(); input.IsCallable("If") && input.ChildrenSize() == 3U &&
        (input.Child(1U)->IsComplete() || input.Child(2U)->IsComplete())) {
        YQL_CLOG(DEBUG, CorePeepHole) << "Dive " << node->Content() << " into " << input.Content();
        return ctx.ChangeChildren(input, {
            input.HeadPtr(),
            ctx.ChangeChild(*node, 0U, input.ChildPtr(1U)),
            ctx.ChangeChild(*node, 0U, input.ChildPtr(2U))
        });
    } else if (node->ChildrenSize() == 2U) {
        if (input.IsCallable("Nothing")) {
            YQL_CLOG(DEBUG, CorePeepHole) << "Drop " << node->Content() << " over " << input.Content();
            return node->TailPtr();
        } else if (input.IsCallable("Just")) {
            YQL_CLOG(DEBUG, CorePeepHole) << "Drop " << node->Content() << " over " << input.Content();
            return IsSameAnnotation(*node->GetTypeAnn(), *input.GetTypeAnn()) ?
                node->HeadPtr() : input.HeadPtr();
        }
    }
    return node;
}

ui64 ToDate(ui64 now)      { return std::min<ui64>(NUdf::MAX_DATE - 1U, now / 86400000000ull); }
ui64 ToDatetime(ui64 now)  { return std::min<ui64>(NUdf::MAX_DATETIME - 1U, now / 1000000ull); }
ui64 ToTimestamp(ui64 now) { return std::min<ui64>(NUdf::MAX_TIMESTAMP - 1ULL, now); }

struct TPeepHoleRules {
    const TPeepHoleOptimizerMap CommonStageRules = {
        {"EquiJoin", &ExpandEquiJoin},
        {"SafeCast", &ExpandCast<false>},
        {"StrictCast", &ExpandCast<true>},
        {"AlterTo", &ExpandAlterTo},
        {"SqlIn", &ExpandSqlIn},
        {"Lookup", &RewriteSearchByKeyForTypesMismatch<false>},
        {"Contains", &RewriteSearchByKeyForTypesMismatch<true>},
        {"ListHas", &ExpandListHas},
        {"PgAnyResolvedOp", &ExpandPgArrayOp},
        {"PgNullIf", &ExpandPgNullIf},
        {"PgAllResolvedOp", &ExpandPgArrayOp},
        {"Map", &CleckClosureOnUpperLambdaOverList},
        {"OrderedMap", &CleckClosureOnUpperLambdaOverList},
        {"FlatMap", &CleckClosureOnUpperLambdaOverList},
        {"OrderedFlatMap", &CleckClosureOnUpperLambdaOverList},
        {"Filter", &CleckClosureOnUpperLambdaOverList},
        {"OrderedFilter", &CleckClosureOnUpperLambdaOverList},
        {"TakeWhile", &CleckClosureOnUpperLambdaOverList},
        {"SkipWhile", &CleckClosureOnUpperLambdaOverList},
        {"TakeWhileInclusive", &CleckClosureOnUpperLambdaOverList},
        {"SkipWhileInclusive", &CleckClosureOnUpperLambdaOverList},
        {"FoldMap", &CleckClosureOnUpperLambdaOverList<2U>},
        {"Fold1Map", &CleckClosureOnUpperLambdaOverList<1U, 2U>},
        {"Chain1Map", &CleckClosureOnUpperLambdaOverList<1U, 2U>},
        {"PartitionsByKeys", &ExpandPartitionsByKeys},
        {"DictItems", &MapForOptionalContainer},
        {"DictKeys", &MapForOptionalContainer},
        {"DictPayloads", &MapForOptionalContainer},
        {"HasItems", &MapForOptionalContainer},
        {"Length", &MapForOptionalContainer},
        {"ToIndexDict", &MapForOptionalContainer},
        {"Iterator", &WrapIteratorForOptionalList},
        {"IsKeySwitch", &ExpandIsKeySwitch},
        {"Mux", &ExpandMux},
        {"Demux", &ExpandDemux},
        {"OptionalReduce", &ExpandOptionalReduce},
        {"AggrMin", &ExpandAggrMinMax<true>},
        {"AggrMax", &ExpandAggrMinMax<false>},
        {"Min", &ExpandMinMax},
        {"Max", &ExpandMinMax},
        {"And", &OptimizeLogicalDups<true>},
        {"Or", &OptimizeLogicalDups<false>},
        {"CombineByKey", &ExpandCombineByKey},
        {"FinalizeByKey", &ExpandFinalizeByKey},
        {"SkipNullMembers", &ExpandSkipNullFields},
        {"SkipNullElements", &ExpandSkipNullFields},
        {"ConstraintsOf", &ExpandConstraintsOf},
        {"==", &ExpandSqlEqual<true, false>},
        {"!=", &ExpandSqlEqual<false, false>},
        {"IsNotDistinctFrom", &ExpandSqlEqual<true, true>},
        {"IsDistinctFrom", &ExpandSqlEqual<false, true>},
        {"<", &ExpandSqlCompare<true, false>},
        {">", &ExpandSqlCompare<false, false>},
        {"<=", &ExpandSqlCompare<true, true>},
        {">=", &ExpandSqlCompare<false, true>},
        {"AggrEquals", &ExpandAggrEqual<true>},
        {"AggrNotEquals", &ExpandAggrEqual<false>},
        {"AggrLess", &ExpandAggrCompare<true, false>},
        {"AggrGreater", &ExpandAggrCompare<false, false>},
        {"AggrLessOrEqual", &ExpandAggrCompare<true, true>},
        {"AggrGreaterOrEqual", &ExpandAggrCompare<false, true>},
        {"RangeEmpty", &ExpandRangeEmpty},
        {"AsRange", &ExpandAsRange},
        {"RangeFor", &ExpandRangeFor},
        {"RangeToPg", &ExpandRangeToPg},
        {"ToFlow", &DropToFlowDeps},
        {"CheckedAdd", &ExpandCheckedAdd},
        {"CheckedSub", &ExpandCheckedSub},
        {"CheckedMul", &ExpandCheckedMul},
        {"CheckedDiv", &ExpandCheckedDiv},
        {"CheckedMod", &ExpandCheckedMod},
        {"CheckedMinus", &ExpandCheckedMinus},
        {"JsonValue", &ExpandJsonValue},
        {"JsonExists", &ExpandJsonExists},
        {"EmptyIterator", &DropDependsOnFromEmptyIterator},
        {"Version", &ExpandVersion},
    };

    const TExtPeepHoleOptimizerMap CommonStageExtRules = {
        {"Aggregate", &ExpandAggregatePeephole},
        {"AggregateCombine", &ExpandAggregatePeephole},
        {"AggregateCombineState", &ExpandAggregatePeephole},
        {"AggregateMergeState", &ExpandAggregatePeephole},
        {"AggregateMergeFinalize", &ExpandAggregatePeephole},
        {"AggregateMergeManyFinalize", &ExpandAggregatePeephole},
        {"AggregateFinalize", &ExpandAggregatePeephole},
        {"CostsOf", &ExpandCostsOf},
        {"JsonQuery", &ExpandJsonQuery},
        {"MatchRecognize", &ExpandMatchRecognize},
        {"CalcOverWindow", &ExpandCalcOverWindow},
        {"CalcOverSessionWindow", &ExpandCalcOverWindow},
        {"CalcOverWindowGroup", &ExpandCalcOverWindow},
    };

    const TPeepHoleOptimizerMap SimplifyStageRules = {
        {"Map", &OptimizeMap<false>},
        {"OrderedMap", &OptimizeMap<true>},
        {"FlatMap", &ExpandFlatMap<false>},
        {"OrderedFlatMap", &ExpandFlatMap<true>},
        {"ListIf", &ExpandContainerIf<false, true>},
        {"OptionalIf", &ExpandContainerIf<false, false>},
        {"FlatListIf", &ExpandContainerIf<true, true>},
        {"FlatOptionalIf", &ExpandContainerIf<true, false>},
        {"IfPresent", &OptimizeIfPresent<false>}
    };

    const TPeepHoleOptimizerMap FinalStageRules = {
        {"Take", &OptimizeTake},
        {"Skip", &OptimizeSkip},
        {"GroupByKey", &PeepHoleConvertGroupBySingleKey},
        {"PartitionByKey", &PeepHolePlainKeyForPartitionByKey},
        {"ExtractMembers", &PeepHoleExpandExtractItems},
        {"DictFromKeys", &PeepHoleDictFromKeysToDict},
        {"AddMember", &ExpandAddMember},
        {"ReplaceMember", &ExpandReplaceMember},
        {"RemoveMember", &ExpandRemoveMember},
        {"RemoveMembers", &ExpandRemoveMembers},
        {"ForceRemoveMembers", &ExpandRemoveMembers},
        {"RemovePrefixMembers", &ExpandRemovePrefixMembers},
        {"AsSet", &ExpandAsSet},
        {"ForceRemoveMember", &ExpandRemoveMember},
        {"DivePrefixMembers", &ExpandDivePrefixMembers},
        {"FlattenMembers", &ExpandFlattenMembers},
        {"FlattenStructs", &ExpandFlattenStructs},
        {"FlattenByColumns", &ExpandFlattenByColumns},
        {"CastStruct", &ExpandCastStruct},
        {"Filter", &ExpandFilter<>},
        {"OrderedFilter", &ExpandFilter<>},
        {"TakeWhile", &ExpandFilter<false>},
        {"SkipWhile", &ExpandFilter<true>},
        {"LMap", &ExpandLMapOrShuffleByKeys},
        {"OrderedLMap", &ExpandLMapOrShuffleByKeys},
        {"ShuffleByKeys", &ExpandLMapOrShuffleByKeys},
        {"ExpandMap", &OptimizeExpandMap},
        {"MultiMap", &OptimizeMultiMap<false>},
        {"OrderedMultiMap", &OptimizeMultiMap<true>},
        {"Nth", &OptimizeNth},
        {"Member", &OptimizeMember},
        {"Condense1", &OptimizeCondense1},
        {"CombineCore", &OptimizeCombineCore},
        {"Chopper", &OptimizeChopper},
        {"WideCombiner", &OptimizeWideCombiner},
        {"WideCondense1", &OptimizeWideCondense1},
        {"WideChopper", &OptimizeWideChopper},
        {"MapJoinCore", &OptimizeMapJoinCore},
        {"GraceJoinCore", &OptimizeGraceJoinCore},
        {"GraceSelfJoinCore", &OptimizeGraceSelfJoinCore},
        {"CommonJoinCore", &OptimizeCommonJoinCore},
        {"BuildTablePath", &DoBuildTablePath},
        {"Exists", &OptimizeExists},
        {"SqueezeToDict", &OptimizeSqueezeToDict},
        {"NarrowFlatMap", &OptimizeNarrowFlatMap},
        {"NarrowMultiMap", &OptimizeWideMaps},
        {"WideMap", &OptimizeWideMaps},
        {"NarrowMap", &OptimizeWideMaps},
        {"Unordered", &DropAssume},
        {"AssumeUnique", &DropAssume},
        {"AssumeDistinct", &DropAssume},
        {"AssumeChopped", &DropAssume},
        {"EmptyFrom", &DropEmptyFrom},
        {"Top", &OptimizeTopOrSort<false, true>},
        {"TopSort", &OptimizeTopOrSort<true, true>},
        {"Sort", &OptimizeTopOrSort<true, false>},
        {"ToFlow", &OptimizeToFlow},
        {"Coalesce", &OptimizeCoalesce},
    };

    const TExtPeepHoleOptimizerMap FinalStageExtRules = {};

    const TExtPeepHoleOptimizerMap FinalStageNonDetRules = {
        {"Random", &Random0Arg<double>},
        {"RandomNumber", &Random0Arg<ui64>},
        {"RandomUuid", &Random0Arg<TGUID>},
        {"Now", &Now0Arg<nullptr>},
        {"CurrentUtcDate", &Now0Arg<&ToDate>},
        {"CurrentUtcDatetime", &Now0Arg<&ToDatetime>},
        {"CurrentUtcTimestamp", &Now0Arg<&ToTimestamp>}
    };

    const TExtPeepHoleOptimizerMap BlockStageExtRules = {
        {"WideMap", &OptimizeWideMapBlocks},
        {"WideFilter", &OptimizeWideFilterBlocks},
        {"WideToBlocks", &OptimizeWideToBlocks},
        {"WideFromBlocks", &OptimizeWideFromBlocks},
        {"WideTakeBlocks", &OptimizeWideTakeSkipBlocks},
        {"WideSkipBlocks", &OptimizeWideTakeSkipBlocks},
        {"BlockCompress", &OptimizeBlockCompress},
        {"WideTopBlocks", &OptimizeBlocksTopOrSort},
        {"WideTopSortBlocks", &OptimizeBlocksTopOrSort},
        {"WideSortBlocks", &OptimizeBlocksTopOrSort},
        {"BlockExtend", &OptimizeBlockExtend},
        {"BlockOrderedExtend", &OptimizeBlockExtend},
        {"ReplicateScalars", &OptimizeReplicateScalars},
        {"Skip", &OptimizeSkipTakeToBlocks},
        {"Take", &OptimizeSkipTakeToBlocks},
        {"BlockCombineAll", &OptimizeBlockCombine},
        {"BlockCombineHashed", &OptimizeBlockCombine},
        {"BlockMergeFinalizeHashed", &OptimizeBlockMerge},
        {"BlockMergeManyFinalizeHashed", &OptimizeBlockMerge},
        {"WideTop", &OptimizeTopOrSortBlocks},
        {"WideTopSort", &OptimizeTopOrSortBlocks},
        {"WideSort", &OptimizeTopOrSortBlocks},
    };

    const TExtPeepHoleOptimizerMap BlockStageExtFinalRules = {
        {"BlockExtend", &ExpandBlockExtend},
        {"BlockOrderedExtend", &ExpandBlockExtend},
        {"ReplicateScalars", &ExpandReplicateScalars} 
    };

    static const TPeepHoleRules& Instance() {
        return *Singleton<TPeepHoleRules>();
    }
};

THolder<IGraphTransformer> CreatePeepHoleCommonStageTransformer(TTypeAnnotationContext& types,
    IGraphTransformer* typeAnnotator, const TPeepholeSettings& peepholeSettings)
{
    TTransformationPipeline pipeline(&types);
    if (peepholeSettings.CommonConfig) {
        peepholeSettings.CommonConfig->AfterCreate(&pipeline);
    }

    AddStandardTransformers<false>(pipeline, typeAnnotator);
    if (peepholeSettings.CommonConfig) {
        peepholeSettings.CommonConfig->AfterTypeAnnotation(&pipeline);
    }

    auto issueCode = TIssuesIds::CORE_EXEC;

    pipeline.AddCommonOptimization(issueCode);
    pipeline.Add(CreateFunctorTransformer(
            [&types](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                return PeepHoleCommonStage(input, output, ctx, types,
                    TPeepHoleRules::Instance().CommonStageRules,
                    TPeepHoleRules::Instance().CommonStageExtRules);
            }
        ),
        "PeepHoleCommon",
        issueCode);

    if (peepholeSettings.CommonConfig) {
        peepholeSettings.CommonConfig->AfterOptimize(&pipeline);
    }

    return pipeline.BuildWithNoArgChecks(false);
}

THolder<IGraphTransformer> CreatePeepHoleFinalStageTransformer(TTypeAnnotationContext& types,
                                                               IGraphTransformer* typeAnnotator,
                                                               bool* hasNonDeterministicFunctions,
                                                               const TPeepholeSettings& peepholeSettings)
{
    TTransformationPipeline pipeline(&types);
    if (peepholeSettings.FinalConfig) {
        peepholeSettings.FinalConfig->AfterCreate(&pipeline);
    }

    AddStandardTransformers<true>(pipeline, typeAnnotator);
    if (peepholeSettings.FinalConfig) {
        peepholeSettings.FinalConfig->AfterTypeAnnotation(&pipeline);
    }

    auto issueCode = TIssuesIds::CORE_EXEC;

    pipeline.Add(
        CreateFunctorTransformer(
            [&types, hasNonDeterministicFunctions, withFinalRules = peepholeSettings.WithFinalStageRules,
            withNonDeterministicRules = peepholeSettings.WithNonDeterministicRules](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                auto stageRules = TPeepHoleRules::Instance().SimplifyStageRules;
                auto extStageRules = TExtPeepHoleOptimizerMap{};
                if (withFinalRules) {
                    const auto& finalRules = TPeepHoleRules::Instance().FinalStageRules;
                    stageRules.insert(finalRules.begin(), finalRules.end());

                    const auto& finalExtRules = TPeepHoleRules::Instance().FinalStageExtRules;
                    extStageRules.insert(finalExtRules.begin(), finalExtRules.end());
                }

                const auto& nonDetStageRules = withNonDeterministicRules ? TPeepHoleRules::Instance().FinalStageNonDetRules : TExtPeepHoleOptimizerMap{};
                return PeepHoleFinalStage(input, output, ctx, types, hasNonDeterministicFunctions, stageRules, extStageRules, nonDetStageRules);
            }
        ),
        "PeepHoleFinal",
        issueCode);

    pipeline.Add(
        CreateFunctorTransformer(
            [&types](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> IGraphTransformer::TStatus {
                if (types.IsBlockEngineEnabled()) {
                    const auto& extStageRules = TPeepHoleRules::Instance().BlockStageExtRules;
                    return PeepHoleBlockStage(input, output, ctx, types, extStageRules);
                } else {
                    output = input;
                    return IGraphTransformer::TStatus::Ok;
                }
            }
        ),
        "PeepHoleBlock",
        issueCode);

    if (peepholeSettings.FinalConfig) {
        peepholeSettings.FinalConfig->AfterOptimize(&pipeline);
    }

    pipeline.Add(
        CreateFunctorTransformer(
            [&types](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> IGraphTransformer::TStatus {
                if (types.IsBlockEngineEnabled()) {
                    const auto& extStageRules = TPeepHoleRules::Instance().BlockStageExtFinalRules;
                    return PeepHoleBlockStage(input, output, ctx, types, extStageRules);
                } else {
                    output = input;
                    return IGraphTransformer::TStatus::Ok;
                }
            }
        ),
        "PeepHoleFinalBlock",
        issueCode);

    return pipeline.BuildWithNoArgChecks(false);
}

}

IGraphTransformer::TStatus DoPeepHoleOptimizeNode(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
    IGraphTransformer& commonTransformer, IGraphTransformer& finalTransformer)
{
    output = input;

    for (bool isFinal = false;;) {
        const auto status = InstantTransform(isFinal ? finalTransformer : commonTransformer, output, ctx, input->IsLambda());
        if (status == IGraphTransformer::TStatus::Error || (status.HasRestart && input->IsLambda())) {
            return status;
        }
        if (status == IGraphTransformer::TStatus::Ok) {
            if (isFinal) {
                break;
            }
            isFinal = true;
        }
    }

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus PeepHoleOptimizeNode(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    TExprContext& ctx, TTypeAnnotationContext& types, IGraphTransformer* typeAnnotator,
    bool& hasNonDeterministicFunctions, const TPeepholeSettings& peepholeSettings)
{
    hasNonDeterministicFunctions = false;
    const auto commonTransformer = CreatePeepHoleCommonStageTransformer(types, typeAnnotator, peepholeSettings);
    const auto finalTransformer = CreatePeepHoleFinalStageTransformer(types, typeAnnotator,
        &hasNonDeterministicFunctions, peepholeSettings);
    return DoPeepHoleOptimizeNode(input, output, ctx, *commonTransformer, *finalTransformer);
}

THolder<IGraphTransformer> MakePeepholeOptimization(TTypeAnnotationContextPtr typeAnnotationContext, const IPipelineConfigurator* config) {
    TPeepholeSettings peepholeSettings;
    peepholeSettings.CommonConfig = peepholeSettings.FinalConfig = config;
    auto commonTransformer = CreatePeepHoleCommonStageTransformer(*typeAnnotationContext, nullptr, peepholeSettings);
    auto finalTransformer = CreatePeepHoleFinalStageTransformer(*typeAnnotationContext, nullptr, nullptr, peepholeSettings);
    return CreateFunctorTransformer(
            [common = std::move(commonTransformer), final = std::move(finalTransformer)](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) -> IGraphTransformer::TStatus {
                return DoPeepHoleOptimizeNode(input, output, ctx, *common, *final);
            });
}

}
