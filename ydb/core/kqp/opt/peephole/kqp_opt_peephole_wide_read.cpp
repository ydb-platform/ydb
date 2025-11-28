#include "kqp_opt_peephole_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {
constexpr const ui32 EliminationColumnLimit = 1000;

bool IsValidBlockAsStruct(const TExprNode *node) {
    if (!node->IsCallable("BlockAsStruct")) {
        return false;
    }
    for (ui32 i = 0; i < node->ChildrenSize(); ++i) {
        const auto *child = node->Child(i);
        if (!child->Child(1)->IsArgument()) {
            return false;
        }
    }
    return true;
}
} // namespace

TExprBase KqpEliminateWideMapForLargeOlapTable(const TExprBase& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    Y_UNUSED(typesCtx);
    if (!node.Maybe<TCoWideMap>()) {
        return node;
    }

    auto wideMap = node.Cast<TCoWideMap>();
    if (!wideMap.Input().Maybe<TCoFromFlow>()) {
        return node;
    }

    auto fromFlow = wideMap.Input().Cast<TCoFromFlow>();
    if (!fromFlow.Input().Maybe<TKqpBlockReadOlapTableRanges>()) {
        return node;
    }

    auto blockRead = fromFlow.Input().Cast<TKqpBlockReadOlapTableRanges>();
    if (blockRead.Columns().Size() < EliminationColumnLimit) {
        return node;
    }

    auto lambda = wideMap.Lambda();
    if (const auto wide = lambda.Ptr()->Head().ChildrenSize(); !wide || (lambda.Ptr()->ChildrenSize() != wide + 1)) {
        return node;
    }

    TVector<TStringBuf> columnsInOrder;
    // Looking for pattern (BlockMember BlockAsStruct ... `colName`) ...
    for (ui32 i = 1, e = lambda.Ptr()->ChildrenSize() - 1; i < e; ++i) {
        const auto *maybeBlockMember = lambda.Ptr()->Child(i);
        if (!maybeBlockMember->IsCallable("BlockMember")) {
            return node;
        }
        if (!IsValidBlockAsStruct(maybeBlockMember->Child(0))) {
            return node;
        }
        columnsInOrder.push_back(maybeBlockMember->Child(1)->Content());
    }

    // Make sure that columns are in the same order, otherwise we cannot eliminate that lambda.
    const auto items =
        lambda.Ptr()->Child(1)->Child(0)->GetTypeAnn()->Cast<TBlockExprType>()->GetItemType()->Cast<TStructExprType>()->GetItems();
    const auto itemsSize = items.size();
    if (itemsSize != columnsInOrder.size()) {
        return node;
    }

    for (ui32 i = 0; i < itemsSize; ++i) {
        if (items[i]->GetName() != columnsInOrder[i]) {
            return node;
        }
    }

    auto newBlockRead = Build<TKqpBlockReadOlapTableRanges>(ctx, node.Pos())
        .Table(blockRead.Table())
        .Ranges(blockRead.Ranges())
        .Columns(blockRead.Columns())
        .Settings(blockRead.Settings())
        .ExplainPrompt(blockRead.ExplainPrompt())
        .Process(blockRead.Process())
    .Done();

    return Build<TCoFromFlow>(ctx, node.Pos())
        .Input(newBlockRead)
    .Done();
}

TExprBase KqpBuildWideReadTable(const TExprBase& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    if (!node.Maybe<TKqpReadTable>() &&
        !node.Maybe<TKqpReadOlapTableRanges>() &&
        !node.Maybe<TKqpReadTableRanges>())
    {
        return node;
    }

    Y_UNUSED(typesCtx);

    auto rowType = node.Ref().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TStructExprType>();

    TVector<TCoArgument> args;
    args.reserve(rowType->GetSize());
    for (ui32 i = 0; i < rowType->GetSize(); ++i) {
        args.push_back(TCoArgument(ctx.NewArgument(node.Pos(), "arg")));
    }

    TVector<TExprBase> structItems;
    structItems.reserve(args.size());
    for (ui32 i = 0; i < args.size(); ++i) {
        structItems.emplace_back(
            Build<TCoNameValueTuple>(ctx, node.Pos())
                .Name().Build(rowType->GetItems()[i]->GetName())
                .Value(args[i])
                .Done());
    }

    TExprNode::TPtr wideRead;
    if (auto maybeRead = node.Maybe<TKqpReadTable>()) {
        wideRead = ctx.RenameNode(*node.Raw(), TKqpWideReadTable::CallableName());
    } else if (auto maybeRead = node.Maybe<TKqpReadTableRanges>()) {
        wideRead = ctx.RenameNode(*node.Raw(), TKqpWideReadTableRanges::CallableName());
    } else if (auto maybeRead = node.Maybe<TKqpReadOlapTableRanges>()) {
        wideRead = Build<TCoToFlow>(ctx, node.Pos())
            .Input<TCoWideFromBlocks>()
                .Input<TCoFromFlow>()
                    .Input(ctx.RenameNode(*node.Raw(), TKqpBlockReadOlapTableRanges::CallableName()))
                    .Build()
                .Build()
            .Done()
            .Ptr();
    } else {
        YQL_ENSURE(false, "Unknown read table operation: " << node.Ptr()->Content());
    }

    return Build<TCoNarrowMap>(ctx, node.Pos())
        .Input(wideRead)
        .Lambda()
            .Args(args)
            .Body<TCoAsStruct>()
                .Add(structItems)
                .Build()
            .Build()
        .Done();
}

} // namespace NKikimr::NKqp::NOpt
