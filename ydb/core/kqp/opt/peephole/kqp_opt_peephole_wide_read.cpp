#include "kqp_opt_peephole_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

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

bool IsIdentityLambda(const TExprNode& lambda) {
    const ui32 argsCount = lambda.Head().ChildrenSize();
    if (!argsCount || lambda.ChildrenSize() != argsCount + 1) {
        return false;
    }
    for (ui32 i = 0; i < argsCount; ++i) {
        if (lambda.Child(i + 1) != lambda.Head().Child(i)) {
            return false;
        }
    }
    return true;
}

bool IsBlockMemberIdentityLambda(const TExprNode& lambda) {
    const ui32 argsCount = lambda.Head().ChildrenSize();
    if (argsCount < 2 || lambda.ChildrenSize() != argsCount + 1) {
        return false;
    }

    // Last body element must be the block count arg passed through.
    if (lambda.Child(lambda.ChildrenSize() - 1) != lambda.Head().Child(argsCount - 1)) {
        return false;
    }

    TVector<TStringBuf> columnsInOrder;
    for (ui32 i = 1, e = lambda.ChildrenSize() - 1; i < e; ++i) {
        const auto *maybeBlockMember = lambda.Child(i);
        if (!maybeBlockMember->IsCallable("BlockMember")) {
            return false;
        }
        if (!IsValidBlockAsStruct(maybeBlockMember->Child(0))) {
            return false;
        }
        columnsInOrder.push_back(maybeBlockMember->Child(1)->Content());
    }

    const auto items =
        lambda.Child(1)->Child(0)->GetTypeAnn()->Cast<TBlockExprType>()->GetItemType()->Cast<TStructExprType>()->GetItems();
    if (items.size() != columnsInOrder.size()) {
        return false;
    }

    for (ui32 i = 0; i < items.size(); ++i) {
        if (items[i]->GetName() != columnsInOrder[i]) {
            return false;
        }
    }
    return true;
}

} // namespace

TExprBase KqpEliminateWideMapForLargeOlapTable(const TExprBase& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    Y_UNUSED(typesCtx);
    Y_UNUSED(ctx);
    if (!node.Maybe<TCoWideMap>()) {
        return node;
    }

    auto wideMap = node.Cast<TCoWideMap>();
    const auto& lambda = *wideMap.Lambda().Ptr();

    if (IsIdentityLambda(lambda) || IsBlockMemberIdentityLambda(lambda)) {
        return wideMap.Input();
    }

    return node;
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
