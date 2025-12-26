#include "kqp_opt_peephole_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

// Check if BlockAsStruct just wraps lambda arguments without any computation
bool IsIdentityBlockAsStruct(const TExprNode *node) {
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

// Check if WideMap lambda is identity: BlockAsStruct followed by BlockMember extracting same columns
bool IsIdentityBlockWideMapLambda(const TExprNode& lambda) {
    // Lambda structure: (lambda (args...) (return BlockMember... lastArg))
    const auto argsCount = lambda.Head().ChildrenSize();
    if (argsCount == 0 || lambda.ChildrenSize() != argsCount + 1) {
        return false;
    }

    // Check each output except the last one (block length column)
    for (ui32 i = 1; i < lambda.ChildrenSize() - 1; ++i) {
        const auto* maybeBlockMember = lambda.Child(i);
        if (!maybeBlockMember->IsCallable("BlockMember")) {
            return false;
        }
        if (!IsIdentityBlockAsStruct(maybeBlockMember->Child(0))) {
            return false;
        }
        // Check that BlockMember extracts the i-1 th field (0-indexed)
        // by comparing with the corresponding BlockAsStruct field name
        const auto* blockAsStruct = maybeBlockMember->Child(0);
        const auto& extractedName = maybeBlockMember->Child(1)->Content();

        // Find position in BlockAsStruct
        bool found = false;
        for (ui32 j = 0; j < blockAsStruct->ChildrenSize(); ++j) {
            if (blockAsStruct->Child(j)->Head().Content() == extractedName) {
                // Check that the argument index matches
                const auto* argNode = blockAsStruct->Child(j)->Child(1);
                // argNode should be the j-th argument of lambda
                ui32 argIndex = 0;
                for (; argIndex < argsCount; ++argIndex) {
                    if (lambda.Head().Child(argIndex) == argNode) {
                        break;
                    }
                }
                // For identity mapping: output i-1 should come from input i-1
                if (argIndex == i - 1 && j == i - 1) {
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            return false;
        }
    }

    // Check that the last output is the last argument (block length)
    const auto* lastOutput = lambda.Child(lambda.ChildrenSize() - 1);
    if (!lastOutput->IsArgument()) {
        return false;
    }
    if (lastOutput != lambda.Head().Child(argsCount - 1)) {
        return false;
    }

    return true;
}

} // namespace

TExprBase KqpEliminateWideMapForOlapTable(const TExprBase& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
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

    auto lambda = wideMap.Lambda();
    if (!IsIdentityBlockWideMapLambda(lambda.Ref())) {
        return node;
    }

    // Make sure that columns are in the same order
    TVector<TStringBuf> columnsInOrder;
    for (ui32 i = 1, e = lambda.Ptr()->ChildrenSize() - 1; i < e; ++i) {
        columnsInOrder.push_back(lambda.Ptr()->Child(i)->Child(1)->Content());
    }

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

TExprBase KqpEliminateIdentityWideMapInBlockHashJoin(const TExprBase& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    Y_UNUSED(typesCtx);

    // Match BlockHashJoinCore callable
    if (!node.Ref().IsCallable("BlockHashJoinCore")) {
        return node;
    }

    // BlockHashJoinCore has at least 2 inputs (left and right)
    if (node.Ref().ChildrenSize() < 2) {
        return node;
    }

    bool needsRewrite = false;
    TExprNode::TListType newChildren;
    newChildren.reserve(node.Ref().ChildrenSize());

    // Check first two children (left and right inputs)
    for (ui32 i = 0; i < 2; ++i) {
        const auto* child = node.Ref().Child(i);

        // Check if child is WideMap with identity lambda
        if (child->IsCallable("WideMap") && child->ChildrenSize() == 2) {
            const auto& wideMapLambda = child->Tail();
            if (IsIdentityBlockWideMapLambda(wideMapLambda)) {
                // Replace WideMap(input, identity_lambda) with just input
                needsRewrite = true;
                newChildren.push_back(child->HeadPtr());
                continue;
            }
        }
        newChildren.push_back(node.Ref().ChildPtr(i));
    }

    // Copy remaining children unchanged
    for (ui32 i = 2; i < node.Ref().ChildrenSize(); ++i) {
        newChildren.push_back(node.Ref().ChildPtr(i));
    }

    if (!needsRewrite) {
        return node;
    }

    return TExprBase(ctx.ChangeChildren(node.Ref(), std::move(newChildren)));
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
