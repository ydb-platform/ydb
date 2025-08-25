#include "kqp_opt_peephole_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

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
