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

    TMaybeNode<TExprBase> wideRead;
    if (auto maybeRead = node.Maybe<TKqpReadTable>()) {
        auto read = maybeRead.Cast();

        wideRead = Build<TKqpWideReadTable>(ctx, node.Pos())
            .Table(read.Table())
            .Range(read.Range())
            .Columns(read.Columns())
            .Settings(read.Settings())
            .Done();
    } else if (auto maybeRead = node.Maybe<TKqpReadTableRanges>()) {
        auto read = maybeRead.Cast();

        wideRead = Build<TKqpWideReadTableRanges>(ctx, node.Pos())
            .Table(read.Table())
            .Ranges(read.Ranges())
            .Columns(read.Columns())
            .Settings(read.Settings())
            .ExplainPrompt(read.ExplainPrompt())
            .Done();
    } else if (auto maybeRead = node.Maybe<TKqpReadOlapTableRanges>()) {
        auto read = maybeRead.Cast();

        if (typesCtx.IsBlockEngineEnabled()) {
            wideRead = Build<TCoWideFromBlocks>(ctx, node.Pos())
                .Input<TKqpBlockReadOlapTableRanges>()
                    .Table(read.Table())
                    .Ranges(read.Ranges())
                    .Columns(read.Columns())
                    .Settings(read.Settings())
                    .ExplainPrompt(read.ExplainPrompt())
                    .Process(read.Process())
                    .Build()
                .Done();
        } else {
            wideRead = Build<TKqpWideReadOlapTableRanges>(ctx, node.Pos())
                .Table(read.Table())
                .Ranges(read.Ranges())
                .Columns(read.Columns())
                .Settings(read.Settings())
                .ExplainPrompt(read.ExplainPrompt())
                .Process(read.Process())
                .Done();
        }
    } else {
        YQL_ENSURE(false, "Unknown read table operation: " << node.Ptr()->Content());
    }

    return Build<TCoNarrowMap>(ctx, node.Pos())
        .Input(wideRead.Cast())
        .Lambda()
            .Args(args)
            .Body<TCoAsStruct>()
                .Add(structItems)
                .Build()
            .Build()
        .Done();
}

} // namespace NKikimr::NKqp::NOpt
