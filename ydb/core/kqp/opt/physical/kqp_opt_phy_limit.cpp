#include "kqp_opt_phy_rules.h"
#include "kqp_opt_phy_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

THashSet<const TExprNode*> CollectConnections(TDqStage stage, TExprBase node) {
    THashSet<const TExprNode*> args;
    for (auto&& arg : stage.Program().Args()) {
        args.insert(arg.Raw());
    }

    THashSet<const TExprNode*> result;
    TNodeOnNodeOwnedMap replaceMap;
    VisitExpr(node.Ptr(), 
        [&](const TExprNode::TPtr& exprPtr) -> bool {
            TExprBase expr(exprPtr);
            if (expr.Maybe<TDqConnection>()) {
                return false;
            }
            if (args.contains(exprPtr.Get())) {
                result.insert(exprPtr.Get());
            }
            return true;
        });
    return result;
}

//FIXME: simplify KIKIMR-16987
TExprBase KqpApplyLimitToReadTableSource(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext&) {
    auto stage = node.Cast<TDqStage>();
    TMaybe<size_t> tableSourceIndex;
    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        auto input = stage.Inputs().Item(i);
        if (input.Maybe<TDqSource>() && input.Cast<TDqSource>().Settings().Maybe<TKqpReadRangesSourceSettings>()) {
            tableSourceIndex = i;
        }
    }
    if (!tableSourceIndex) {
        return node;
    }

    auto source = stage.Inputs().Item(*tableSourceIndex).Cast<TDqSource>();
    auto readRangesSource = source.Settings().Cast<TKqpReadRangesSourceSettings>();
    auto settings = TKqpReadTableSettings::Parse(readRangesSource.Settings());

    if (settings.ItemsLimit) {
        return node; // already set?
    }

    auto sourceArg = stage.Program().Args().Arg(*tableSourceIndex);
    TExprNode::TPtr foundTake;
    bool singleConsumer = true;
    VisitExpr(stage.Program().Body().Ptr(),
        [&](const TExprNode::TPtr& exprPtr) -> bool {
            TExprBase expr(exprPtr);
            if (expr.Maybe<TDqConnection>() || expr.Maybe<TDqPrecompute>() || expr.Maybe<TDqPhyPrecompute>()) {
                return false;
            }
            if (auto take = expr.Maybe<TCoTake>()) {
                auto maybeSkip = take.Input().Maybe<TCoSkip>();
                auto input = (maybeSkip ? maybeSkip.Cast().Input() : take.Input()).Cast();
                if (input.Raw() == sourceArg.Raw()) {
                    auto ptr = take.Cast().Ptr();
                    if (foundTake && foundTake != ptr) {
                        singleConsumer = false;
                    }
                    foundTake = ptr;
                }
            }
            return true;
        });

    if (!singleConsumer || !foundTake) {
        return node;
    }

    auto take = TCoTake(foundTake);

    auto maybeSkip = take.Input().Maybe<TCoSkip>();
    auto input = maybeSkip ? maybeSkip.Cast().Input() : take.Input();

    TMaybeNode<TExprBase> limitValue;
    auto maybeTakeCount = take.Count().Maybe<TCoUint64>();
    auto maybeSkipCount = maybeSkip.Count().Maybe<TCoUint64>();

    if (maybeTakeCount && (!maybeSkip || maybeSkipCount)) {
        ui64 totalLimit = FromString<ui64>(maybeTakeCount.Cast().Literal().Value());

        if (maybeSkipCount) {
            totalLimit += FromString<ui64>(maybeSkipCount.Cast().Literal().Value());
        }

        limitValue = Build<TCoUint64>(ctx, node.Pos())
            .Literal<TCoAtom>()
            .Value(ToString(totalLimit)).Build()
            .Done();
    } else {
        limitValue = take.Count();
        if (maybeSkip) {
            limitValue = Build<TCoPlus>(ctx, node.Pos())
                .Left(limitValue.Cast())
                .Right(maybeSkip.Cast().Count())
                .Done();
        }
    }

    YQL_CLOG(TRACE, ProviderKqp) << "-- set limit items value to " << limitValue.Cast().Ref().Dump();

    if (limitValue.Maybe<TCoUint64>()) {
        settings.SetItemsLimit(limitValue.Cast().Ptr());
    } else {
        if (auto args = CollectConnections(stage, limitValue.Cast())) {
            TVector<TCoArgument> stageArgs;
            TVector<TExprBase> inputs;
            TNodeOnNodeOwnedMap replaces;

            size_t index = 0;
            for (auto&& arg : stage.Program().Args()) {
                if (args.contains(arg.Raw())) {
                    TCoArgument replace{ctx.NewArgument(node.Pos(), TStringBuilder() << "_kqp_pc_arg_" << index)};
                    inputs.push_back(stage.Inputs().Item(index));
                    stageArgs.push_back(replace);
                    replaces[arg.Raw()] = replace.Ptr();
                }
                index += 1;
            }

            limitValue = Build<TDqCnValue>(ctx, node.Pos())
                    .Output()
                        .Stage<TDqStage>()
                            .Settings().Build()
                            .Inputs().Add(inputs).Build()
                            .Program<TCoLambda>()
                                .Args(stageArgs)
                                .Body<TCoToStream>()
                                    .Input<TCoJust>()
                                        .Input(ctx.ReplaceNodes(limitValue.Cast().Ptr(), replaces))
                                        .Build()
                                    .Build()
                                .Build()
                            .Build()
                        .Index().Build("0")
                        .Build()
                    .Done();
        }

        settings.SetItemsLimit(Build<TDqPrecompute>(ctx, node.Pos())
            .Input(limitValue.Cast())
            .Done().Ptr());
    }

    auto newSettings = Build<TKqpReadRangesSourceSettings>(ctx, source.Pos())
        .Table(readRangesSource.Table())
        .Columns(readRangesSource.Columns())
        .Settings(settings.BuildNode(ctx, source.Pos()))
        .RangesExpr(readRangesSource.RangesExpr())
        .ExplainPrompt(readRangesSource.ExplainPrompt())
        .Done();

    return ReplaceTableSourceSettings(stage, *tableSourceIndex, newSettings, ctx);
}                             
                              

TExprBase KqpApplyLimitToReadTable(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TCoTake>()) {
        return node;
    }
    auto take = node.Cast<TCoTake>();

    auto maybeSkip = take.Input().Maybe<TCoSkip>();
    auto input = maybeSkip ? maybeSkip.Cast().Input() : take.Input();

    bool isReadTable = input.Maybe<TKqpReadTable>().IsValid();
    bool isReadTableRanges = input.Maybe<TKqpReadTableRanges>().IsValid() || input.Maybe<TKqpReadOlapTableRanges>().IsValid() ;

    if (!isReadTable && !isReadTableRanges) {
        return node;
    }

    if (kqpCtx.IsScanQuery()) {
        auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, GetReadTablePath(input, isReadTableRanges));

        if (tableDesc.Metadata->Kind != EKikimrTableKind::Olap) {
            return node;
        }
    }

    auto settings = GetReadTableSettings(input, isReadTableRanges);
    if (settings.ItemsLimit) {
        return node; // already set?
    }

    TMaybeNode<TExprBase> limitValue;
    auto maybeTakeCount = take.Count().Maybe<TCoUint64>();
    auto maybeSkipCount = maybeSkip.Count().Maybe<TCoUint64>();

    if (maybeTakeCount && (!maybeSkip || maybeSkipCount)) {
        ui64 totalLimit = FromString<ui64>(maybeTakeCount.Cast().Literal().Value());

        if (maybeSkipCount) {
            totalLimit += FromString<ui64>(maybeSkipCount.Cast().Literal().Value());
        }

        limitValue = Build<TCoUint64>(ctx, node.Pos())
            .Literal<TCoAtom>()
            .Value(ToString(totalLimit)).Build()
            .Done();
    } else {
        limitValue = take.Count();
        if (maybeSkip) {
            limitValue = Build<TCoPlus>(ctx, node.Pos())
                .Left(limitValue.Cast())
                .Right(maybeSkip.Cast().Count())
                .Done();
        }
    }

    YQL_CLOG(TRACE, ProviderKqp) << "-- set limit items value to " << limitValue.Cast().Ref().Dump();

    if (limitValue.Maybe<TCoUint64>()) {
        settings.SetItemsLimit(limitValue.Cast().Ptr());
    } else {
        settings.SetItemsLimit(Build<TDqPrecompute>(ctx, node.Pos())
            .Input(limitValue.Cast())
            .Done().Ptr());
    }

    input = BuildReadNode(node.Pos(), ctx, input, settings);

    if (maybeSkip) {
        input = Build<TCoSkip>(ctx, node.Pos())
            .Input(input)
            .Count(maybeSkip.Cast().Count())
            .Done();
    }

    return Build<TCoTake>(ctx, take.Pos())
        .Input(input)
        .Count(take.Count())
        .Done();
}

} // namespace NKikimr::NKqp::NOpt

