#include "kqp_opt_phy_effects_rules.h"
#include "kqp_opt_phy_effects_impl.h"
#include "kqp_opt_phy_uniq_helper.h"

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

TMaybeNode<TDqCnUnionAll> MakeConditionalInsertRows(const TExprBase& input, const TKikimrTableDescription& table,
    const TMaybe<THashSet<TStringBuf>>& inputColumns, bool abortOnError, TPositionHandle pos, TExprContext& ctx)
{
    auto condenseResult = CondenseInput(input, ctx);
    if (!condenseResult) {
        return {};
    }

    auto helper = CreateInsertUniqBuildHelper(table, inputColumns, pos, ctx);
    auto computeKeysStage = helper->CreateComputeKeysStage(condenseResult.GetRef(), pos, ctx);

    auto inputPrecompute = helper->CreateInputPrecompute(computeKeysStage, pos, ctx);
    auto uniquePrecomputes = helper->CreateUniquePrecompute(computeKeysStage, pos, ctx);

    auto _true = MakeBool(pos, true, ctx);

    auto aggrStage = helper->CreateLookupExistStage(computeKeysStage, table, _true, pos, ctx);

    // Returns <bool>: <true> - no existing keys, <false> - at least one key exists
    auto noExistingKeysPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
            .Output()
                .Stage(aggrStage)
                .Index().Build("0")
                .Build()
            .Build()
        .Done();

    struct TUniqueCheckNodes {
        TUniqueCheckNodes(size_t sz) {
            Bodies.reserve(sz);
            Args.reserve(sz);
        }
        TVector<TExprNode::TPtr> Bodies;
        TVector<TCoArgument> Args;
    } uniqueCheckNodes(helper->GetChecksNum());

    TCoArgument noExistingKeysArg(ctx.NewArgument(pos, "no_existing_keys"));
    TExprNode::TPtr noExistingKeysCheck;

    // Build condition checks depending on INSERT kind
    if (abortOnError) {
        for (size_t i = 0; i < helper->GetChecksNum(); i++) {
            uniqueCheckNodes.Args.emplace_back(ctx.NewArgument(pos, "are_keys_unique"));
            uniqueCheckNodes.Bodies.emplace_back(Build<TKqpEnsure>(ctx, pos)
                .Value(_true)
                .Predicate(uniqueCheckNodes.Args.back())
                .IssueCode().Build(ToString((ui32) TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION))
                .Message(MakeMessage("Duplicated keys found.", pos, ctx))
                .Done().Ptr()
            );
        }

        noExistingKeysCheck = Build<TKqpEnsure>(ctx, pos)
            .Value(_true)
            .Predicate(noExistingKeysArg)
            .IssueCode().Build(ToString((ui32) TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION))
            .Message(MakeMessage("Conflict with existing key.", pos, ctx))
            .Done().Ptr();
    } else {
        for (size_t i = 0; i < helper->GetChecksNum(); i++) {
            uniqueCheckNodes.Args.emplace_back(ctx.NewArgument(pos, "are_keys_unique"));
            uniqueCheckNodes.Bodies.emplace_back(uniqueCheckNodes.Args.back().Ptr());
        }

        noExistingKeysCheck = noExistingKeysArg.Ptr();
    }


    TCoArgument inputRowsArg(ctx.NewArgument(pos, "input_rows"));
    // Final pure compute stage to compute rows to insert and check for both conditions:
    // 1. No rows were found in table PK from input
    // 2. No duplicate PKs were found in input
    TVector<TCoArgument> args;
    args.reserve(uniqueCheckNodes.Args.size() + 2);
    args.emplace_back(inputRowsArg);
    args.insert(args.end(), uniqueCheckNodes.Args.begin(), uniqueCheckNodes.Args.end());
    args.emplace_back(noExistingKeysArg);
    auto conditionStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(inputPrecompute)
            .Add(uniquePrecomputes)
            .Add(noExistingKeysPrecompute)
            .Build()
        .Program()
            .Args(args)
            .Body<TCoToStream>()
                .Input<TCoIfStrict>()
                    .Predicate<TCoAnd>()
                        .Add(uniqueCheckNodes.Bodies)
                        .Add(noExistingKeysCheck)
                        .Build()
                    .ThenValue(inputRowsArg)
                    .ElseValue<TCoList>()
                        .ListType(ExpandType(pos, *input.Ref().GetTypeAnn(), ctx))
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(conditionStage)
            .Index().Build("0")
            .Build()
        .Done();
}

TExprBase KqpBuildInsertStages(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlInsertRows>()) {
        return node;
    }

    auto insert = node.Cast<TKqlInsertRows>();
    bool abortOnError = insert.OnConflict().Value() == "abort"sv;
    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, insert.Table().Path());

    const bool isSink = NeedSinks(table, kqpCtx);
    const bool needPrecompute = !(isSink && abortOnError);

    if (needPrecompute) {
        const static TMaybe<THashSet<TStringBuf>> empty;
        auto insertRows = MakeConditionalInsertRows(insert.Input(), table, empty, abortOnError, insert.Pos(), ctx);
        if (!insertRows) {
            return node;
        }

        return Build<TKqlUpsertRows>(ctx, insert.Pos())
            .Table(insert.Table())
            .Input(insertRows.Cast())
            .Columns(insert.Columns())
            .ReturningColumns(insert.ReturningColumns())
            .Settings(insert.Settings())
            .Done();
    } else {
        return Build<TKqlUpsertRows>(ctx, insert.Pos())
            .Table(insert.Table())
            .Input(insert.Input())
            .Columns(insert.Columns())
            .ReturningColumns(insert.ReturningColumns())
            .Settings(insert.Settings())
            .Done();
    }
}

} // namespace NKikimr::NKqp::NOpt
