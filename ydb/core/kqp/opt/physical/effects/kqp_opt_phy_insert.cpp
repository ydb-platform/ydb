#include "kqp_opt_phy_effects_rules.h"
#include "kqp_opt_phy_effects_impl.h"

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

TMaybeNode<TDqCnUnionAll> MakeConditionalInsertRows(const TExprBase& input, const TKikimrTableDescription& table,
    bool abortOnError, TPositionHandle pos, TExprContext& ctx)
{
    auto condenseResult = CondenseInput(input, ctx);
    if (!condenseResult) {
        return {};
    }

    TCoArgument rowsListArg(ctx.NewArgument(pos, "rows_list"));

    auto dict = Build<TCoToDict>(ctx, pos)
        .List(rowsListArg)
        .KeySelector(MakeTableKeySelector(table, pos, ctx))
        .PayloadSelector()
            .Args({"stub"})
            .Body<TCoVoid>()
                .Build()
            .Build()
        .Settings()
            .Add().Build("One")
            .Add().Build("Hashed")
            .Build()
        .Done();

    auto dictKeys = Build<TCoDictKeys>(ctx, pos)
        .Dict(dict)
        .Done();

    auto areKeysUniqueValue = Build<TCoCmpEqual>(ctx, pos)
        .Left<TCoLength>()
            .List(rowsListArg)
            .Build()
        .Right<TCoLength>()
            .List(dict)
            .Build()
        .Done();

    auto variantType = Build<TCoVariantType>(ctx, pos)
        .UnderlyingType<TCoTupleType>()
            .Add<TCoTypeOf>()
                .Value(rowsListArg)
                .Build()
            .Add<TCoTypeOf>()
                .Value(dictKeys)
                .Build()
            .Add<TCoTypeOf>()
                .Value(areKeysUniqueValue)
                .Build()
            .Build()
        .Done();

    auto computeKeysStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(condenseResult->StageInputs)
            .Build()
        .Program()
            .Args(condenseResult->StageArgs)
            .Body<TCoFlatMap>()
                .Input(condenseResult->Stream)
                .Lambda()
                    .Args({rowsListArg})
                    .Body<TCoAsList>()
                        .Add<TCoVariant>()
                            .Item(rowsListArg)
                            .Index().Build("0")
                            .VarType(variantType)
                            .Build()
                        .Add<TCoVariant>()
                            .Item(dictKeys)
                            .Index().Build("1")
                            .VarType(variantType)
                            .Build()
                        .Add<TCoVariant>()
                            .Item(areKeysUniqueValue)
                            .Index().Build("2")
                            .VarType(variantType)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    auto inputPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
           .Output()
               .Stage(computeKeysStage)
               .Index().Build("0")
               .Build()
           .Build()
        .Done();

    auto lookupKeysPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
           .Output()
               .Stage(computeKeysStage)
               .Index().Build("1")
               .Build()
           .Build()
        .Done();

    auto areKeysUniquePrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
           .Output()
               .Stage(computeKeysStage)
               .Index().Build("2")
               .Build()
           .Build()
        .Done();

    auto _true = MakeBool(pos, true, ctx);
    auto _false = MakeBool(pos, false, ctx);

    // Returns optional<bool>: <none> - nothing found, <false> - at least one key exists
    auto lookupStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(lookupKeysPrecompute)
            .Build()
        .Program()
            .Args({"keys_list"})
            .Body<TCoFlatMap>()
                .Input<TCoTake>()
                    .Input<TKqpLookupTable>()
                        .Table(BuildTableMeta(table, pos, ctx))
                        .LookupKeys<TCoIterator>()
                            .List("keys_list")
                            .Build()
                        .Columns()
                            .Build()
                        .Build()
                    .Count<TCoUint64>()
                        .Literal().Build("1")
                        .Build()
                    .Build()
                .Lambda()
                    .Args({"row"})
                    .Body<TCoJust>()
                        .Input(_false)
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    // Returns <bool>: <true> - nothing found, <false> - at least one key exists
    auto aggrStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add<TDqCnUnionAll>()
                .Output()
                    .Stage(lookupStage)
                    .Index().Build("0")
                    .Build()
                .Build()
            .Build()
        .Program()
            .Args({"row"})
            .Body<TCoCondense>()
                .Input("row")
                .State(_true)
                .SwitchHandler()
                    .Args({"item", "state"})
                    .Body(_false)
                    .Build()
                .UpdateHandler()
                    .Args({"item", "state"})
                    .Body(_false)
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    // Returns <bool>: <true> - no existing keys, <false> - at least one key exists
    auto noExistingKeysPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
            .Output()
                .Stage(aggrStage)
                .Index().Build("0")
                .Build()
            .Build()
        .Done();

    // Build condition checks depending on INSERT kind
    TCoArgument inputRowsArg(ctx.NewArgument(pos, "input_rows"));
    TCoArgument areKeysUniquesArg(ctx.NewArgument(pos, "are_keys_unique"));
    TCoArgument noExistingKeysArg(ctx.NewArgument(pos, "no_existing_keys"));

    TExprNode::TPtr uniqueKeysCheck;
    TExprNode::TPtr noExistingKeysCheck;

    if (abortOnError) {
        uniqueKeysCheck = Build<TKqpEnsure>(ctx, pos)
            .Value(_true)
            .Predicate(areKeysUniquesArg)
            .IssueCode().Build(ToString((ui32) TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION))
            .Message(MakeMessage("Duplicated keys found.", pos, ctx))
            .Done().Ptr();

        noExistingKeysCheck = Build<TKqpEnsure>(ctx, pos)
            .Value(_true)
            .Predicate(noExistingKeysArg)
            .IssueCode().Build(ToString((ui32) TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION))
            .Message(MakeMessage("Conflict with existing key.", pos, ctx))
            .Done().Ptr();
    } else {
        uniqueKeysCheck = areKeysUniquesArg.Ptr();
        noExistingKeysCheck = noExistingKeysArg.Ptr();
    }

    // Final pure compute stage to compute rows to insert and check for both conditions:
    // 1. No rows were found in table PK from input
    // 2. No duplicate PKs were found in input
    auto conditionStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(inputPrecompute)
            .Add(areKeysUniquePrecompute)
            .Add(noExistingKeysPrecompute)
            .Build()
        .Program()
            .Args({inputRowsArg, areKeysUniquesArg, noExistingKeysArg})
            .Body<TCoToStream>()
                .Input<TCoIfStrict>()
                    .Predicate<TCoAnd>()
                        .Add(uniqueKeysCheck)
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

    auto insertRows = MakeConditionalInsertRows(insert.Input(), table, abortOnError, insert.Pos(), ctx);
    if (!insertRows) {
        return node;
    }

    return Build<TKqlUpsertRows>(ctx, insert.Pos())
        .Table(insert.Table())
        .Input(insertRows.Cast())
        .Columns(insert.Columns())
        .Done();
}

} // namespace NKikimr::NKqp::NOpt
