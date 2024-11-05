#include "kqp_opt_phy_effects_rules.h"
#include "kqp_opt_phy_effects_impl.h"

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

TDictAndKeysResult PrecomputeDictAndKeys(const TCondenseInputResult& condenseResult, TPositionHandle pos,
    TExprContext& ctx)
{
    auto dictType = Build<TCoListItemType>(ctx, pos)
        .ListType<TCoTypeOf>()
            .Value<TCoCollect>()
                .Input(condenseResult.Stream)
                .Build()
            .Build()
        .Done();

    auto variantType = Build<TCoVariantType>(ctx, pos)
        .UnderlyingType<TCoTupleType>()
            .Add(dictType)
            .Add<TCoListType>()
                .ItemType<TCoDictKeyType>()
                    .DictType(dictType)
                    .Build()
                .Build()
            .Build()
        .Done();

    auto computeKeysStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(condenseResult.StageInputs)
            .Build()
        .Program()
            .Args(condenseResult.StageArgs)
            .Body<TCoFlatMap>()
                .Input(condenseResult.Stream)
                .Lambda()
                    .Args({"dict"})
                    .Body<TCoAsList>()
                        .Add<TCoVariant>()
                            .Item("dict")
                            .Index().Build("0")
                            .VarType(variantType)
                            .Build()
                        .Add<TCoVariant>()
                            .Item<TCoDictKeys>()
                                .Dict("dict")
                                .Build()
                            .Index().Build("1")
                            .VarType(variantType)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    auto dictPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
            .Output()
                .Stage(computeKeysStage)
                .Index().Build("0")
                .Build()
            .Build()
        .Done();

    auto keysPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
            .Output()
                .Stage(computeKeysStage)
                .Index().Build("1")
                .Build()
            .Build()
        .Done();

    return TDictAndKeysResult {
        .DictPrecompute = dictPrecompute,
        .KeysPrecompute = keysPrecompute
    };
}

TExprBase KqpBuildUpdateStages(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlUpdateRows>()) {
        return node;
    }
    auto update = node.Cast<TKqlUpdateRows>();

    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, update.Table().Path());

    const bool isSink = NeedSinks(table, kqpCtx);
    const bool needPrecompute = !isSink;
    
    if (needPrecompute) {
        auto payloadSelector = MakeRowsPayloadSelector(update.Columns(), table, update.Pos(), ctx);
        auto condenseResult = CondenseInputToDictByPk(update.Input(), table, payloadSelector, ctx);
        if (!condenseResult) {
            return node;
        }

        auto inputDictAndKeys = PrecomputeDictAndKeys(*condenseResult, update.Pos(), ctx);

        auto prepareUpdateStage = Build<TDqStage>(ctx, update.Pos())
            .Inputs()
                .Add(inputDictAndKeys.KeysPrecompute)
                .Add(inputDictAndKeys.DictPrecompute)
                .Build()
            .Program()
                .Args({"keys_list", "dict"})
                .Body<TCoFlatMap>()
                    .Input<TKqpLookupTable>()
                        .Table(update.Table())
                        .LookupKeys<TCoIterator>()
                            .List("keys_list")
                            .Build()
                        .Columns(BuildColumnsList(table.Metadata->KeyColumnNames, update.Pos(), ctx))
                        .Build()
                    .Lambda()
                        .Args({"existingKey"})
                        .Body<TCoJust>()
                            .Input<TCoFlattenMembers>()
                                .Add()
                                    .Name().Build("")
                                    .Value<TCoUnwrap>() // Key should always exist in the dict
                                        .Optional<TCoLookup>()
                                            .Collection("dict")
                                            .Lookup("existingKey")
                                            .Build()
                                        .Build()
                                    .Build()
                                .Add()
                                    .Name().Build("")
                                    .Value("existingKey")
                                    .Build()
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Settings().Build()
            .Done();

        auto prepareUpdate = Build<TDqCnUnionAll>(ctx, update.Pos())
            .Output()
                .Stage(prepareUpdateStage)
                .Index().Build("0")
                .Build()
            .Done();

        return Build<TKqlUpsertRows>(ctx, node.Pos())
            .Table(update.Table())
            .Input(prepareUpdate)
            .Columns(update.Columns())
            .ReturningColumns(update.ReturningColumns())
            .Settings()
                .Add()
                    .Name().Build("IsUpdate")
                .Build()
            .Build()
            .Done();
    } else {
        return Build<TKqlUpsertRows>(ctx, update.Pos())
            .Table(update.Table())
            .Input(update.Input())
            .Columns(update.Columns())
            .ReturningColumns(update.ReturningColumns())
            .Settings()
                .Add()
                    .Name().Build("Mode")
                    .Value<TCoAtom>().Build("update")
                .Build()
                .Add()
                    .Name().Build("IsUpdate")
                .Build()
            .Build()
            .Done();
    }
}

} // namespace NKikimr::NKqp::NOpt
