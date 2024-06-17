#include "kqp_opt_phy_effects_rules.h"
#include "kqp_opt_phy_effects_impl.h"

using namespace NYql;
using namespace NYql::NNodes;

namespace NKikimr::NKqp::NOpt {

TMaybeNode<TDqPhyPrecompute> PrecomputeCurrentDefaultsForKeys(const TDqPhyPrecompute& lookupKeys,
    const TCoAtomList& columnsWithDefault,
    const TKikimrTableDescription& table, TPositionHandle pos, TExprContext& ctx)
{
    TVector<TExprBase> lookupColumns;

    for(const auto& key: table.Metadata->KeyColumnNames) {
        auto atom = Build<TCoAtom>(ctx, pos)
                .Value(key)
                .Done();

        lookupColumns.emplace_back(std::move(atom));        
    }

    for(const auto& atom: columnsWithDefault) {
        lookupColumns.push_back(atom);
    }

    return PrecomputeTableLookupDict(
        lookupKeys, table, lookupColumns, pos, ctx, false);
}

TCoAtomList BuildNonDefaultColumns(
    const TKikimrTableDescription& table,
    const TCoAtomList& allColumns,
    const TCoAtomList& columnsWithDefault,
    TPositionHandle pos, TExprContext& ctx)
{
    TVector<TExprBase> columnsToUpdateSet;
    std::unordered_set<TString> unchangedColumns;

    for(const auto& column: columnsWithDefault) {
        unchangedColumns.emplace(TString(column));
    }

    for(const TString& key: table.Metadata->KeyColumnNames) {
        unchangedColumns.emplace(key);
    }

    for (const auto& column : allColumns) {
        auto colName = TString(column);
        auto it = unchangedColumns.find(colName);
        if (it != unchangedColumns.end()) {
            continue;
        }

        auto atom = Build<TCoAtom>(ctx, pos)
            .Value(colName)
            .Done();

        columnsToUpdateSet.emplace_back(std::move(atom));
    }

    return Build<TCoAtomList>(ctx, pos)
        .Add(columnsToUpdateSet)
        .Done();
}

TDqStage BuildInsertOnConflictUpdateInputStage(
    const TKikimrTableDescription& table,
    const TCoAtomList& upsertColumns,
    const TCoAtomList& columnsWithDefault,
    const TDictAndKeysResult& inputDictAndKeys,
    const TDqPhyPrecompute& lookupDict,
    TPositionHandle pos, TExprContext& ctx)
{
    auto nonDefaultColumns = BuildNonDefaultColumns(table, upsertColumns, columnsWithDefault, pos, ctx);

    auto inputKeysArg = TCoArgument(ctx.NewArgument(pos, "input_keys"));
    auto inputDictArg = TCoArgument(ctx.NewArgument(pos, "input_dict"));
    auto inputKeyArg = TCoArgument(ctx.NewArgument(pos, "input_key"));
    auto lookupDictArg = TCoArgument(ctx.NewArgument(pos, "lookup_dict"));
    auto presetHandlerPayload = TCoArgument(ctx.NewArgument(pos, "payload"));

    return Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(inputDictAndKeys.KeysPrecompute)
            .Add(inputDictAndKeys.DictPrecompute)
            .Add(lookupDict)
            .Build()
        .Program()
            .Args({inputKeysArg, inputDictArg, lookupDictArg})
            .Body<TCoIterator>()
                .List<TCoMap>()
                    .Input(inputKeysArg)
                    .Lambda()
                        .Args(inputKeyArg)
                        .Body<TCoIfPresent>()
                            .Optional<TCoLookup>()
                                .Collection(lookupDictArg)
                                .Lookup(inputKeyArg)    
                                .Build()
                            .PresentHandler<TCoLambda>()
                                .Args(presetHandlerPayload)
                                .Body<TCoFlattenMembers>()
                                    .Add()
                                        .Name().Build("")
                                        .Value(presetHandlerPayload)
                                    .Build()
                                .Add()
                                    .Name().Build("")
                                    .Value<TCoUnwrap>()
                                        .Optional<TCoExtractMembers>()
                                            .Input<TCoLookup>()
                                                .Collection(inputDictArg)
                                                .Lookup(inputKeyArg)
                                                .Build()
                                            .Members(nonDefaultColumns)
                                            .Build()
                                        .Build()
                                    .Build()
                                .Add()
                                    .Name().Build("")
                                    .Value(inputKeyArg)
                                    .Build()
                                .Build()
                            .Build()
                            .MissingValue<TCoFlattenMembers>()
                                .Add()
                                    .Name().Build("")
                                    .Value<TCoUnwrap>()
                                        .Optional<TCoLookup>()
                                            .Collection(inputDictArg)
                                            .Lookup(inputKeyArg)
                                            .Build()
                                        .Build()
                                    .Build()
                                .Add()
                                    .Name().Build("")
                                    .Value(inputKeyArg)
                                    .Build()
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();
}

TExprBase KqpRewriteGenerateIfInsert(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    auto maybeInsertOnConlictUpdate = node.Maybe<TKqlInsertOnConflictUpdateRows>();
    if (!maybeInsertOnConlictUpdate) {
        return node;
    }

    auto insertOnConlictUpdate = maybeInsertOnConlictUpdate.Cast();
    YQL_ENSURE(insertOnConlictUpdate.GenerateColumnsIfInsert().Ref().ChildrenSize() > 0);
    TCoAtomList columnsWithDefault = insertOnConlictUpdate.GenerateColumnsIfInsert();

    auto input = insertOnConlictUpdate.Input();
    auto pos = insertOnConlictUpdate.Input().Pos();

    const auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, insertOnConlictUpdate.Table().Path());

    auto payloadSelector = MakeRowsPayloadSelector(insertOnConlictUpdate.Columns(), tableDesc, pos, ctx);
    auto condenseResult = CondenseInputToDictByPk(input, tableDesc, payloadSelector, ctx);
    if (!condenseResult) {
        return node;
    }

    auto inputDictAndKeys = PrecomputeDictAndKeys(*condenseResult, pos, ctx);
    auto lookupDict = PrecomputeCurrentDefaultsForKeys(inputDictAndKeys.KeysPrecompute, columnsWithDefault, tableDesc, pos, ctx);
    if (!lookupDict) {
        return node;
    }

    auto newInput = Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(BuildInsertOnConflictUpdateInputStage(tableDesc, insertOnConlictUpdate.Columns(), columnsWithDefault, inputDictAndKeys, lookupDict.Cast(), pos, ctx))
            .Index().Build("0")
            .Build()
        .Done();

    return Build<TKqlUpsertRows>(ctx, insertOnConlictUpdate.Pos())
        .Input(newInput.Ptr())
        .Table(insertOnConlictUpdate.Table())
        .Columns(insertOnConlictUpdate.Columns())
        .Settings(insertOnConlictUpdate.Settings())
        .ReturningColumns(insertOnConlictUpdate.ReturningColumns())
        .Done();    
}

} // namespace NKikimr::NKqp::NOpt