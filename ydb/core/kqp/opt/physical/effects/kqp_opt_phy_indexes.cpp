#include "kqp_opt_phy_effects_impl.h"

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

TVector<TExprBase> CreateColumnsToSelectToUpdateIndex(
    const TVector<TString>& pk,
    const THashSet<TString>& dataColumns,
    const THashSet<TString>& keyColumns,
    TPositionHandle pos,
    TExprContext& ctx)
{
    TVector<TExprBase> columnsToSelect;
    TSet<TString> columns;

    for (const auto& col : keyColumns) {
        if (columns.insert(col).second) {
            auto atom = Build<TCoAtom>(ctx, pos)
                .Value(col)
                .Done();
            columnsToSelect.emplace_back(std::move(atom));
        }
    }

    for (const auto& col : dataColumns) {
        if (columns.insert(col).second) {
            auto atom = Build<TCoAtom>(ctx, pos)
                .Value(col)
                .Done();
            columnsToSelect.emplace_back(std::move(atom));
        }
    }

    for (const auto& p : pk) {
        const auto& atom = Build<TCoAtom>(ctx, pos)
            .Value(p)
            .Done();
        if (columns.insert(p).second) {
            columnsToSelect.push_back(atom);
        }
    }

    return columnsToSelect;
}

} // namespace

TDqPhyPrecompute PrecomputeCondenseInputResult(const TCondenseInputResult& condenseResult,
    TPositionHandle pos, TExprContext& ctx)
{
    auto computeDictStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(condenseResult.StageInputs)
            .Build()
        .Program()
            .Args(condenseResult.StageArgs)
            .Body(condenseResult.Stream)
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
            .Output()
                .Stage(computeDictStage)
                .Index().Build("0")
                .Build()
            .Build()
        .Done();
}

TVector<std::pair<TExprNode::TPtr, const TIndexDescription*>> BuildSecondaryIndexVector(
    const TKikimrTableDescription& table,
    TPositionHandle pos,
    TExprContext& ctx,
    const THashSet<TStringBuf>* filter,
    const std::function<TExprBase (const TKikimrTableMetadata&, TPositionHandle, TExprContext&)>& tableBuilder)
{
    TVector<std::pair<TExprNode::TPtr, const TIndexDescription*>> secondaryIndexes;
    secondaryIndexes.reserve(table.Metadata->Indexes.size());
    YQL_ENSURE(table.Metadata->Indexes.size() == table.Metadata->SecondaryGlobalIndexMetadata.size());
    for (size_t i = 0; i < table.Metadata->Indexes.size(); i++) {
        const auto& indexMeta = table.Metadata->Indexes[i];

        if (!indexMeta.ItUsedForWrite()) {
            continue;
        }

        // Add index if filter absent
        bool addIndex = filter ? false : true;

        for (const auto& col : indexMeta.KeyColumns) {

            if (filter) {
                // Add index if filter and at least one column present in the filter
                addIndex |= filter->contains(TStringBuf(col));
            }
        }

        for (const auto& col : indexMeta.DataColumns) {

            if (filter) {
                // Add index if filter and at least one column present in the filter
                addIndex |= filter->contains(TStringBuf(col));
            }
        }

        if (indexMeta.KeyColumns && addIndex) {
            auto indexTable = tableBuilder(*table.Metadata->SecondaryGlobalIndexMetadata[i], pos, ctx).Ptr();
            secondaryIndexes.emplace_back(std::make_pair(indexTable, &indexMeta));
        }
    }
    return secondaryIndexes;
}

TSecondaryIndexes BuildSecondaryIndexVector(const TKikimrTableDescription& table, TPositionHandle pos,
    TExprContext& ctx, const THashSet<TStringBuf>* filter)
{
    static auto cb = [] (const TKikimrTableMetadata& meta, TPositionHandle pos, TExprContext& ctx) -> TExprBase {
        return BuildTableMeta(meta, pos, ctx);
    };

    return BuildSecondaryIndexVector(table, pos, ctx, filter, cb);
}

TMaybeNode<TDqPhyPrecompute> PrecomputeTableLookupDict(const TDqPhyPrecompute& lookupKeys,
    const TKikimrTableDescription& table, const TVector<TExprBase>& columnsList,
    TPositionHandle pos, TExprContext& ctx, bool fixLookupKeys)
{
    auto lookupColumnsList = Build<TCoAtomList>(ctx, pos)
        .Add(columnsList)
        .Done();

    TExprNode::TPtr keys;

    // we need to left only table key columns to perform lookup
    // unfortunately we can't do it inside lookup stage
    if (fixLookupKeys) {
        auto keyArg = TCoArgument(ctx.NewArgument(pos, "key"));
        auto keysList = TCoArgument(ctx.NewArgument(pos, "keys_list"));
        TVector<TExprBase> keyLookupTuples;
        keyLookupTuples.reserve(table.Metadata->KeyColumnNames.size());

        for (const auto& key : table.Metadata->KeyColumnNames) {
            keyLookupTuples.emplace_back(
                Build<TCoNameValueTuple>(ctx, pos)
                    .Name().Build(key)
                    .Value<TCoMember>()
                        .Struct(keyArg)
                        .Name().Build(key)
                        .Build()
                    .Done());
        }

        auto list = Build<TCoToStream>(ctx, pos)
            .Input<TCoJust>()
                .Input<TCoMap>()
                    .Input(keysList)
                    .Lambda()
                        .Args({keyArg})
                        .Body<TCoAsStruct>()
                            .Add(keyLookupTuples)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Done().Ptr();

        keys = Build<TDqStage>(ctx, pos)
            .Inputs()
                .Add(lookupKeys)
                .Build()
            .Program()
                .Args({keysList})
                .Body(list)
                .Build()
            .Settings().Build()
            .Done().Ptr();

        keys = Build<TDqPhyPrecompute>(ctx, pos)
            .Connection<TDqCnValue>()
                .Output()
                    .Stage(keys)
                    .Index().Build("0")
                    .Build()
                .Build()
            .Done().Ptr();
    } else {
        keys = lookupKeys.Ptr();
    }

    auto lookupStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(keys)
            .Build()
        .Program()
            .Args({"keys_stage_arg"})
            .Body<TKqpLookupTable>()
                .Table(BuildTableMeta(table, pos, ctx))
                .LookupKeys<TCoIterator>()
                    .List("keys_stage_arg")
                    .Build()
                .Columns(lookupColumnsList)
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    auto lookup = Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(lookupStage)
            .Index().Build("0")
            .Build()
        .Done();

    auto lookupPayloadSelector = MakeRowsPayloadSelector(lookupColumnsList, table, lookupKeys.Pos(), ctx);
    auto condenseLookupResult = CondenseInputToDictByPk(lookup, table, lookupPayloadSelector, ctx);
    if (!condenseLookupResult) {
        return {};
    }

    return PrecomputeCondenseInputResult(*condenseLookupResult, lookupKeys.Pos(), ctx);
}

TMaybeNode<TDqPhyPrecompute> PrecomputeTableLookupDict(const TDqPhyPrecompute& lookupKeys,
    const TKikimrTableDescription& table, const THashSet<TString>& dataColumns,
    const THashSet<TString>& keyColumns, TPositionHandle pos, TExprContext& ctx)
{
    auto lookupColumns = CreateColumnsToSelectToUpdateIndex(table.Metadata->KeyColumnNames, dataColumns,
        keyColumns, pos, ctx);

    return PrecomputeTableLookupDict(lookupKeys, table, lookupColumns, pos, ctx, false);
}

TExprBase MakeRowsFromDict(const TDqPhyPrecompute& dict, const TVector<TString>& dictKeys,
    const TVector<TStringBuf>& columns, TPositionHandle pos, TExprContext& ctx)
{
    THashSet<TString> dictKeysSet(dictKeys.begin(), dictKeys.end());
    auto dictTupleArg = TCoArgument(ctx.NewArgument(pos, "dict_tuple"));

    TVector<TExprBase> rowTuples;
    for (const auto& column : columns) {
        auto columnAtom = ctx.NewAtom(pos, column);
        auto tupleIndex = dictKeysSet.contains(column)
            ? 0  // Key
            : 1; // Payload

        auto tuple = Build<TCoNameValueTuple>(ctx, pos)
            .Name(columnAtom)
            .Value<TCoMember>()
                .Struct<TCoNth>()
                    .Tuple(dictTupleArg)
                    .Index().Build(tupleIndex)
                    .Build()
                .Name(columnAtom)
                .Build()
            .Done();

        rowTuples.emplace_back(std::move(tuple));
    }

    auto computeRowsStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(dict)
            .Build()
        .Program()
            .Args({"dict"})
            .Body<TCoIterator>()
                .List<TCoMap>()
                    .Input<TCoDictItems>()
                        .Dict("dict")
                        .Build()
                    .Lambda()
                        .Args(dictTupleArg)
                        .Body<TCoAsStruct>()
                            .Add(rowTuples)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(computeRowsStage)
            .Index().Build("0")
            .Build()
        .Done();
}

TExprBase MakeRowsFromTupleDict(const TDqPhyPrecompute& dict, const TVector<TString>& dictKeys,
    const TVector<TStringBuf>& columns, TPositionHandle pos, TExprContext& ctx)
{
    THashSet<TString> dictKeysSet(dictKeys.begin(), dictKeys.end());
    auto dictTupleArg = TCoArgument(ctx.NewArgument(pos, "dict_tuple"));

    TVector<TExprBase> rowTuples;
    for (const auto& column : columns) {
        auto columnAtom = ctx.NewAtom(pos, column);
        auto tupleIndex = dictKeysSet.contains(column)
            ? 0  // Key
            : 1; // Payload

        auto extractor = Build<TCoNth>(ctx, pos)
            .Tuple(dictTupleArg)
            .Index().Build(tupleIndex)
            .Done().Ptr();

        if (tupleIndex) {
            extractor = Build<TCoNth>(ctx, pos)
                .Tuple(extractor)
                .Index().Build(0)
                .Done().Ptr();
        }

        auto tuple = Build<TCoNameValueTuple>(ctx, pos)
            .Name(columnAtom)
            .Value<TCoMember>()
                .Struct(extractor)
                .Name(columnAtom)
                .Build()
            .Done();

        rowTuples.emplace_back(std::move(tuple));
    }

    auto computeRowsStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(dict)
            .Build()
        .Program()
            .Args({"dict"})
            .Body<TCoIterator>()
                .List<TCoFlatMap>()
                    .Input<TCoDictItems>()
                        .Dict("dict")
                        .Build()
                    .Lambda()
                        .Args(dictTupleArg)
                        .Body<TCoOptionalIf>()
                            .Predicate<TCoNth>()
                                .Tuple<TCoNth>()
                                    .Tuple(dictTupleArg)
                                    .Index().Build(1)
                                    .Build()
                                .Index().Build(1)
                                .Build()
                             .Value<TCoAsStruct>()
                                .Add(rowTuples)
                                .Build()
                             .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(computeRowsStage)
            .Index().Build("0")
            .Build()
        .Done();
}

} // namespace NKikimr::NKqp::NOpt
