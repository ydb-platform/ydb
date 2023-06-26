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

TDqPhyPrecompute PrecomputeDict(const TCondenseInputResult& condenseResult, TPositionHandle pos, TExprContext& ctx) {
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

} // namespace

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
    const TKikimrTableDescription& table, const THashSet<TString>& dataColumns,
    const THashSet<TString>& keyColumns, TPositionHandle pos, TExprContext& ctx)
{
    auto lookupColumns = CreateColumnsToSelectToUpdateIndex(table.Metadata->KeyColumnNames, dataColumns,
        keyColumns, pos, ctx);

    auto lookupColumnsList = Build<TCoAtomList>(ctx, pos)
        .Add(lookupColumns)
        .Done();

    auto lookupStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(lookupKeys)
            .Build()
        .Program()
            .Args({"keys_list"})
            .Body<TKqpLookupTable>()
                .Table(BuildTableMeta(table, pos, ctx))
                .LookupKeys<TCoIterator>()
                    .List("keys_list")
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

    return PrecomputeDict(*condenseLookupResult, lookupKeys.Pos(), ctx);
}

TExprBase MakeRowsFromDict(const TDqPhyPrecompute& dict, const TVector<TString>& dictKeys,
    const THashSet<TStringBuf>& columns, TPositionHandle pos, TExprContext& ctx)
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

} // namespace NKikimr::NKqp::NOpt
