#include "kqp_opt_helpers.h"

#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h> 

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;

TExprBase ExtractKeys(TCoArgument itemArg, const TKikimrTableDescription& tableDesc,
    TExprContext& ctx)
{
    TVector<TExprBase> keys;
    for (TString& keyColumnName : tableDesc.Metadata->KeyColumnNames) {
        auto key = Build<TCoMember>(ctx, itemArg.Pos())
            .Struct(itemArg)
            .Name().Build(keyColumnName)
            .Done();

        keys.emplace_back(std::move(key));
    }

    if (keys.size() == 1) {
        return keys[0];
    }

    return Build<TExprList>(ctx, itemArg.Pos())
        .Add(keys)
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

TVector<TExprBase> CreateColumnsToSelectToUpdateIndex(
    const TVector<std::pair<TExprNode::TPtr, const TIndexDescription*>> indexes,
    const TVector<TString>& pk,
    const THashSet<TString>& dataColumns,
    TPositionHandle pos,
    TExprContext& ctx)
{
    TVector<TExprBase> columnsToSelect;
    TSet<TString> columns;

    for (const auto& pair : indexes) {
        for (const auto& col : pair.second->KeyColumns) {
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

// Return set of data columns need to be save during index update
THashSet<TString> CreateDataColumnSetToRead(
    const TVector<std::pair<TExprNode::TPtr, const TIndexDescription*>>& indexes,
    const THashSet<TStringBuf>& inputColumns)
{
    THashSet<TString> res;

    for (const auto& index : indexes) {
        for (const auto& col : index.second->DataColumns) {
            if (!inputColumns.contains(col)) {
                res.emplace(col);
            }
        }
    }

    return res;
}

TExprBase RemoveDuplicateKeyFromInput(const TExprBase& input, const TKikimrTableDescription& tableDesc,
    TPositionHandle pos, TExprContext& ctx)
{
    const auto& keySelectorArg = Build<TCoArgument>(ctx, pos)
        .Name("item")
        .Done();

    const auto& streamArg = Build<TCoArgument>(ctx, pos)
        .Name("streamArg")
        .Done();

    return Build<TCoPartitionByKey>(ctx, pos)
        .Input(input)
        .KeySelectorLambda()
            .Args(keySelectorArg)
            .Body(ExtractKeys(keySelectorArg, tableDesc, ctx))
            .Build()
        .SortDirections<TCoVoid>().Build()
        .SortKeySelectorLambda<TCoVoid>().Build()
        .ListHandlerLambda()
            .Args({TStringBuf("stream")})
            .Body<TCoFlatMap>()
                .Input(TStringBuf("stream"))
                .Lambda<TCoLambda>()
                    .Args(streamArg)
                    .Body<TCoLast>()
                        .Input<TCoForwardList>()
                            .Stream<TCoNth>()
                                .Tuple(streamArg)
                                .Index().Value(ToString(1))
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Build()
        .Done();
}

// Replace absent input columns to NULL to perform REPLACE via UPSERT
std::pair<TExprBase, TCoAtomList> CreateRowsToReplace(const TExprBase& input,
    const TCoAtomList& inputColumns, const TKikimrTableDescription& tableDesc,
    TPositionHandle pos, TExprContext& ctx)
{
    THashSet<TStringBuf> inputColumnsSet;
    for (const auto& name : inputColumns) {
        inputColumnsSet.insert(name.Value());
    }

    auto rowArg = Build<TCoArgument>(ctx, pos)
        .Name("row")
        .Done();

    TVector<TCoAtom> writeColumns;
    TVector<TExprBase> writeMembers;

    for (const auto& [name, _] : tableDesc.Metadata->Columns) {
        TMaybeNode<TExprBase> memberValue;
        if (tableDesc.GetKeyColumnIndex(name) || inputColumnsSet.contains(name)) {
            memberValue = Build<TCoMember>(ctx, pos)
                .Struct(rowArg)
                .Name().Build(name)
                .Done();
        } else {
            auto type = tableDesc.GetColumnType(name);
            YQL_ENSURE(type);

            memberValue = Build<TCoNothing>(ctx, pos)
                .OptionalType(NCommon::BuildTypeExpr(pos, *type, ctx))
                .Done();
        }

        auto nameAtom = TCoAtom(ctx.NewAtom(pos, name));

        YQL_ENSURE(memberValue);
        auto memberTuple = Build<TCoNameValueTuple>(ctx, pos)
            .Name(nameAtom)
            .Value(memberValue.Cast())
            .Done();

        writeColumns.emplace_back(std::move(nameAtom));
        writeMembers.emplace_back(std::move(memberTuple));
    }

    auto writeData = Build<TCoMap>(ctx, pos)
        .Input(input)
        .Lambda()
            .Args({rowArg})
            .Body<TCoAsStruct>()
                .Add(writeMembers)
                .Build()
            .Build()
        .Done();

    auto columnList = Build<TCoAtomList>(ctx, pos)
        .Add(writeColumns)
        .Done();

    return {writeData, columnList};
}

} // namespace NKqp
} // namespace NKikimr

