#include "kqp_opt_impl.h"

#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

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

bool UseReadTableRanges(const TKikimrTableDescription& tableData, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx) {
    /*
     * OLAP tables can not work with ordinary ReadTable in case there is no support in physical
     * optimizers for them.
     */
    if (tableData.Metadata->Kind == EKikimrTableKind::Olap) {
        return true;
    }

    auto predicateExtractSetting = kqpCtx->Config->GetOptPredicateExtract();

    if (predicateExtractSetting != EOptionalFlag::Auto) {
        return predicateExtractSetting == EOptionalFlag::Enabled;
    }

    if (kqpCtx->IsScanQuery() && kqpCtx->Config->EnablePredicateExtractForScanQuery) {
        return true;
    }

    if (kqpCtx->IsDataQuery() && kqpCtx->Config->EnablePredicateExtractForDataQuery) {
        return true;
    }

    if (kqpCtx->IsGenericQuery()) {
        return true;
    }

    return false;
}

bool HasIndexesToWrite(const TKikimrTableDescription& tableData) {
    bool hasIndexesToWrite = false;
    YQL_ENSURE(tableData.Metadata->Indexes.size() == tableData.Metadata->SecondaryGlobalIndexMetadata.size());
    for (const auto& index : tableData.Metadata->Indexes) {
        if (index.ItUsedForWrite()) {
            hasIndexesToWrite = true;
            break;
        }
    }

    return hasIndexesToWrite;
}

TExprBase BuildReadTable(const TCoAtomList& columns, TPositionHandle pos, const TKikimrTableDescription& tableData, bool forcePrimary,
    TExprContext& ctx, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
{
    TExprNode::TPtr readTable;
    const auto& tableMeta = BuildTableMeta(tableData, pos, ctx);

    TKqpReadTableSettings settings;
    settings.ForcePrimary = forcePrimary;

    if (UseReadTableRanges(tableData, kqpCtx)) {
        readTable = Build<TKqlReadTableRanges>(ctx, pos)
            .Table(tableMeta)
            .Ranges<TCoVoid>()
                .Build()
            .Columns(columns)
            .Settings(settings.BuildNode(ctx, pos))
            .ExplainPrompt()
                .Build()
            .Done().Ptr();
    } else {
        readTable = Build<TKqlReadTable>(ctx, pos)
            .Table(tableMeta)
            .Range()
                .From<TKqlKeyInc>()
                    .Build()
                .To<TKqlKeyInc>()
                    .Build()
                .Build()
            .Columns(columns)
            .Settings(settings.BuildNode(ctx, pos))
            .Done().Ptr();
    }

    return TExprBase(readTable);

}

TExprBase BuildReadTable(const TKiReadTable& read, const TKikimrTableDescription& tableData, bool forcePrimary,
    bool withSystemColumns, TExprContext& ctx, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
{
    const auto& columns = read.GetSelectColumns(ctx, tableData, withSystemColumns);

    auto readNode = BuildReadTable(columns, read.Pos(), tableData, forcePrimary, ctx, kqpCtx);

    return readNode;
}

TExprBase BuildReadTableIndex(const TKiReadTable& read, const TKikimrTableDescription& tableData,
    const TString& indexName, bool withSystemColumns, TExprContext& ctx, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
{
    if (UseReadTableRanges(tableData, kqpCtx)) {
        return Build<TKqlReadTableIndexRanges>(ctx, read.Pos())
            .Table(BuildTableMeta(tableData, read.Pos(), ctx))
            .Ranges<TCoVoid>()
                .Build()
            .ExplainPrompt()
                .Build()
            .Columns(read.GetSelectColumns(ctx, tableData, withSystemColumns))
            .Settings()
                .Build()
            .Index().Build(indexName)
            .Done();
    } else {
        return Build<TKqlReadTableIndex>(ctx, read.Pos())
            .Table(BuildTableMeta(tableData, read.Pos(), ctx))
            .Range()
                .From<TKqlKeyInc>()
                    .Build()
                .To<TKqlKeyInc>()
                    .Build()
                .Build()
            .Columns(read.GetSelectColumns(ctx, tableData, withSystemColumns))
            .Settings()
                .Build()
            .Index().Build(indexName)
            .Done();
    }
}

TExprNode::TPtr GetPgNotNullColumns(
    const TKikimrTableDescription& table,
    TPositionHandle pos,
    TExprContext& ctx)
{
    auto pgNotNullColumns = Build<TCoAtomList>(ctx, pos);

    for (const auto& [column, meta] : table.Metadata->Columns) {
        if (meta.NotNull && table.GetColumnType(column)->GetKind() == ETypeAnnotationKind::Pg) {
            pgNotNullColumns.Add<TCoAtom>()
                .Value(column).Build();
        }
    }
    return pgNotNullColumns.Done().Ptr();
}

TExprNode::TPtr IsUpdateSetting(TExprContext& ctx, const TPositionHandle& pos) {
    return Build<TCoNameValueTupleList>(ctx, pos)
        .Add()
            .Name().Build("IsUpdate")
        .Build()
    .Done().Ptr();
}

TExprBase BuildKqlSequencer(TExprBase& input, const TKikimrTableDescription& table,
    const TCoAtomList& outputCols, const TCoAtomList& defaultConstraintColumns,
    TPositionHandle pos, TExprContext& ctx)
{
    return Build<TKqlSequencer>(ctx, pos)
        .Input(input.Ptr())
        .Table(BuildTableMeta(table, pos, ctx))
        .Columns(outputCols.Ptr())
        .DefaultConstraintColumns(defaultConstraintColumns.Ptr())
        .InputItemType(ExpandType(pos, *input.Ref().GetTypeAnn(), ctx))
        .Done();
}

TCoAtomList BuildUpsertInputColumns(const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement, TPositionHandle pos, TExprContext& ctx)
{
    TVector<TExprNode::TPtr> result;
    result.reserve(inputColumns.Ref().ChildrenSize() + autoincrement.Ref().ChildrenSize());
    for(const auto& item: inputColumns) {
        result.push_back(item.Ptr());
    }

    for(const auto& item: autoincrement) {
        result.push_back(item.Ptr());
    }

    return Build<TCoAtomList>(ctx, pos)
        .Add(result)
        .Done();
}

std::pair<TExprBase, TCoAtomList> BuildWriteInput(const TKiWriteTable& write, const TKikimrTableDescription& table,
    const TCoAtomList& inputColumns, const TCoAtomList& autoIncrement,
    TPositionHandle pos, TExprContext& ctx)
{
    auto input = write.Input();
    const bool isWriteReplace = (GetTableOp(write) == TYdbOperation::Replace);

    TCoAtomList inputCols = BuildUpsertInputColumns(inputColumns, autoIncrement, pos, ctx);

    if (autoIncrement.Ref().ChildrenSize() > 0) {
        input = BuildKqlSequencer(input, table, inputCols, autoIncrement, pos, ctx);
    }

    if (isWriteReplace) {
        std::tie(input, inputCols) = CreateRowsToReplace(input, inputCols, table, write.Pos(), ctx);
    }

    auto baseInput = Build<TKqpWriteConstraint>(ctx, pos)
        .Input(input)
        .Columns(GetPgNotNullColumns(table, pos, ctx))
        .Done();

    return {baseInput, inputCols};
}

TExprBase BuildUpsertTable(const TKiWriteTable& write, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement,
    const TKikimrTableDescription& table, TExprContext& ctx)
{
    auto generateColumnsIfInsertNode = GetSetting(write.Settings().Ref(), "generate_columns_if_insert");
    YQL_ENSURE(generateColumnsIfInsertNode);
    TCoAtomList generateColumnsIfInsert = TCoNameValueTuple(generateColumnsIfInsertNode).Value().Cast<TCoAtomList>();

    const auto [input, columns] = BuildWriteInput(write, table, inputColumns, autoincrement, write.Pos(), ctx);
    if (generateColumnsIfInsert.Ref().ChildrenSize() > 0) {
        return Build<TKqlInsertOnConflictUpdateRows>(ctx, write.Pos())
            .Table(BuildTableMeta(table, write.Pos(), ctx))
            .Input(input.Ptr())
            .Columns(columns.Ptr())
            .ReturningColumns(write.ReturningColumns())
            .GenerateColumnsIfInsert(generateColumnsIfInsert)
            .Done();
    }

    auto effect = Build<TKqlUpsertRows>(ctx, write.Pos())
        .Table(BuildTableMeta(table, write.Pos(), ctx))
        .Input(input.Ptr())
        .Columns(columns.Ptr())
        .ReturningColumns(write.ReturningColumns())
        .Done();

    return effect;
}

TExprBase BuildUpsertTableWithIndex(const TKiWriteTable& write, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement,
    const TKikimrTableDescription& table, TExprContext& ctx)
{
    const auto [input, columns] = BuildWriteInput(write, table, inputColumns, autoincrement, write.Pos(), ctx);
    auto generateColumnsIfInsertNode = GetSetting(write.Settings().Ref(), "generate_columns_if_insert");
    YQL_ENSURE(generateColumnsIfInsertNode);
    TCoAtomList generateColumnsIfInsert = TCoNameValueTuple(generateColumnsIfInsertNode).Value().Cast<TCoAtomList>();

    auto effect = Build<TKqlUpsertRowsIndex>(ctx, write.Pos())
        .Table(BuildTableMeta(table, write.Pos(), ctx))
        .Input(input.Ptr())
        .Columns(columns.Ptr())
        .ReturningColumns(write.ReturningColumns())
        .GenerateColumnsIfInsert(generateColumnsIfInsert)
        .Done();

    return effect;
}

TExprBase BuildReplaceTable(const TKiWriteTable& write, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement,
    const TKikimrTableDescription& table, TExprContext& ctx)
{
    const auto [input, columns] = BuildWriteInput(write, table, inputColumns, autoincrement, write.Pos(), ctx);
    auto effect = Build<TKqlUpsertRows>(ctx, write.Pos())
        .Table(BuildTableMeta(table, write.Pos(), ctx))
        .Input(input.Ptr())
        .Columns(columns)
        .ReturningColumns(write.ReturningColumns())
        .Done();

    return effect;
}

TExprBase BuildReplaceTableWithIndex(const TKiWriteTable& write, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement,
    const TKikimrTableDescription& table, TExprContext& ctx)
{
    const auto [input, columns] = BuildWriteInput(write, table, inputColumns, autoincrement, write.Pos(), ctx);
    auto effect = Build<TKqlUpsertRowsIndex>(ctx, write.Pos())
        .Table(BuildTableMeta(table, write.Pos(), ctx))
        .Input(input.Ptr())
        .Columns(columns.Ptr())
        .ReturningColumns(write.ReturningColumns())
        .GenerateColumnsIfInsert<TCoAtomList>().Build()
        .Done();

    return effect;
}

TExprBase BuildInsertTable(const TKiWriteTable& write, bool abort, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement,
    const TKikimrTableDescription& table, TExprContext& ctx)
{
    const auto [input, columns] = BuildWriteInput(write, table, inputColumns, autoincrement, write.Pos(), ctx);
    auto effect = Build<TKqlInsertRows>(ctx, write.Pos())
        .Table(BuildTableMeta(table, write.Pos(), ctx))
        .Input(input.Ptr())
        .Columns(columns)
        .ReturningColumns(write.ReturningColumns())
        .OnConflict()
            .Value(abort ? "abort"sv : "revert"sv)
            .Build()
        .Done();

    return effect;
}

TExprBase BuildInsertTableWithIndex(const TKiWriteTable& write, bool abort, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement,
    const TKikimrTableDescription& table, TExprContext& ctx)
{
    const auto [input, columns] = BuildWriteInput(write, table, inputColumns, autoincrement, write.Pos(), ctx);
    return Build<TKqlInsertRowsIndex>(ctx, write.Pos())
        .Table(BuildTableMeta(table, write.Pos(), ctx))
        .Input(input.Ptr())
        .Columns(columns.Ptr())
        .ReturningColumns(write.ReturningColumns())
        .OnConflict()
            .Value(abort ? "abort"sv : "revert"sv)
            .Build()
        .Done();
}

TExprBase BuildUpdateOnTable(const TKiWriteTable& write, const TCoAtomList& inputColumns,
    const TKikimrTableDescription& tableData, TExprContext& ctx)
{
    return Build<TKqlUpdateRows>(ctx, write.Pos())
        .Table(BuildTableMeta(tableData, write.Pos(), ctx))
        .Input<TKqpWriteConstraint>()
            .Input(write.Input())
            .Columns(GetPgNotNullColumns(tableData, write.Pos(), ctx))
        .Build()
        .Columns(inputColumns)
        .ReturningColumns(write.ReturningColumns())
        .Done();
}


TExprBase BuildUpdateOnTableWithIndex(const TKiWriteTable& write, const TCoAtomList& inputColumns,
    const TKikimrTableDescription& tableData, TExprContext& ctx)
{
    return Build<TKqlUpdateRowsIndex>(ctx, write.Pos())
        .Table(BuildTableMeta(tableData, write.Pos(), ctx))
        .Input<TKqpWriteConstraint>()
            .Input(write.Input())
            .Columns(GetPgNotNullColumns(tableData, write.Pos(), ctx))
        .Build()
        .Columns(inputColumns)
        .ReturningColumns(write.ReturningColumns())
        .Settings(IsUpdateSetting(ctx, write.Pos()))
        .Done();
}

TExprBase BuildDeleteTable(const TKiWriteTable& write, const TKikimrTableDescription& tableData, TExprContext& ctx) {
    const auto keysToDelete = ProjectColumns(write.Input(), tableData.Metadata->KeyColumnNames, ctx);

    return Build<TKqlDeleteRows>(ctx, write.Pos())
        .Table(BuildTableMeta(tableData, write.Pos(), ctx))
        .Input(keysToDelete)
        .Done();
}

TExprBase BuildDeleteTableWithIndex(const TKiWriteTable& write, const TKikimrTableDescription& tableData, TExprContext& ctx) {
    const auto keysToDelete = ProjectColumns(write.Input(), tableData.Metadata->KeyColumnNames, ctx);

    return Build<TKqlDeleteRowsIndex>(ctx, write.Pos())
        .Table(BuildTableMeta(tableData, write.Pos(), ctx))
        .Input(keysToDelete)
        .Done();
}

TExprBase BuildRowsToDelete(const TKikimrTableDescription& tableData, bool withSystemColumns, const TCoLambda& filter,
    const TPositionHandle pos, TExprContext& ctx, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
{
    const auto tableMeta = BuildTableMeta(tableData, pos, ctx);
    const auto tableColumns = BuildColumnsList(tableData, pos, ctx, withSystemColumns, true /*ignoreWriteOnlyColumns*/);

    const auto allRows = BuildReadTable(tableColumns, pos, tableData, false, ctx, kqpCtx);

    return Build<TCoFilter>(ctx, pos)
        .Input(allRows)
        .Lambda(filter)
        .Done();
}

TExprBase BuildDeleteTable(const TKiDeleteTable& del, const TKikimrTableDescription& tableData, bool withSystemColumns,
    TExprContext& ctx, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
{
    auto rowsToDelete = BuildRowsToDelete(tableData, withSystemColumns, del.Filter(), del.Pos(), ctx, kqpCtx);
    auto keysToDelete = ProjectColumns(rowsToDelete, tableData.Metadata->KeyColumnNames, ctx);

    return Build<TKqlDeleteRows>(ctx, del.Pos())
        .Table(BuildTableMeta(tableData, del.Pos(), ctx))
        .Input(keysToDelete)
        .Done();
}

TExprBase BuildDeleteTableWithIndex(const TKiDeleteTable& del, const TKikimrTableDescription& tableData,
    bool withSystemColumns, TExprContext& ctx, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
{
    auto rowsToDelete = BuildRowsToDelete(tableData, withSystemColumns, del.Filter(), del.Pos(), ctx, kqpCtx);

    auto indexes = BuildSecondaryIndexVector(tableData, del.Pos(), ctx, nullptr,
        [] (const TKikimrTableMetadata& meta, TPositionHandle pos, TExprContext& ctx) -> TExprBase {
            return BuildTableMeta(meta, pos, ctx);
        });
    YQL_ENSURE(indexes);

    const auto& pk = tableData.Metadata->KeyColumnNames;

    auto tableDelete = Build<TKqlDeleteRows>(ctx, del.Pos())
        .Table(BuildTableMeta(tableData, del.Pos(), ctx))
        .Input(ProjectColumns(rowsToDelete, pk, ctx))
        .Done();

    TVector<TExprBase> effects;
    effects.push_back(tableDelete);

    for (const auto& [indexMeta, indexDesc] : indexes) {
        THashSet<TStringBuf> indexTableColumns;

        THashSet<TString> keyColumns;
        for (const auto& column : indexDesc->KeyColumns) {
            YQL_ENSURE(keyColumns.emplace(column).second);
            indexTableColumns.emplace(column);
        }

        for (const auto& column : pk) {
            if (keyColumns.insert(column).second) {
                indexTableColumns.emplace(column);
            }
        }

        auto indexDelete = Build<TKqlDeleteRows>(ctx, del.Pos())
            .Table(indexMeta)
            .Input(ProjectColumns(rowsToDelete, indexTableColumns, ctx))
            .Done();

        effects.push_back(indexDelete);
    }

    return Build<TExprList>(ctx, del.Pos()).Add(effects).Done();
}

TExprBase BuildRowsToUpdate(const TKikimrTableDescription& tableData, bool withSystemColumns, const TCoLambda& filter,
    const TPositionHandle pos, TExprContext& ctx, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
{
    auto kqlReadTable = BuildReadTable(BuildColumnsList(tableData, pos, ctx, withSystemColumns, true /*ignoreWriteOnlyColumns*/), pos, tableData, false, ctx, kqpCtx);

    return Build<TCoFilter>(ctx, pos)
        .Input(kqlReadTable)
        .Lambda(filter)
        .Done();
}

TExprBase BuildUpdatedRows(const TExprBase& rows, const TCoLambda& update, const THashSet<TStringBuf>& columns,
    const TPositionHandle pos, TExprContext& ctx) {
    auto rowArg = Build<TCoArgument>(ctx, pos)
        .Name("row")
        .Done();

    auto updateStruct = Build<TExprApplier>(ctx, pos)
        .Apply(update)
        .With(0, rowArg)
        .Done();

    const auto& updateStructType = update.Ref().GetTypeAnn()->Cast<TStructExprType>();
    TVector<TExprBase> updateTuples;
    for (const auto& column : columns) {
        TCoAtom columnAtom(ctx.NewAtom(pos, column));

        TExprBase valueSource = updateStructType->FindItem(column)
            ? updateStruct
            : TExprBase(rowArg);

        auto tuple = Build<TCoNameValueTuple>(ctx, pos)
            .Name(columnAtom)
            .Value<TCoMember>()
                .Struct(valueSource)
                .Name(columnAtom)
                .Build()
            .Done();

        updateTuples.push_back(tuple);
    }

    return Build<TCoMap>(ctx, pos)
        .Input(rows)
        .Lambda()
            .Args({rowArg})
            .Body<TCoAsStruct>()
                .Add(updateTuples)
                .Build()
            .Build()
        .Done();
}

THashSet<TStringBuf> GetUpdateColumns(const TKikimrTableDescription& tableData, const TCoLambda& update) {
    THashSet<TStringBuf> updateColumns;
    for (const auto& keyColumn : tableData.Metadata->KeyColumnNames) {
        updateColumns.emplace(keyColumn);
    }

    const auto& updateStructType = update.Ref().GetTypeAnn()->Cast<TStructExprType>();
    for (const auto& item : updateStructType->GetItems()) {
        updateColumns.emplace(item->GetName());
    }

    return updateColumns;
}

TExprBase BuildUpdateTable(const TKiUpdateTable& update, const TKikimrTableDescription& tableData,
    bool withSystemColumns, TExprContext& ctx, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
{
    auto rowsToUpdate = BuildRowsToUpdate(tableData, withSystemColumns, update.Filter(), update.Pos(), ctx, kqpCtx);

    auto updateColumns = GetUpdateColumns(tableData, update.Update());
    auto updatedRows = BuildUpdatedRows(rowsToUpdate, update.Update(), updateColumns, update.Pos(), ctx);

    TVector<TCoAtom> updateColumnsList;
    for (const auto& column : updateColumns) {
        updateColumnsList.push_back(TCoAtom(ctx.NewAtom(update.Pos(), column)));
    }

    return Build<TKqlUpsertRows>(ctx, update.Pos())
        .Table(BuildTableMeta(tableData, update.Pos(), ctx))
        .Input<TKqpWriteConstraint>()
            .Input(updatedRows)
            .Columns(GetPgNotNullColumns(tableData, update.Pos(), ctx))
        .Build()
        .Columns()
            .Add(updateColumnsList)
            .Build()
        .Settings(IsUpdateSetting(ctx, update.Pos()))
        .ReturningColumns(update.ReturningColumns())
        .Done();
}

TExprBase BuildUpdateTableWithIndex(const TKiUpdateTable& update, const TKikimrTableDescription& tableData,
    bool withSystemColumns, TExprContext& ctx, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
{
    auto rowsToUpdate = BuildRowsToUpdate(tableData, withSystemColumns, update.Filter(), update.Pos(), ctx, kqpCtx);

    TVector<TExprBase> effects;

    auto updateColumns = GetUpdateColumns(tableData, update.Update());

    auto updatedRows = BuildUpdatedRows(rowsToUpdate, update.Update(), updateColumns, update.Pos(), ctx);

    TVector<TCoAtom> updateColumnsList;

    for (const auto& column : updateColumns) {
        updateColumnsList.push_back(TCoAtom(ctx.NewAtom(update.Pos(), column)));
    }

    auto indexes = BuildSecondaryIndexVector(tableData, update.Pos(), ctx, nullptr,
        [] (const TKikimrTableMetadata& meta, TPositionHandle pos, TExprContext& ctx) -> TExprBase {
            return BuildTableMeta(meta, pos, ctx);
        });

    auto is_uniq = [](std::pair<TExprNode::TPtr, const TIndexDescription*>& x) {
        return x.second->Type == TIndexDescription::EType::GlobalSyncUnique;
    };

    const bool hasUniqIndex = std::find_if(indexes.begin(), indexes.end(), is_uniq) != indexes.end();

    // For uniq index rewrite UPDATE in to UPDATE ON
    if (hasUniqIndex) {
        auto effect = Build<TKqlUpdateRowsIndex>(ctx, update.Pos())
            .Table(BuildTableMeta(tableData, update.Pos(), ctx))
            .Input<TKqpWriteConstraint>()
                .Input(updatedRows)
                .Columns(GetPgNotNullColumns(tableData, update.Pos(), ctx))
            .Build()
            .ReturningColumns<TCoAtomList>().Build()
            .Columns<TCoAtomList>()
                .Add(updateColumnsList)
                .Build()
            .Settings(IsUpdateSetting(ctx, update.Pos()))
            .Done();

        effects.emplace_back(effect);
        return Build<TExprList>(ctx, update.Pos()).Add(effects).Done();
    }

    const auto& pk = tableData.Metadata->KeyColumnNames;

    auto tableUpsert = Build<TKqlUpsertRows>(ctx, update.Pos())
        .Table(BuildTableMeta(tableData, update.Pos(), ctx))
        .Input<TKqpWriteConstraint>()
            .Input(updatedRows)
            .Columns(GetPgNotNullColumns(tableData, update.Pos(), ctx))
        .Build()
        .Columns()
            .Add(updateColumnsList)
            .Build()
        .Settings(IsUpdateSetting(ctx, update.Pos()))
        .ReturningColumns(update.ReturningColumns())
        .Done();

    effects.push_back(tableUpsert);

    for (const auto& [indexMeta, indexDesc] : indexes) {
        THashSet<TStringBuf> indexTableColumns;

        bool indexKeyColumnsUpdated = false;
        THashSet<TString> keyColumns;
        for (const auto& column : indexDesc->KeyColumns) {
            YQL_ENSURE(keyColumns.emplace(column).second);

            // Table PK cannot be updated, so don't consider PK columns update as index update
            if (updateColumns.contains(column) && !tableData.GetKeyColumnIndex(column)) {
                indexKeyColumnsUpdated = true;
            }

            indexTableColumns.emplace(column);
        }

        for (const auto& column : pk) {
            if (keyColumns.insert(column).second) {
                indexTableColumns.emplace(column);
            }
        }

        if (indexKeyColumnsUpdated) {
            // Have to delete old index value from index table in case when index key columns were updated
            auto indexDelete = Build<TKqlDeleteRows>(ctx, update.Pos())
                .Table(indexMeta)
                .Input(ProjectColumns(rowsToUpdate, indexTableColumns, ctx))
                .Done();

            effects.push_back(indexDelete);
        }

        bool indexDataColumnsUpdated = false;
        for (const auto& column : indexDesc->DataColumns) {
            if (updateColumns.contains(column)) {
                indexDataColumnsUpdated = true;
            }

            indexTableColumns.emplace(column);
        }

        // Index table update required in case when index key or data columns were updated
        bool needIndexTableUpdate = indexKeyColumnsUpdated || indexDataColumnsUpdated;

        if (needIndexTableUpdate) {
            auto indexRows = BuildUpdatedRows(rowsToUpdate, update.Update(), indexTableColumns, update.Pos(), ctx);

            TVector<TCoAtom> indexColumnsList;
            for (const auto& column : indexTableColumns) {
                indexColumnsList.push_back(TCoAtom(ctx.NewAtom(update.Pos(), column)));
            }

            auto indexUpsert = Build<TKqlUpsertRows>(ctx, update.Pos())
                .Table(indexMeta)
                .Input<TKqpWriteConstraint>()
                    .Input(indexRows)
                    .Columns(GetPgNotNullColumns(tableData, update.Pos(), ctx))
                .Build()
                .ReturningColumns<TCoAtomList>().Build()
                .Columns()
                    .Add(indexColumnsList)
                    .Build()
                .Settings().Build()
                .Done();

            effects.push_back(indexUpsert);
        }
    }

    return Build<TExprList>(ctx, update.Pos()).Add(effects).Done();
}

TExprNode::TPtr HandleReadTable(const TKiReadTable& read, TExprContext& ctx, const TKikimrTablesData& tablesData,
    bool withSystemColumns, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
{
    TKikimrKey key(ctx);
    YQL_ENSURE(key.Extract(read.TableKey().Ref()));
    YQL_ENSURE(key.GetKeyType() == TKikimrKey::Type::Table);
    auto& tableData = GetTableData(tablesData, read.DataSource().Cluster(), key.GetTablePath());
    auto view = key.GetView();

    if (view && !view->PrimaryFlag) {
        const auto& indexName = view->Name;
        if (!ValidateTableHasIndex(tableData.Metadata, ctx, read.Pos())) {
            return nullptr;
        }

        if (kqpCtx->IsScanQuery() && !kqpCtx->Config->EnableKqpScanQueryStreamLookup) {
            const TString err = "Secondary index is not supported for ScanQuery";
            ctx.AddError(YqlIssue(ctx.GetPosition(read.Pos()), TIssuesIds::KIKIMR_BAD_REQUEST, err));
            return nullptr;
        }

        auto [metadata, state] = tableData.Metadata->GetIndexMetadata(indexName);
        YQL_ENSURE(metadata, "unable to find metadata for index: " << indexName);
        YQL_ENSURE(state == TIndexDescription::EIndexState::Ready
            || state == TIndexDescription::EIndexState::WriteOnly);

        if (state != TIndexDescription::EIndexState::Ready) {
            auto err = TStringBuilder()
                << "Requested index: " << indexName
                << " is not ready to use";
            ctx.AddError(YqlIssue(ctx.GetPosition(read.Pos()), TIssuesIds::KIKIMR_INDEX_IS_NOT_READY, err));
            return nullptr;
        }

        return BuildReadTableIndex(read, tableData, indexName, withSystemColumns, ctx, kqpCtx).Ptr();
    }

    return BuildReadTable(read, tableData, view && view->PrimaryFlag, withSystemColumns, ctx, kqpCtx).Ptr();
}

TExprBase WriteTableSimple(const TKiWriteTable& write, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement,
    const TKikimrTableDescription& tableData, TExprContext& ctx)
{
    auto op = GetTableOp(write);
    switch (op) {
        case TYdbOperation::Upsert:
            return BuildUpsertTable(write, inputColumns, autoincrement, tableData, ctx);
        case TYdbOperation::Replace:
            return BuildReplaceTable(write, inputColumns, autoincrement, tableData, ctx);
        case TYdbOperation::InsertAbort:
        case TYdbOperation::InsertRevert:
            return BuildInsertTable(write, op == TYdbOperation::InsertAbort, inputColumns, autoincrement, tableData, ctx);
        case TYdbOperation::UpdateOn:
            return BuildUpdateOnTable(write, inputColumns, tableData, ctx);
        case TYdbOperation::Delete:
            return BuildDeleteTable(write, tableData, ctx);
        case TYdbOperation::DeleteOn:
            return BuildDeleteTable(write, tableData, ctx);
        default:
            YQL_ENSURE(false, "Unsupported table operation: " << op << ", table: " << tableData.Metadata->Name);
    }
}

TExprBase WriteTableWithIndexUpdate(const TKiWriteTable& write, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement,
    const TKikimrTableDescription& tableData, TExprContext& ctx)
{
    auto op = GetTableOp(write);
    switch (op) {
        case TYdbOperation::Upsert:
            return BuildUpsertTableWithIndex(write, inputColumns, autoincrement, tableData, ctx);
        case TYdbOperation::Replace:
            return BuildReplaceTableWithIndex(write, inputColumns, autoincrement, tableData, ctx);
        case TYdbOperation::InsertAbort:
        case TYdbOperation::InsertRevert:
            return BuildInsertTableWithIndex(write, op == TYdbOperation::InsertAbort, inputColumns, autoincrement, tableData, ctx);
        case TYdbOperation::UpdateOn:
            return BuildUpdateOnTableWithIndex(write, inputColumns, tableData, ctx);
        case TYdbOperation::DeleteOn:
            return BuildDeleteTableWithIndex(write, tableData, ctx);
        default:
            YQL_ENSURE(false, "Unsupported table operation: " << (ui32)op << ", table: " << tableData.Metadata->Name);
    }

    Y_UNREACHABLE();
}

TExprNode::TPtr HandleWriteTable(const TKiWriteTable& write, TExprContext& ctx, const TKikimrTablesData& tablesData)
{
    auto& tableData = GetTableData(tablesData, write.DataSink().Cluster(), write.Table().Value());

    auto inputColumnsSetting = GetSetting(write.Settings().Ref(), "input_columns");
    YQL_ENSURE(inputColumnsSetting);
    auto inputColumns = TCoNameValueTuple(inputColumnsSetting).Value().Cast<TCoAtomList>();

    auto defaultConstraintColumnsNode = GetSetting(write.Settings().Ref(), "default_constraint_columns");
    YQL_ENSURE(defaultConstraintColumnsNode);
    auto defaultConstraintColumns = TCoNameValueTuple(defaultConstraintColumnsNode).Value().Cast<TCoAtomList>();

    auto op = GetTableOp(write);
    if (defaultConstraintColumns.Ref().ChildrenSize() > 0) {
        if (op == TYdbOperation::UpdateOn || op == TYdbOperation::DeleteOn) {
            const TString err = "Key columns are not specified.";
            ctx.AddError(YqlIssue(ctx.GetPosition(write.Pos()), TIssuesIds::KIKIMR_BAD_REQUEST, err));
            return nullptr;
        }
    }

    if (HasIndexesToWrite(tableData)) {
        return WriteTableWithIndexUpdate(write, inputColumns, defaultConstraintColumns, tableData, ctx).Ptr();
    } else {
        return WriteTableSimple(write, inputColumns, defaultConstraintColumns, tableData, ctx).Ptr();
    }
}

TExprBase HandleUpdateTable(const TKiUpdateTable& update, TExprContext& ctx,
    const TKikimrTablesData& tablesData, bool withSystemColumns, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
{
    const auto& tableData = GetTableData(tablesData, update.DataSink().Cluster(), update.Table().Value());

    if (HasIndexesToWrite(tableData)) {
        return BuildUpdateTableWithIndex(update, tableData, withSystemColumns, ctx, kqpCtx);
    } else {
        return BuildUpdateTable(update, tableData, withSystemColumns, ctx, kqpCtx);
    }
}

TExprBase HandleDeleteTable(const TKiDeleteTable& del, TExprContext& ctx, const TKikimrTablesData& tablesData,
    bool withSystemColumns, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
{
    auto& tableData = GetTableData(tablesData, del.DataSink().Cluster(), del.Table().Value());
    if (HasIndexesToWrite(tableData)) {
        return BuildDeleteTableWithIndex(del, tableData, withSystemColumns, ctx, kqpCtx);
    } else {
        return BuildDeleteTable(del, tableData, withSystemColumns, ctx, kqpCtx);
    }
}

TExprNode::TPtr HandleExternalWrite(const TCallable& effect, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    if (effect.Ref().ChildrenSize() <= 1) {
        return {};
    }
    // As a rule data sink is a second child for all write callables
    TExprBase dataSinkArg(effect.Ref().Child(1));
    if (auto maybeDataSink = dataSinkArg.Maybe<TCoDataSink>()) {
        TStringBuf dataSinkCategory = maybeDataSink.Cast().Category();
        auto dataSinkProviderIt = typesCtx.DataSinkMap.find(dataSinkCategory);
        if (dataSinkProviderIt != typesCtx.DataSinkMap.end()) {
            if (auto* dqIntegration = dataSinkProviderIt->second->GetDqIntegration()) {
                if (auto canWrite = dqIntegration->CanWrite(*effect.Raw(), ctx)) {
                    YQL_ENSURE(*canWrite, "Erros handling write");
                    if (auto result = dqIntegration->WrapWrite(effect.Ptr(), ctx)) {
                        return Build<TKqlExternalEffect>(ctx, effect.Pos())
                            .Input(result)
                            .Done()
                            .Ptr();
                    }
                }
            }
        }
    }
    return {};
}

} // namespace

const TKikimrTableDescription& GetTableData(const TKikimrTablesData& tablesData,
    TStringBuf cluster, TStringBuf table)
{
    const auto& tableData = tablesData.ExistingTable(cluster, table);
    YQL_ENSURE(tableData.Metadata);

    return tableData;
}

TIntrusivePtr<TKikimrTableMetadata> GetIndexMetadata(const TKqlReadTableIndex& read,
    const TKikimrTablesData& tables, TStringBuf cluster)
{
    const auto& tableDesc = GetTableData(tables, cluster, read.Table().Path());
    const auto& [indexMeta, _ ] = tableDesc.Metadata->GetIndexMetadata(read.Index().StringValue());
    return indexMeta;
}

TMaybe<TKqlQueryList> BuildKqlQuery(TKiDataQueryBlocks dataQueryBlocks, const TKikimrTablesData& tablesData,
    TExprContext& ctx, bool withSystemColumns, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typesCtx)
{
    TVector<TKqlQuery> queryBlocks;
    queryBlocks.reserve(dataQueryBlocks.ArgCount());
    for (const auto& block : dataQueryBlocks) {
        TVector<TExprBase> kqlEffects;
        TNodeOnNodeOwnedMap effectsMap;
        for (const auto& effect : block.Effects()) {
            if (auto maybeWrite = effect.Maybe<TKiWriteTable>()) {
                auto write = HandleWriteTable(maybeWrite.Cast(), ctx, tablesData);
                if (!write) {
                    return {};
                }

                kqlEffects.push_back(TExprBase(write));
            }

            if (auto maybeUpdate = effect.Maybe<TKiUpdateTable>()) {
                kqlEffects.push_back(HandleUpdateTable(maybeUpdate.Cast(), ctx, tablesData, withSystemColumns, kqpCtx));
            }

            if (auto maybeDelete = effect.Maybe<TKiDeleteTable>()) {
                kqlEffects.push_back(HandleDeleteTable(maybeDelete.Cast(), ctx, tablesData, withSystemColumns, kqpCtx));
            }

            if (TExprNode::TPtr result = HandleExternalWrite(effect, ctx, typesCtx)) {
                kqlEffects.emplace_back(result);
            }
            effectsMap[effect.Raw()] = kqlEffects.back().Ptr();
        }

        TVector<TKqlQueryResult> kqlResults;
        kqlResults.reserve(block.Results().Size());
        for (const auto& kiResult : block.Results()) {
            kqlResults.emplace_back(
                Build<TKqlQueryResult>(ctx, kiResult.Pos())
                    .Value(kiResult.Value())
                    .ColumnHints(kiResult.Columns())
                    .Done());
        }

        auto query = Build<TKqlQuery>(ctx, dataQueryBlocks.Pos())
            .Results()
                .Add(kqlResults)
                .Build()
             .Effects()
                .Add(kqlEffects)
                .Build()
             .Done();

        TOptimizeExprSettings optSettings(nullptr);
        optSettings.VisitChanges = true;
        TExprNode::TPtr optResult;
        auto status = OptimizeExpr(query.Ptr(), optResult,
            [&tablesData, &effectsMap](const TExprNode::TPtr& input, TExprContext& ctx) {
                auto node = TExprBase(input);

                if (auto maybeReturning = node.Maybe<TKiReturningList>()) {
                    auto returning = maybeReturning.Cast();
                    auto effect = returning.Update();

                    TStringBuf cluster;
                    TStringBuf table;

                    if (auto write = effect.Maybe<TKiWriteTable>()) {
                        cluster = write.Cast().DataSink().Cluster();
                        table = write.Cast().Table().Value();
                    }

                    if (auto update = effect.Maybe<TKiUpdateTable>()) {
                        cluster = update.Cast().DataSink().Cluster();
                        table = update.Cast().Table().Value();
                    }

                    auto& tableData = GetTableData(tablesData, cluster, table);
                    const auto& tableMeta = BuildTableMeta(tableData, effect.Pos(), ctx);
                    YQL_ENSURE(effectsMap[effect.Raw()]);

                    return Build<TKqlReturningList>(ctx, returning.Pos())
                        .Update(effectsMap[effect.Raw()])
                        .Columns(returning.Columns())
                        .Table(tableMeta)
                        .Done().Ptr();
                }

                return input;
            }, ctx, optSettings);

        if (status == IGraphTransformer::TStatus::Error) {
            return {};
        }

        YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);
        YQL_ENSURE(TMaybeNode<TKqlQuery>(optResult));

        queryBlocks.emplace_back(TKqlQuery(optResult));
    }

    for (auto& queryBlock : queryBlocks) {
        TExprNode::TPtr optResult;
        TOptimizeExprSettings optSettings(nullptr);
        optSettings.VisitChanges = true;
        auto status = OptimizeExpr(queryBlock.Ptr(), optResult,
            [&tablesData, withSystemColumns, &kqpCtx, &typesCtx](const TExprNode::TPtr& input, TExprContext& ctx) {
                auto node = TExprBase(input);
                if (auto input = node.Maybe<TCoRight>().Input()) {
                    if (auto maybeRead = input.Maybe<TKiReadTable>()) {
                        return HandleReadTable(maybeRead.Cast(), ctx, tablesData, withSystemColumns, kqpCtx);
                    }
                    if (input.Raw()->ChildrenSize() > 1 && TCoDataSource::Match(input.Raw()->Child(1))) {
                        auto dataSourceName = input.Raw()->Child(1)->Child(0)->Content();
                        auto dataSource = typesCtx.DataSourceMap.FindPtr(dataSourceName);
                        YQL_ENSURE(dataSource);
                        if (auto dqIntegration = (*dataSource)->GetDqIntegration()) {
                            auto newRead = dqIntegration->WrapRead(NYql::TDqSettings(), input.Cast().Ptr(), ctx);
                            if (newRead.Get() != input.Raw()) {
                                return newRead;
                            }
                        }
                    }
                }
                return input;
            }, ctx, optSettings);

        if (status == IGraphTransformer::TStatus::Error) {
            return {};
        }

        YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);
        YQL_ENSURE(TMaybeNode<TKqlQuery>(optResult));
        queryBlock = TMaybeNode<TKqlQuery>(optResult).Cast();
    }

    return Build<TKqlQueryList>(ctx, dataQueryBlocks.Pos())
        .Add(queryBlocks)
        .Done();
}

} // namespace NKikimr::NKqp::NOpt
