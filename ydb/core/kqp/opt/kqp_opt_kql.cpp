#include "kqp_opt_impl.h"

#include <ydb/core/kqp/common/kqp_batch_operations.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

TVector<TString> GetMissingInputColumnsForReturning(
    const TKiWriteTable& write, const TCoAtomList& inputColumns)
{
    auto returnColumns = write.ReturningColumns().Cast<TCoAtomList>();
    if (returnColumns.Ref().ChildrenSize() == 0) {
        return {};
    }

    THashSet<TStringBuf> currentInput;
    for (const auto& name : inputColumns) {
        currentInput.insert(name.Value());
    }

    TVector<TString> result;
    for(const auto& returnCol : returnColumns) {
        if (!currentInput.contains(returnCol.Value())) {
            result.push_back(TString(returnCol.Value()));
        }
    }

    return result;
}

// Replace absent input columns to NULL to perform REPLACE via UPSERT
std::pair<TExprBase, TCoAtomList> ExtendInputRowsWithAbsentNullColumns(const TKiWriteTable& write, const TExprBase& input,
    const TCoAtomList& inputColumns, const TKikimrTableDescription& tableDesc,
    TPositionHandle pos, TExprContext& ctx)
{
    TVector<TString> maybeMissingColumnsToReplace;
    const auto op = GetTableOp(write);
    if (op == TYdbOperation::Replace) {
        for(const auto&[name, _]: tableDesc.Metadata->Columns) {
            maybeMissingColumnsToReplace.push_back(name);
        }
    }

    if (op == TYdbOperation::InsertAbort || op == TYdbOperation::InsertRevert || op == TYdbOperation::Upsert) {
        maybeMissingColumnsToReplace = GetMissingInputColumnsForReturning(write, inputColumns);
        if (maybeMissingColumnsToReplace.size() > 0) {
            for(const auto& inputCol: inputColumns) {
                maybeMissingColumnsToReplace.push_back(TString(inputCol.Value()));
            }
        }
    }

    if (maybeMissingColumnsToReplace.size() == 0) {
        return {input, inputColumns};
    }

    THashSet<TStringBuf> inputColumnsSet;
    for (const auto& name : inputColumns) {
        inputColumnsSet.insert(name.Value());
    }

    auto rowArg = Build<TCoArgument>(ctx, pos)
        .Name("row")
        .Done();

    TVector<TCoAtom> writeColumns;
    TVector<TExprBase> writeMembers;

    for (const auto& name : maybeMissingColumnsToReplace) {
        TMaybeNode<TExprBase> memberValue;
        if (tableDesc.GetKeyColumnIndex(name) || inputColumnsSet.contains(name)) {
            memberValue = Build<TCoMember>(ctx, pos)
                .Struct(rowArg)
                .Name().Build(name)
                .Done();
        } else {
            auto type = tableDesc.GetColumnType(name);
            YQL_ENSURE(type);
            const auto* optionalType = type->IsOptionalOrNull()
                ? type
                : ctx.MakeType<TOptionalExprType>(type);

            memberValue = Build<TCoNothing>(ctx, pos)
                .OptionalType(NCommon::BuildTypeExpr(pos, *optionalType, ctx))
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

bool HasIndexesToWrite(const TKikimrTableDescription& tableData) {
    bool hasIndexesToWrite = false;
    YQL_ENSURE(tableData.Metadata->Indexes.size() == tableData.Metadata->ImplTables.size());
    for (const auto& index : tableData.Metadata->Indexes) {
        if (index.ItUsedForWrite()) {
            hasIndexesToWrite = true;
            break;
        }
    }

    return hasIndexesToWrite;
}

TString IndexTypeToName(NYql::TIndexDescription::EType type) {
    switch (type) {
        case NYql::TIndexDescription::EType::GlobalSync:
            return "global sync secondary";
        case NYql::TIndexDescription::EType::GlobalAsync:
            return "global async secondary";
        case NYql::TIndexDescription::EType::GlobalSyncUnique:
            return "global sync unique secondary";
        case NYql::TIndexDescription::EType::GlobalSyncVectorKMeansTree:
            return "global sync vector_kmeans_tree";
        case NYql::TIndexDescription::EType::GlobalFulltextPlain:
            return "global sync fulltext_plain";
        case NYql::TIndexDescription::EType::GlobalFulltextRelevance:
            return "global sync fulltext_relevance";
    }
}

TExprBase BuildReadTable(const TCoAtomList& columns, TPositionHandle pos, const TKikimrTableDescription& tableData, bool forcePrimary, TMaybe<ui64> tabletId,
    TExprContext& ctx)
{
    TExprNode::TPtr readTable;
    const auto& tableMeta = BuildTableMeta(tableData, pos, ctx);

    TKqpReadTableSettings settings;
    settings.ForcePrimary = forcePrimary;
    settings.TabletId = tabletId;

    readTable = Build<TKqlReadTableRanges>(ctx, pos)
        .Table(tableMeta)
        .Ranges<TCoVoid>()
            .Build()
        .Columns(columns)
        .Settings(settings.BuildNode(ctx, pos))
        .ExplainPrompt()
            .Build()
        .Done().Ptr();

    return TExprBase(readTable);

}

TExprBase BuildReadTable(const TKiReadTable& read, const TKikimrTableDescription& tableData, bool forcePrimary,
    bool withSystemColumns, TExprContext& ctx)
{
    const auto& columns = read.GetSelectColumns(ctx, tableData, withSystemColumns);
    const auto tabletId =  NYql::HasSetting(read.Settings().Ref(), "tabletid")
        ? TMaybe<ui64>{FromString<ui64>(NYql::GetSetting(read.Settings().Ref(), "tabletid")->Child(1)->Content())}
        : TMaybe<ui64>{};
    auto readNode = BuildReadTable(columns, read.Pos(), tableData, forcePrimary, tabletId, ctx);

    return readNode;
}

TExprBase BuildReadTableIndex(const TKiReadTable& read, const TKikimrTableDescription& tableData,
    const TString& indexName, bool withSystemColumns, TExprContext& ctx)
{
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

TExprNode::TPtr IsUpdateSetting(const bool isStreamIndexWrite, TExprContext& ctx, const TPositionHandle& pos) {
    auto settings = Build<TCoNameValueTupleList>(ctx, pos);
    if (isStreamIndexWrite) {
        settings
            .Add()
                .Name().Build("Mode")
                .Value<TCoAtom>().Build("update")
            .Build();
    }
    return settings
        .Add()
            .Name().Build("IsUpdate")
        .Build()
    .Done().Ptr();
}

TExprNode::TPtr IsConditionalUpdateSetting(const bool isStreamIndexWrite, TExprContext& ctx, const TPositionHandle& pos) {
    return Build<TCoNameValueTupleList>(ctx, pos)
        .Add()
            .Name().Build("Mode")
            .Value<TCoAtom>().Build(isStreamIndexWrite ? "update_conditional" : "upsert") // Rows were read from table, so we are sure that they exist.
        .Build()
        .Add()
            .Name().Build("IsUpdate")
        .Build()
        .Add()
            .Name().Build("IsConditionalUpdate")
        .Build()
    .Done().Ptr();
}

TExprNode::TPtr IsConditionalDeleteSetting(TExprContext& ctx, const TPositionHandle& pos) {
    return Build<TCoNameValueTupleList>(ctx, pos)
        .Add()
            .Name().Build("IsConditionalDelete")
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
    const TCoAtomList& inputColumns, const TCoAtomList& autoIncrement, const bool /*isSink*/,
    TPositionHandle pos, TExprContext& ctx)
{
    auto input = write.Input();

    TCoAtomList inputCols = BuildUpsertInputColumns(inputColumns, autoIncrement, pos, ctx);

    if (autoIncrement.Ref().ChildrenSize() > 0) {
        input = BuildKqlSequencer(input, table, inputCols, autoIncrement, pos, ctx);
    }

    std::tie(input, inputCols) = ExtendInputRowsWithAbsentNullColumns(write, input, inputCols, table, write.Pos(), ctx);

    auto baseInput = Build<TKqpWriteConstraint>(ctx, pos)
        .Input(input)
        .Columns(GetPgNotNullColumns(table, pos, ctx))
        .Done();

    return {baseInput, inputCols};
}


TCoAtomList ExtendGenerateOnInsertColumnsList(const TKiWriteTable& write, TCoAtomList& generateColumnsIfInsert, const TCoAtomList& inputColumns, const TCoAtomList& autoincrement, TExprContext& ctx) {
    auto inputCols = BuildUpsertInputColumns(inputColumns, autoincrement, write.Pos(), ctx);
    auto maybeMissingColumnsToReplace = GetMissingInputColumnsForReturning(write, inputCols);
    TVector<TExprNode::TPtr> result;
    result.reserve(generateColumnsIfInsert.Ref().ChildrenSize() + maybeMissingColumnsToReplace.size());
    for(const auto& item: generateColumnsIfInsert) {
        result.push_back(item.Ptr());
    }

    for(auto& name: maybeMissingColumnsToReplace) {
        auto atom = TCoAtom(ctx.NewAtom(write.Pos(), name));
        result.push_back(atom.Ptr());
    }

    return Build<TCoAtomList>(ctx, write.Pos()).Add(result).Done();
}

TExprBase BuildFillTable(const TKiWriteTable& write, TExprContext& ctx)
{
    auto originalPathNode = GetSetting(write.Settings().Ref(), "OriginalPath");
    AFL_ENSURE(originalPathNode);
    return Build<TKqlFillTable>(ctx, write.Pos())
        .Input(write.Input())
        .Table(write.Table())
        .Cluster(write.DataSink().Cluster())
        .OriginalPath(TCoNameValueTuple(originalPathNode).Value().Cast<TCoAtom>())
        .Done();
}

TExprBase BuildUpsertTable(const TKiWriteTable& write, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement, const bool isSink,
    const TKikimrTableDescription& table, TExprContext& ctx, TKqpOptimizeContext& kqpCtx)
{
    auto generateColumnsIfInsertNode = GetSetting(write.Settings().Ref(), "generate_columns_if_insert");
    YQL_ENSURE(generateColumnsIfInsertNode);
    TCoAtomList generateColumnsIfInsert = TCoNameValueTuple(generateColumnsIfInsertNode).Value().Cast<TCoAtomList>();
    auto settings = FilterSettings(write.Settings().Ref(), {"AllowInconsistentWrites"}, ctx);

    generateColumnsIfInsert = ExtendGenerateOnInsertColumnsList(write, generateColumnsIfInsert, inputColumns, autoincrement, ctx);

    settings = AddSetting(*settings, write.Pos(), "Mode", Build<TCoAtom>(ctx, write.Pos()).Value("upsert").Done().Ptr(), ctx);

    if (const auto requestContext = kqpCtx.UserRequestContext; requestContext && requestContext->IsStreamingQuery) {
        settings = AddSetting(*settings, write.Pos(), "AllowInconsistentWrites", nullptr, ctx);
    }

    const auto [input, columns] = BuildWriteInput(write, table, inputColumns, autoincrement, isSink, write.Pos(), ctx);

    const bool useStreamIndex = isSink && kqpCtx.Config->GetEnableIndexStreamWrite();
    if (!useStreamIndex && generateColumnsIfInsert.Ref().ChildrenSize() > 0) {
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
        .IsBatch(ctx.NewAtom(write.Pos(), "false"))
        .DefaultColumns(generateColumnsIfInsert)
        .Settings(settings)
        .Done();

    return effect;
}

TExprBase BuildUpsertTableWithIndex(const TKiWriteTable& write, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement, const bool isSink, const bool isStreamIndexWrite,
    const TKikimrTableDescription& table, TExprContext& ctx)
{
    auto settings = FilterSettings(write.Settings().Ref(), {"AllowInconsistentWrites"}, ctx);
    settings = AddSetting(*settings, write.Pos(), "Mode", Build<TCoAtom>(ctx, write.Pos()).Value("upsert").Done().Ptr(), ctx);
    const auto [input, columns] = BuildWriteInput(write, table, inputColumns, autoincrement, isSink, write.Pos(), ctx);
    auto generateColumnsIfInsertNode = GetSetting(write.Settings().Ref(), "generate_columns_if_insert");
    YQL_ENSURE(generateColumnsIfInsertNode);
    TCoAtomList generateColumnsIfInsert = TCoNameValueTuple(generateColumnsIfInsertNode).Value().Cast<TCoAtomList>();

    generateColumnsIfInsert = ExtendGenerateOnInsertColumnsList(write, generateColumnsIfInsert, inputColumns, autoincrement, ctx);

    if (isSink && isStreamIndexWrite) {
        auto indexes = BuildAffectedIndexTables(table, write.Pos(), ctx, nullptr,
            [] (const TKikimrTableMetadata& meta, TPositionHandle pos, TExprContext& ctx) -> TExprBase {
                return BuildTableMeta(meta, pos, ctx);
            });
        const auto onlyStreamIndexes = std::all_of(indexes.begin(), indexes.end(), [](const auto& index) {
            return index.second->Type != TIndexDescription::EType::GlobalSyncVectorKMeansTree
                && index.second->Type != TIndexDescription::EType::GlobalFulltextPlain
                && index.second->Type != TIndexDescription::EType::GlobalFulltextRelevance;
        });

        if (onlyStreamIndexes) {
            return Build<TKqlUpsertRows>(ctx, write.Pos())
                .Table(BuildTableMeta(table, write.Pos(), ctx))
                .Input(input.Ptr())
                .Columns(columns.Ptr())
                .ReturningColumns(write.ReturningColumns())
                .IsBatch(ctx.NewAtom(write.Pos(), "false"))
                .DefaultColumns(generateColumnsIfInsert)
                .Settings(settings)
                .Done();
        }
    }

    auto effect = Build<TKqlUpsertRowsIndex>(ctx, write.Pos())
        .Table(BuildTableMeta(table, write.Pos(), ctx))
        .Input(input.Ptr())
        .Columns(columns.Ptr())
        .ReturningColumns(write.ReturningColumns())
        .GenerateColumnsIfInsert(generateColumnsIfInsert)
        .IsBatch(ctx.NewAtom(write.Pos(), "false"))
        .Settings(settings)
        .Done();

    return effect;
}

TExprBase BuildReplaceTable(const TKiWriteTable& write, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement, const bool isSink,
    const TKikimrTableDescription& table, TExprContext& ctx)
{
    auto settings = FilterSettings(write.Settings().Ref(), {"AllowInconsistentWrites"}, ctx);
    settings = AddSetting(*settings, write.Pos(), "Mode", Build<TCoAtom>(ctx, write.Pos()).Value("replace").Done().Ptr(), ctx);
    const auto [input, columns] = BuildWriteInput(write, table, inputColumns, autoincrement, isSink, write.Pos(), ctx);
    auto effect = Build<TKqlUpsertRows>(ctx, write.Pos())
        .Table(BuildTableMeta(table, write.Pos(), ctx))
        .Input(input.Ptr())
        .Columns(columns)
        .ReturningColumns(write.ReturningColumns())
        .IsBatch(ctx.NewAtom(write.Pos(), "false"))
        .DefaultColumns<TCoAtomList>().Build()
        .Settings(settings)
        .Done();

    return effect;
}

TExprBase BuildReplaceTableWithIndex(const TKiWriteTable& write, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement, const bool isSink,
    const TKikimrTableDescription& table, TExprContext& ctx)
{
    auto settings = FilterSettings(write.Settings().Ref(), {"AllowInconsistentWrites"}, ctx);
    settings = AddSetting(*settings, write.Pos(), "Mode", Build<TCoAtom>(ctx, write.Pos()).Value("replace").Done().Ptr(), ctx);
    const auto [input, columns] = BuildWriteInput(write, table, inputColumns, autoincrement, isSink, write.Pos(), ctx);
    auto effect = Build<TKqlUpsertRowsIndex>(ctx, write.Pos())
        .Table(BuildTableMeta(table, write.Pos(), ctx))
        .Input(input.Ptr())
        .Columns(columns.Ptr())
        .ReturningColumns(write.ReturningColumns())
        .GenerateColumnsIfInsert<TCoAtomList>().Build()
        .IsBatch(ctx.NewAtom(write.Pos(), "false"))
        .Settings(settings)
        .Done();

    return effect;
}

TExprBase BuildInsertTable(const TKiWriteTable& write, bool abort, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement, const bool isSink,
    const TKikimrTableDescription& table, TExprContext& ctx)
{
    auto settings = FilterSettings(write.Settings().Ref(), {"AllowInconsistentWrites"}, ctx);
    settings = AddSetting(*settings, write.Pos(), "Mode", Build<TCoAtom>(ctx, write.Pos()).Value("insert").Done().Ptr(), ctx);
    const auto [input, columns] = BuildWriteInput(write, table, inputColumns, autoincrement, isSink, write.Pos(), ctx);
    auto effect = Build<TKqlInsertRows>(ctx, write.Pos())
        .Table(BuildTableMeta(table, write.Pos(), ctx))
        .Input(input.Ptr())
        .Columns(columns)
        .ReturningColumns(write.ReturningColumns())
        .OnConflict()
            .Value(abort ? "abort"sv : "revert"sv)
            .Build()
        .Settings(settings)
        .Done();

    return effect;
}

TExprBase BuildInsertTableWithIndex(const TKiWriteTable& write, bool abort, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement, const bool isSink,
    const TKikimrTableDescription& table, TExprContext& ctx)
{
    auto settings = FilterSettings(write.Settings().Ref(), {"AllowInconsistentWrites"}, ctx);
    settings = AddSetting(*settings, write.Pos(), "Mode", Build<TCoAtom>(ctx, write.Pos()).Value("insert").Done().Ptr(), ctx);
    const auto [input, columns] = BuildWriteInput(write, table, inputColumns, autoincrement, isSink, write.Pos(), ctx);
    return Build<TKqlInsertRowsIndex>(ctx, write.Pos())
        .Table(BuildTableMeta(table, write.Pos(), ctx))
        .Input(input.Ptr())
        .Columns(columns.Ptr())
        .ReturningColumns(write.ReturningColumns())
        .OnConflict()
            .Value(abort ? "abort"sv : "revert"sv)
            .Build()
        .Settings(settings)
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
    const bool isSink, const bool isStreamIndexWrite, const TKikimrTableDescription& tableData, TExprContext& ctx)
{
    if (isSink) {
        auto indexes = BuildAffectedIndexTables(tableData, write.Pos(), ctx, nullptr,
            [] (const TKikimrTableMetadata& meta, TPositionHandle pos, TExprContext& ctx) -> TExprBase {
                return BuildTableMeta(meta, pos, ctx);
            });

        THashSet<TStringBuf> inputColumnsSet;
        for (const auto& column : inputColumns) {
            inputColumnsSet.emplace(column.Value());
        }

        const bool needToUpdateIndex = std::any_of(
            std::begin(indexes), std::end(indexes),
            [&] (const auto& index) {
                return std::any_of(std::begin(index.second->KeyColumns), std::end(index.second->KeyColumns),
                        [&] (const auto& column) { return !tableData.GetKeyColumnIndex(column) && inputColumnsSet.contains(column); })
                   ||  std::any_of(std::begin(index.second->DataColumns), std::end(index.second->DataColumns),
                        [&] (const auto& column) { return inputColumnsSet.contains(column); });
            });

        if (!needToUpdateIndex) {
            // If indexes don't need to be updated, we can use update without lookup.
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
    }


    return Build<TKqlUpdateRowsIndex>(ctx, write.Pos())
        .Table(BuildTableMeta(tableData, write.Pos(), ctx))
        .Input<TKqpWriteConstraint>()
            .Input(write.Input())
            .Columns(GetPgNotNullColumns(tableData, write.Pos(), ctx))
        .Build()
        .Columns(inputColumns)
        .ReturningColumns(write.ReturningColumns())
        .IsBatch(ctx.NewAtom(write.Pos(), "false"))
        .Settings(IsUpdateSetting(isStreamIndexWrite, ctx, write.Pos()))
        .Done();
}

TExprBase BuildDeleteTable(const TKiWriteTable& write, const TKikimrTableDescription& tableData, TExprContext& ctx) {
    const auto keysToDelete = ProjectColumns(write.Input(), tableData.Metadata->KeyColumnNames, ctx);

    return Build<TKqlDeleteRows>(ctx, write.Pos())
        .Table(BuildTableMeta(tableData, write.Pos(), ctx))
        .Input(keysToDelete)
        .ReturningColumns(write.ReturningColumns())
        .IsBatch(ctx.NewAtom(write.Pos(), "false"))
        .Done();
}

TExprBase BuildDeleteTableWithIndex(const TKiWriteTable& write, const TKikimrTableDescription& tableData, TExprContext& ctx) {
    const auto keysToDelete = ProjectColumns(write.Input(), tableData.Metadata->KeyColumnNames, ctx);

    return Build<TKqlDeleteRowsIndex>(ctx, write.Pos())
        .Table(BuildTableMeta(tableData, write.Pos(), ctx))
        .Input(keysToDelete)
        .ReturningColumns(write.ReturningColumns())
        .IsBatch(ctx.NewAtom(write.Pos(), "false"))
        .Done();
}

TExprBase BuildRowsToDelete(const TKikimrTableDescription& tableData, bool withSystemColumns,
    const TCoLambda& filter, const TCoAtom& isBatch, const TPositionHandle pos, TExprContext& ctx)
{
    const auto tableMeta = BuildTableMeta(tableData, pos, ctx);
    const auto tableColumns = BuildColumnsList(tableData, pos, ctx, withSystemColumns, true /*ignoreWriteOnlyColumns*/);

    const auto allRows = BuildReadTable(tableColumns, pos, tableData, (isBatch == "true"), {}, ctx);

    return Build<TCoFilter>(ctx, pos)
        .Input(allRows)
        .Lambda(filter)
        .Done();
}

TExprBase BuildDeleteTable(const TKiDeleteTable& del, const TKikimrTableDescription& tableData, bool withSystemColumns,
    TExprContext& ctx)
{
    auto rowsToDelete = BuildRowsToDelete(tableData, withSystemColumns, del.Filter(), del.IsBatch(), del.Pos(), ctx);
    auto keysToDelete = ProjectColumns(rowsToDelete, tableData.Metadata->KeyColumnNames, ctx);

    return Build<TKqlDeleteRows>(ctx, del.Pos())
        .Table(BuildTableMeta(tableData, del.Pos(), ctx))
        .Input(keysToDelete)
        .ReturningColumns<TCoAtomList>().Build()
        .IsBatch(del.IsBatch())
        .Settings(IsConditionalDeleteSetting(ctx, del.Pos()))
        .Done();
}

TExprBase BuildDeleteTableWithIndex(const TKiDeleteTable& del, const TKikimrTableDescription& tableData,
    bool withSystemColumns, TExprContext& ctx)
{
    TKqpDeleteRowsIndexSettings settings;
    settings.SkipLookup = true;
    auto rowsToDelete = BuildRowsToDelete(tableData, withSystemColumns, del.Filter(), del.IsBatch(), del.Pos(), ctx);
    return Build<TKqlDeleteRowsIndex>(ctx, del.Pos())
        .Table(BuildTableMeta(tableData, del.Pos(), ctx))
        .Input(rowsToDelete)
        .ReturningColumns(del.ReturningColumns())
        .IsBatch(del.IsBatch())
        .Settings(settings.BuildNode(ctx, del.Pos()))
        .Done();
}

TExprBase BuildRowsToUpdate(const TKikimrTableDescription& tableData, bool withSystemColumns, const TCoLambda& filter,
    const TCoAtom& isBatch, const TPositionHandle pos, TExprContext& ctx)
{
    auto kqlReadTable = BuildReadTable(BuildColumnsList(tableData, pos, ctx, withSystemColumns, true /*ignoreWriteOnlyColumns*/), pos, tableData, (isBatch == "true"), {}, ctx);

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
    bool withSystemColumns, TExprContext& ctx)
{
    auto rowsToUpdate = BuildRowsToUpdate(tableData, withSystemColumns, update.Filter(), update.IsBatch(), update.Pos(), ctx);

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
        .IsBatch(update.IsBatch())
        .DefaultColumns<TCoAtomList>().Build()
        .Settings(IsConditionalUpdateSetting(false, ctx, update.Pos()))
        .ReturningColumns(update.ReturningColumns())
        .Done();
}

TExprBase BuildUpdateTableWithIndex(const TKiUpdateTable& update, const TKikimrTableDescription& tableData,
    bool withSystemColumns, TExprContext& ctx, TKqpOptimizeContext& kqpCtx)
{
    auto rowsToUpdate = BuildRowsToUpdate(tableData, withSystemColumns, update.Filter(), update.IsBatch(), update.Pos(), ctx);

    TVector<TExprBase> effects;

    auto updateColumns = GetUpdateColumns(tableData, update.Update());

    auto updatedRows = BuildUpdatedRows(rowsToUpdate, update.Update(), updateColumns, update.Pos(), ctx);

    TVector<TCoAtom> updateColumnsList;

    for (const auto& column : updateColumns) {
        updateColumnsList.push_back(TCoAtom(ctx.NewAtom(update.Pos(), column)));
    }

    auto indexes = BuildAffectedIndexTables(tableData, update.Pos(), ctx, nullptr,
        [] (const TKikimrTableMetadata& meta, TPositionHandle pos, TExprContext& ctx) -> TExprBase {
            return BuildTableMeta(meta, pos, ctx);
        });

    // Rewrite UPDATE to UPDATE ON for complex indexes
    auto idxNeedsKqpEffect = [](std::pair<TExprNode::TPtr, const TIndexDescription*>& x) {
        switch (x.second->Type) {
            case TIndexDescription::EType::GlobalSync:
            case TIndexDescription::EType::GlobalAsync:
                return false;
            case TIndexDescription::EType::GlobalSyncUnique:
            case TIndexDescription::EType::GlobalSyncVectorKMeansTree:
            case TIndexDescription::EType::GlobalFulltextPlain:
            case TIndexDescription::EType::GlobalFulltextRelevance:
                return true;
        }
    };
    const bool needsKqpEffect = std::find_if(indexes.begin(), indexes.end(), idxNeedsKqpEffect) != indexes.end();

    const bool isSink = NeedSinks(tableData, kqpCtx);

    const bool useStreamIndex = isSink && kqpCtx.Config->GetEnableIndexStreamWrite();

    // For unique or vector index rewrite UPDATE to UPDATE ON
    if (needsKqpEffect || useStreamIndex) {
        return Build<TKqlUpdateRowsIndex>(ctx, update.Pos())
            .Table(BuildTableMeta(tableData, update.Pos(), ctx))
            .Input<TKqpWriteConstraint>()
                .Input(updatedRows)
                .Columns(GetPgNotNullColumns(tableData, update.Pos(), ctx))
            .Build()
            .Columns<TCoAtomList>()
                .Add(updateColumnsList)
                .Build()
            .ReturningColumns<TCoAtomList>().Build()
            .IsBatch(update.IsBatch())
            .Settings(IsConditionalUpdateSetting(useStreamIndex, ctx, update.Pos()))
            .Done();
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
        .IsBatch(update.IsBatch())
        .DefaultColumns<TCoAtomList>().Build()
        .Settings(IsConditionalUpdateSetting(false, ctx, update.Pos()))
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
                .ReturningColumns<TCoAtomList>().Build()
                .IsBatch(ctx.NewAtom(update.Pos(), "false"))
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
                .IsBatch(ctx.NewAtom(update.Pos(), "false"))
                .DefaultColumns<TCoAtomList>().Build()
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
    Y_UNUSED(kqpCtx);
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

        return BuildReadTableIndex(read, tableData, indexName, withSystemColumns, ctx).Ptr();
    }

    return BuildReadTable(read, tableData, view && view->PrimaryFlag, withSystemColumns, ctx).Ptr();
}

TExprBase WriteTableSimple(const TKiWriteTable& write, const TCoAtomList& inputColumns,
    const TCoAtomList& autoincrement,
    const TKikimrTableDescription& tableData, TExprContext& ctx, TKqpOptimizeContext& kqpCtx, const bool isSink)
{
    Y_UNUSED(isSink);
    auto op = GetTableOp(write);
    switch (op) {
        case TYdbOperation::Upsert:
            return BuildUpsertTable(write, inputColumns, autoincrement, isSink, tableData, ctx, kqpCtx);
        case TYdbOperation::Replace:
            return BuildReplaceTable(write, inputColumns, autoincrement, isSink, tableData, ctx);
        case TYdbOperation::InsertAbort:
        case TYdbOperation::InsertRevert:
            return BuildInsertTable(write, op == TYdbOperation::InsertAbort, inputColumns, autoincrement, isSink, tableData, ctx);
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
    const TKikimrTableDescription& tableData, TExprContext& ctx, const bool isSink, const bool isStreamIndexWrite)
{
    Y_UNUSED(isSink);
    auto op = GetTableOp(write);
    switch (op) {
        case TYdbOperation::Upsert:
            return BuildUpsertTableWithIndex(write, inputColumns, autoincrement, isSink, isStreamIndexWrite, tableData, ctx);
        case TYdbOperation::Replace:
            return BuildReplaceTableWithIndex(write, inputColumns, autoincrement, isSink, tableData, ctx);
        case TYdbOperation::InsertAbort:
        case TYdbOperation::InsertRevert:
            return BuildInsertTableWithIndex(write, op == TYdbOperation::InsertAbort, inputColumns, autoincrement, isSink, tableData, ctx);
        case TYdbOperation::UpdateOn:
            return BuildUpdateOnTableWithIndex(write, inputColumns, isSink, isStreamIndexWrite, tableData, ctx);
        case TYdbOperation::DeleteOn:
            return BuildDeleteTableWithIndex(write, tableData, ctx);
        default:
            YQL_ENSURE(false, "Unsupported table operation: " << (ui32)op << ", table: " << tableData.Metadata->Name);
    }

    Y_UNREACHABLE();
}

bool CheckWriteToIndex(const TExprBase& write, const NYql::TKikimrTableDescription& tableData, TExprContext& ctx) {
    if (tableData.Metadata->IsIndexImplTable) {
        const TString err = TStringBuilder() << "Writing to index implementation tables is not allowed. Table: `"
            << tableData.Metadata->Name << "`.";
        ctx.AddError(YqlIssue(ctx.GetPosition(write.Pos()), TIssuesIds::KIKIMR_BAD_REQUEST, err));
        return false;
    }
    return true;
}

bool CheckDisabledWriteToUniqIndex(const TExprBase& write, const NYql::TKikimrTableDescription& tableData, TExprContext& ctx) {
    if (tableData.Metadata->WritesToTableAreDisabled) {
        const TString err = TStringBuilder() << "Table `" << tableData.Metadata->Name << "` modification is disabled: "
                << tableData.Metadata->DisableWritesReason;
        ctx.AddError(YqlIssue(ctx.GetPosition(write.Pos()), TIssuesIds::KIKIMR_BAD_REQUEST, err));
        return false;
    }
    return true;
}

bool ValidateBatchOperation(const NYql::TKikimrTableDescription& tableData, const TExprBase& expr, TExprContext& ctx, TKqpOptimizeContext& kqpCtx)
{
    const bool allowBatchUpdates = kqpCtx.Config->GetEnableBatchUpdates() && kqpCtx.Config->GetEnableOltpSink();
    const bool enabledIndexStreamWrite = kqpCtx.Config->GetEnableIndexStreamWrite();

    if (!allowBatchUpdates) {
        const TString err = "BATCH operations are not supported at the current time.";
        ctx.AddError(YqlIssue(ctx.GetPosition(expr.Pos()), TIssuesIds::KIKIMR_PRECONDITION_FAILED, err));
        return false;
    }

    for (const auto& index : tableData.Metadata->Indexes) {
        if (!NBatchOperations::IsIndexSupported(index.Type, enabledIndexStreamWrite)) {
            const TString err = "BATCH operations are not supported for tables with " + IndexTypeToName(index.Type) + " indexes (index: `" + index.Name + "`).";
            ctx.AddError(YqlIssue(ctx.GetPosition(expr.Pos()), NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED, err));
            return false;
        }
    }
    return true;
}

TExprNode::TPtr HandleWriteTable(const TKiWriteTable& write, TExprContext& ctx, TKqpOptimizeContext& kqpCtx, const TKikimrTablesData& tablesData)
{
    if (GetTableOp(write) == TYdbOperation::FillTable) {
        return BuildFillTable(write, ctx).Ptr();
    }
    auto& tableData = GetTableData(tablesData, write.DataSink().Cluster(), write.Table().Value());
    if (!CheckWriteToIndex(write, tableData, ctx) || !CheckDisabledWriteToUniqIndex(write, tableData, ctx)) {
        return nullptr;
    }
    const bool isSink = NeedSinks(tableData, kqpCtx);

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
        const bool isStreamIndexWrite = kqpCtx.Config->GetEnableIndexStreamWrite();
        return WriteTableWithIndexUpdate(write, inputColumns, defaultConstraintColumns, tableData, ctx, isSink, isStreamIndexWrite).Ptr();
    } else {
        return WriteTableSimple(write, inputColumns, defaultConstraintColumns, tableData, ctx, kqpCtx, isSink).Ptr();
    }
}

TExprNode::TPtr HandleUpdateTable(const TKiUpdateTable& update, TExprContext& ctx, TKqpOptimizeContext& kqpCtx,
    const TKikimrTablesData& tablesData, bool withSystemColumns)
{
    const auto& tableData = GetTableData(tablesData, update.DataSink().Cluster(), update.Table().Value());
    if (!CheckWriteToIndex(update, tableData, ctx) || !CheckDisabledWriteToUniqIndex(update, tableData, ctx)) {
        return nullptr;
    }

    if (update.IsBatch() == "true" && !ValidateBatchOperation(tableData, update, ctx, kqpCtx)) {
        return nullptr;
    }

    if (HasIndexesToWrite(tableData)) {
        return BuildUpdateTableWithIndex(update, tableData, withSystemColumns, ctx, kqpCtx).Ptr();
    } else {
        return BuildUpdateTable(update, tableData, withSystemColumns, ctx).Ptr();
    }
}

TExprNode::TPtr HandleDeleteTable(const TKiDeleteTable& del, TExprContext& ctx, TKqpOptimizeContext& kqpCtx,
    const TKikimrTablesData& tablesData, bool withSystemColumns)
{
    auto& tableData = GetTableData(tablesData, del.DataSink().Cluster(), del.Table().Value());
    if (!CheckWriteToIndex(del, tableData, ctx) || !CheckDisabledWriteToUniqIndex(del, tableData, ctx)) {
        return nullptr;
    }

    if (del.IsBatch() == "true" && !ValidateBatchOperation(tableData, del, ctx, kqpCtx)) {
        return nullptr;
    }

    if (HasIndexesToWrite(tableData)) {
        return BuildDeleteTableWithIndex(del, tableData, withSystemColumns, ctx).Ptr();
    } else {
        return BuildDeleteTable(del, tableData, withSystemColumns, ctx).Ptr();
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
    const auto& [indexMeta, _ ] = tableDesc.Metadata->GetIndexMetadata(read.Index().Value());
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
                auto writeOp = HandleWriteTable(maybeWrite.Cast(), ctx, *kqpCtx, tablesData);
                if (!writeOp) {
                    return {};
                }

                kqlEffects.push_back(TExprBase(writeOp));
            }

            if (auto maybeUpdate = effect.Maybe<TKiUpdateTable>()) {
                auto updateOp = HandleUpdateTable(maybeUpdate.Cast(), ctx, *kqpCtx, tablesData, withSystemColumns);
                if (!updateOp) {
                    return {};
                }
                kqlEffects.push_back(TExprBase(updateOp));
            }

            if (auto maybeDelete = effect.Maybe<TKiDeleteTable>()) {
                auto deleteOp = HandleDeleteTable(maybeDelete.Cast(), ctx, *kqpCtx, tablesData, withSystemColumns);
                if (!deleteOp) {
                    return {};
                }
                kqlEffects.push_back(TExprBase(deleteOp));
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

                    if (auto del = effect.Maybe<TKiDeleteTable>()) {
                        cluster = del.Cast().DataSink().Cluster();
                        table = del.Cast().Table().Value();
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
                            IDqIntegration::TWrapReadSettings wrSettings;
                            if (kqpCtx->Config->GetEnableWatermarks()) {
                                wrSettings = {
                                    .WatermarksMode = "default",
                                };
                            }
                            auto newRead = dqIntegration->WrapRead(input.Cast().Ptr(), ctx, wrSettings);
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
