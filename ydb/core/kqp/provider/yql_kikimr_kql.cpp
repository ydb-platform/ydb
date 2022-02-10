#include "yql_kikimr_provider_impl.h"
#include "kqp_opt_helpers.h"

#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NYql {
namespace {

using namespace NNodes;
using namespace NKikimr::NKqp;

TCoNameValueTupleList CreateSecondaryIndexKeyTuples(TCoArgument itemArg, const TVector<TString>& keyColumnNames,
    const THashSet<TStringBuf>& inputColumns, const TKikimrTableDescription& table,
    const TExprBase& fetch, bool nullExtension, TExprContext& ctx)
{
    TVector<TExprBase> keyTuples;
    THashSet<TString> uniqColumns;
    for (const TString& name : keyColumnNames) {
        // skip already added columns
        if (!uniqColumns.insert(name).second) {
            continue;
        }
        if (inputColumns.contains(name)) {
            const auto& tuple = Build<TCoNameValueTuple>(ctx, itemArg.Pos())
                .Name().Build(name)
                .Value<TCoMember>()
                    .Struct(itemArg)
                    .Name().Build(name)
                    .Build()
                .Done();

            keyTuples.push_back(tuple);
        } else {
            if (nullExtension) {
                const auto& type = table.GetColumnType(name);
                const auto& member = Build<TCoNameValueTuple>(ctx, itemArg.Pos())
                    .Name().Build(name)
                    .template Value<TCoNothing>()
                         .OptionalType(NCommon::BuildTypeExpr(itemArg.Pos(), *type, ctx))
                         .Build()
                    .Done();
                keyTuples.emplace_back(TExprBase(member));
            } else {
                const auto& member = Build<TCoNameValueTuple>(ctx, itemArg.Pos())
                    .Name().Build(name)
                    .Value<TCoMember>()
                        .Struct(fetch)
                        .Name().Build(name)
                        .Build()
                    .Done();
                keyTuples.emplace_back(TExprBase(member));
            }
        }
    }
    return Build<TCoNameValueTupleList>(ctx, itemArg.Pos())
        .Add(keyTuples)
        .Done();
}

TVector<std::pair<TExprNode::TPtr, const TIndexDescription*>> BuildSecondaryIndexVector(
    const TKikimrTableDescription& table,
    TPositionHandle pos,
    TExprContext& ctx,
    const THashSet<TStringBuf>* filter = nullptr)
{
    return NKikimr::NKqp::BuildSecondaryIndexVector(table, pos, ctx, filter, &BuildVersionedTable);
}

TExprBase BuildConditionalErase(
    const TExprBase& condition,
    const TCoArgument& itemArg,
    const TVector<TExprBase>& keyToErase,
    TExprNode::TPtr table,
    const TKiWriteTable& node,
    TExprContext& ctx)
{
    const auto& erase = Build<TKiEraseRow>(ctx, node.Pos())
        .Cluster(node.DataSink().Cluster())
        .Table(table)
        .Key<TCoNameValueTupleList>()
            .Add(keyToErase)
            .Build()
        .Done();

    return Build<TCoIfPresent>(ctx, node.Pos())
        .Optional(condition)
        .PresentHandler<TCoLambda>()
            .Args(itemArg)
            .Body(erase)
            .Build()
        .MissingValue<TCoVoid>().Build()
        .Done();
}

TExprBase CreateUpdateRowWithSecondaryIndex(
    const TKiUpdateRow& updateTable,
    const TVector<std::pair<TExprNode::TPtr, const TIndexDescription*>> indexes,
    const TKikimrTableDescription& table,
    const TCoNameValueTupleList& tablePk,
    const THashSet<TStringBuf>& inputColumns,
    const TCoArgument& itemArg,
    const TKiWriteTable& node, TExprContext& ctx, bool replace, bool skipErase)
{
    TVector<TExprBase> updates;
    updates.push_back(updateTable);

    const TVector<TString>& pk = table.Metadata->KeyColumnNames;
    // in case of replace mode no need to read old data columns
    const auto dataColumnSet = replace ? THashSet<TString>() : CreateDataColumnSetToRead(indexes, inputColumns);
    const auto columnsToSelect = NKikimr::NKqp::CreateColumnsToSelectToUpdateIndex(indexes, pk, dataColumnSet, node.Pos(), ctx);

    const TExprBase& fetch = Build<TKiSelectRow>(ctx, node.Pos())
        .Cluster(node.DataSink().Cluster())
        .Table(updateTable.Table())
        .Key(tablePk)
        .template Select<TCoAtomList>()
            .Add(columnsToSelect)
            .Build()
        .Done();

    for (const auto& pair : indexes) {
        TVector<TString> indexTablePk;
        TVector<TCoNameValueTuple> dataColumnUpdates;

        // skip update of index table if indexed column not present in request
        // and upserted rows already present
        bool mayBeSkipIndexUpdate = !replace;
        indexTablePk.reserve(pair.second->KeyColumns.size() + pk.size());
        for (const auto& col : pair.second->KeyColumns) {
            indexTablePk.push_back(col);
            if (inputColumns.contains(col)) {
                mayBeSkipIndexUpdate = false;
            }
        }
        indexTablePk.insert(indexTablePk.end(), pk.begin(), pk.end());

        // We can`t skip index update if data column present in query
        // but in this case we may be need to skip erace
        bool mustUpdateDataColumn = false;

        for (const auto& col : pair.second->DataColumns) {
            if (inputColumns.contains(col)) {
                mustUpdateDataColumn = true;
                auto tuple = Build<TCoNameValueTuple>(ctx, node.Pos())
                    .Name().Build(col)
                    .Value<TCoMember>()
                        .Struct(itemArg)
                        .Name().Build(col)
                        .Build()
                    .Done();
                dataColumnUpdates.push_back(tuple);
            } else if (!replace) {
                // Index table has data column, but data column has not been
                // specifyed in request
                const auto& tuple = Build<TCoNameValueTuple>(ctx, itemArg.Pos())
                    .Name().Build(col)
                    .Value<TCoMember>()
                        .Struct(fetch)
                        .Name().Build(col)
                    .Build()
                    .Done();
                dataColumnUpdates.push_back(tuple);
            }
        }

        TExprBase updateRowSecondaryIndex = Build<TKiUpdateRow>(ctx, node.Pos())
            .Cluster(node.DataSink().Cluster())
            .Table(pair.first)
            .Key(CreateSecondaryIndexKeyTuples(
                itemArg,
                indexTablePk,
                inputColumns,
                table,
                fetch,
                replace || skipErase,
                ctx))
            .Update<TCoNameValueTupleList>()
                .Add(dataColumnUpdates)
                .Build()
            .Done();

        if (!mustUpdateDataColumn && mayBeSkipIndexUpdate) {
            updateRowSecondaryIndex = Build<TCoIf>(ctx, node.Pos())
                .Predicate<TCoHasItems>()
                    .List<TCoToList>()
                        .Optional(fetch)
                        .Build()
                    .Build()
                .ThenValue<TCoVoid>()
                    .Build()
                .ElseValue(updateRowSecondaryIndex)
            .Done();
        }

        TVector<TExprBase> keyToErase;
        keyToErase.reserve(pair.second->KeyColumns.size() + pk.size());
        if (!skipErase && !mayBeSkipIndexUpdate) {
            THashSet<TString> uniqColumns;
            for (const auto& col : pair.second->KeyColumns) {
                if (uniqColumns.insert(col).second) {
                    const auto& member = Build<TCoNameValueTuple>(ctx, node.Pos())
                        .Name().Build(col)
                        .Value<TCoMember>()
                            .Struct(fetch)
                            .Name().Build(col)
                            .Build()
                        .Done();
                    keyToErase.emplace_back(TExprBase(member));
                }
            }

            for (const auto& k : pk) {
                if (uniqColumns.insert(k).second) {
                    const auto& member = Build<TCoNameValueTuple>(ctx, node.Pos())
                        .Name().Build(k)
                        .Value<TCoMember>()
                            .Struct(fetch)
                            .Name().Build(k)
                            .Build()
                        .Done();
                     keyToErase.emplace_back(TExprBase(member));
                }
            }

            const auto& conditionalErase = BuildConditionalErase(
                fetch, itemArg, keyToErase, pair.first, node, ctx);
            updates.push_back(conditionalErase);
        }
        updates.push_back(updateRowSecondaryIndex);
    }

    return Build<TCoAsList>(ctx, node.Pos())
        .Add(updates)
        .Done();
}

TExprNode::TPtr KiUpsertTableToKql(const TKiWriteTable& node, TExprContext& ctx, const TKikimrTableDescription& table,
    bool replace, bool skipErase, TExprNode::TPtr& effect)
{
    auto itemArg = Build<TCoArgument>(ctx, node.Pos())
        .Name("item")
        .Done();

    auto inputColumnsSetting = GetSetting(node.Settings().Ref(), "input_columns");
    YQL_ENSURE(inputColumnsSetting);

    THashSet<TStringBuf> inputColumns;
    for (const auto& atom : TCoNameValueTuple(inputColumnsSetting).Value().Cast<TCoAtomList>()) {
        inputColumns.insert(atom.Value());
    }

    const auto& secondaryIndexes = BuildSecondaryIndexVector(table, node.Pos(), ctx);

    TVector<TCoNameValueTuple> valueTuples;

    const auto versionedTable = BuildVersionedTable(*table.Metadata, node.Pos(), ctx);
    YQL_ENSURE(versionedTable.Path() == node.Table());

    for (auto& pair : table.Metadata->Columns) {
        const TString& name = pair.first;

        if (table.GetKeyColumnIndex(name)) {
            continue;
        }

        if (inputColumns.contains(name)) {
            auto tuple = Build<TCoNameValueTuple>(ctx, node.Pos())
                .Name().Build(name)
                .Value<TCoMember>()
                    .Struct(itemArg)
                    .Name().Build(name)
                    .Build()
                .Done();
            valueTuples.push_back(tuple);
        } else if (replace) {
            auto type = table.GetColumnType(name);
            YQL_ENSURE(type, "No such column: " << name); 
            auto tuple = Build<TCoNameValueTuple>(ctx, node.Pos())
                .Name().Build(name)
                .Value<TCoNothing>()
                    .OptionalType(NCommon::BuildTypeExpr(node.Pos(), *type, ctx))
                    .Build()
                .Done();
            valueTuples.push_back(tuple);
        }
    }

    const auto& tablePk = ExtractNamedKeyTuples(itemArg, table, ctx);

    // Update for main table
    const auto& tableUpdate = Build<TKiUpdateRow>(ctx, node.Pos())
        .Cluster(node.DataSink().Cluster())
        .Table(versionedTable)
        .Key(tablePk)
        .Update<TCoNameValueTupleList>()
            .Add(valueTuples)
            .Build()
        .Done();

    const auto& input = (skipErase || secondaryIndexes.empty()) ? node.Input() : RemoveDuplicateKeyFromInput(node.Input(), table, node.Pos(), ctx);

    effect = Build<TCoFlatMap>(ctx, node.Pos())
            .Input(input)
                .Lambda<TCoLambda>()
                    .Args({itemArg})
                    .Body(CreateUpdateRowWithSecondaryIndex(
                        tableUpdate,
                        secondaryIndexes,
                        table,
                        tablePk,
                        inputColumns,
                        itemArg,
                        node,
                        ctx,
                        replace,
                        skipErase)
                    )
                .Build()
            .Done()
            .Ptr();

    return Build<TCoWorld>(ctx, node.Pos())
        .Done()
        .Ptr();
}

TExprNode::TPtr KiInsertTableToKql(const TKiWriteTable& node, TExprContext& ctx, const TKikimrTableDescription& table,
    const TYdbOperation& op, TExprNode::TPtr& effect) 
{
    auto fetchItemArg = Build<TCoArgument>(ctx, node.Pos())
        .Name("fetchItem")
        .Done();

    const auto versionedTable = BuildVersionedTable(*table.Metadata, node.Pos(), ctx);
    YQL_ENSURE(versionedTable.Path() == node.Table());

    auto fetchLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args(fetchItemArg)
        .Body<TKiSelectRow>()
            .Cluster(node.DataSink().Cluster())
            .Table(versionedTable)
            .Key(ExtractNamedKeyTuples(fetchItemArg, table, ctx))
            .Select()
                .Build()
            .Build()
        .Done();

    auto getKeyListItemArg = Build<TCoArgument>(ctx, node.Pos())
        .Name("getKeyListItem")
        .Done();

    auto keyList = Build<TCoMap>(ctx, node.Pos())
        .Input(node.Input())
        .Lambda()
            .Args(getKeyListItemArg)
            .Body(ExtractKeys(getKeyListItemArg, table, ctx))
            .Build()
        .Done();

    auto duplicatesPredicate = Build<TCoCmpNotEqual>(ctx, node.Pos())
        .Left<TCoLength>()
            .List(keyList)
            .Build()
        .Right<TCoLength>()
            .List<TCoToDict>()
                .List(keyList)
                .KeySelector()
                    .Args({"item"})
                    .Body("item")
                    .Build()
                .PayloadSelector()
                    .Args({"item"})
                    .Body<TCoVoid>().Build()
                    .Build()
                .Settings()
                    .Add().Build("One")
                    .Add().Build("Hashed")
                    .Build()
                .Build()
            .Build()
        .Done();

    auto fetchPredicate = Build<TCoHasItems>(ctx, node.Pos())
        .List<TCoFlatMap>()
            .Input(node.Input())
            .Lambda(fetchLambda)
            .Build()
        .Done();

    auto predicate = Build<TCoOr>(ctx, node.Pos())
        .Add({duplicatesPredicate, fetchPredicate})
        .Done();

    TExprNode::TPtr insertEffect;
    auto insertKql = KiUpsertTableToKql(node, ctx, table, false, true, insertEffect);

    if (op == TYdbOperation::InsertAbort) { 
        effect = Build<TKiAbortIf>(ctx, node.Pos())
            .Predicate(predicate)
            .Effect(insertEffect)
            .Constraint().Build("insert_pk")
            .Done()
            .Ptr();
    } else if (op == TYdbOperation::InsertRevert) { 
        effect = Build<TKiRevertIf>(ctx, node.Pos())
            .Predicate(predicate)
            .Effect(insertEffect)
            .Constraint().Build("insert_pk")
            .Done()
            .Ptr();
    } else {
        YQL_ENSURE(false, "Unexpected table operation");
    }

    return insertKql;
}

TExprNode::TPtr KiDeleteOnTableToKql(const TKiWriteTable& node, TExprContext& ctx,
    const TKikimrTableDescription& table, TExprNode::TPtr& effect)
{
    auto itemArg = Build<TCoArgument>(ctx, node.Pos())
        .Name("item")
        .Done();

    TVector<TExprBase> updates;

    const auto& tablePk = ExtractNamedKeyTuples(itemArg, table, ctx);
    const TVector<TString>& pk = table.Metadata->KeyColumnNames;

    const auto versionedTable = BuildVersionedTable(*table.Metadata, node.Pos(), ctx);
    YQL_ENSURE(versionedTable.Path() == node.Table());

    updates.emplace_back(Build<TKiEraseRow>(ctx, node.Pos())
        .Cluster(node.DataSink().Cluster())
        .Table(versionedTable)
        .Key(tablePk)
        .Done()
    );

    const auto& indexes = BuildSecondaryIndexVector(table, node.Pos(), ctx);

    if (indexes) {
        // No need to read dataColumn - we just going to remove row
        const THashSet<TString> dummyDataColumns;
        const auto& columnsToSelect = NKikimr::NKqp::CreateColumnsToSelectToUpdateIndex(indexes, pk, dummyDataColumns, node.Pos(), ctx);
        const TExprBase& fetch = Build<TKiSelectRow>(ctx, node.Pos())
            .Cluster(node.DataSink().Cluster())
            .Table(versionedTable)
            .Key(tablePk)
            .template Select<TCoAtomList>()
                .Add(columnsToSelect)
                .Build()
            .Done();

        for (const auto& pair : indexes) {

            TVector<TExprBase> keyToErase;
            keyToErase.reserve(pair.second->KeyColumns.size() + pk.size());

            THashSet<TString> uniqColumns;
            for (const auto& col : pair.second->KeyColumns) {
                if (!uniqColumns.insert(col).second) {
                    continue;
                }
                const auto& member = Build<TCoNameValueTuple>(ctx, node.Pos())
                    .Name().Build(col)
                    .Value<TCoMember>()
                        .Struct(fetch)
                        .Name().Build(col)
                        .Build()
                    .Done();
                keyToErase.emplace_back(TExprBase(member));
            }

            for (const auto& k : pk) {
                if (!uniqColumns.insert(k).second) {
                    continue;
                }
                const auto& member = Build<TCoNameValueTuple>(ctx, node.Pos())
                    .Name().Build(k)
                    .Value<TCoMember>()
                        .Struct(fetch)
                        .Name().Build(k)
                        .Build()
                    .Done();
                 keyToErase.emplace_back(TExprBase(member));
            }

            const auto& erase = Build<TKiEraseRow>(ctx, node.Pos())
                .Cluster(node.DataSink().Cluster())
                .Table(pair.first)
                .Key<TCoNameValueTupleList>()
                    .Add(keyToErase)
                    .Build()
                .Done();

            updates.push_back(erase);
        }

    }

    effect = Build<TCoFlatMap>(ctx, node.Pos())
        .Input(node.Input())
            .Lambda<TCoLambda>()
                .Args({itemArg})
                .Body<TCoAsList>()
                    .Add(updates)
                .Build()
            .Build()
        .Done()
        .Ptr();

    return Build<TCoWorld>(ctx, node.Pos())
        .Done()
        .Ptr();
}

TExprNode::TPtr KiReadTableToKql(TCoRight right, TExprContext& ctx, const TKikimrTablesData& tablesData, bool withSystemColumns) { 
    const auto& read = right.Input().Cast<TKiReadTable>();
    bool unwrapValues = HasSetting(read.Settings().Ref(), "unwrap_values");

    TKikimrKey key(ctx);
    YQL_ENSURE(key.Extract(read.TableKey().Ref()));
    YQL_ENSURE(key.GetKeyType() == TKikimrKey::Type::Table);
    const auto& cluster = read.DataSource().Cluster();
    const auto& table = key.GetTablePath();

    const auto& tableDesc = tablesData.ExistingTable(TString(cluster), TString(table));

    const auto versionedTable = BuildVersionedTable(*tableDesc.Metadata, read.Pos(), ctx);
    YQL_ENSURE(versionedTable.Path().Value() == table);

    TMaybe<TString> secondaryIndex;

    if (const auto& view = key.GetView()) {
        YQL_ENSURE(tableDesc.Metadata);
        if (!ValidateTableHasIndex(tableDesc.Metadata, ctx, read.Pos())) {
            return nullptr;
        }
        auto [metadata, state] = tableDesc.Metadata->GetIndexMetadata(view.GetRef());
        YQL_ENSURE(metadata, "unable to find metadta for index: " << view.GetRef());
        YQL_ENSURE(state == TIndexDescription::EIndexState::Ready
            || state == TIndexDescription::EIndexState::WriteOnly);
        if (state != TIndexDescription::EIndexState::Ready) {
            auto err = TStringBuilder()
                << "Requested index: " << view.GetRef()
                << " is not ready to use";
            ctx.AddError(YqlIssue(ctx.GetPosition(read.Pos()), TIssuesIds::KIKIMR_INDEX_IS_NOT_READY, err));
            return nullptr;
        }
        secondaryIndex = metadata->Name;
    }

    if (secondaryIndex) {
        const auto& indexTableName = secondaryIndex.GetRef();
        const auto& keyTableDesc = tablesData.ExistingTable(TString(cluster), TString(indexTableName));
        TKikimrKeyRange range(ctx, keyTableDesc);

        const auto& selectRange = Build<TKiSelectIndexRange>(ctx, read.Pos())
            .Cluster(cluster)
            .Table(versionedTable)
            .Range(range.ToRangeExpr(read, ctx))
            .Select(read.GetSelectColumns(ctx, tablesData, withSystemColumns)) 
            .Settings()
                .Build()
            .IndexName()
                .Value(secondaryIndex.GetRef())
                .Build()
            .Done();

        if (unwrapValues) {
            return UnwrapKiReadTableValues(selectRange, tableDesc, selectRange.Select(), ctx).Ptr();
        } else {
            return TExprBase(selectRange).Ptr();
        }
    } else {
        TKikimrKeyRange range(ctx, tableDesc);

        const auto& selectRange = Build<TKiSelectRange>(ctx, read.Pos())
            .Cluster(cluster)
            .Table(versionedTable)
            .Range(range.ToRangeExpr(read, ctx))
            .Select(read.GetSelectColumns(ctx, tablesData, withSystemColumns)) 
            .Settings()
                .Build()
            .Done();

        if (unwrapValues) {
            return UnwrapKiReadTableValues(selectRange, tableDesc, selectRange.Select(), ctx).Ptr();
        } else {
            return TExprBase(selectRange).Ptr();
        }
    }
}

TExprNode::TPtr KiUpdateOnTableToKql(const TKiWriteTable& node, TExprContext& ctx,
    const TKikimrTableDescription& tableDesc, TExprNode::TPtr& effect)
{
    // TODO: KIKIMR-3206
    // This function should be rewriten

    const auto& cluster = node.DataSink().Cluster();

    const auto& itemArg = Build<TCoArgument>(ctx, node.Pos())
        .Name("item")
        .Done();

    const auto& inputColumnsSetting = GetSetting(node.Settings().Ref(), "input_columns");
    YQL_ENSURE(inputColumnsSetting);

    TVector<TCoNameValueTuple> valueTuples;
    THashSet<TStringBuf> updatedColumns;
    for (const auto& atom : TCoNameValueTuple(inputColumnsSetting).Value().Cast<TCoAtomList>()) {
        if (tableDesc.GetKeyColumnIndex(TString(atom.Value()))) {
            continue;
        }

        auto tuple = Build<TCoNameValueTuple>(ctx, node.Pos())
            .Name(atom)
            .Value<TCoMember>()
                .Struct(itemArg)
                .Name(atom)
                .Build()
            .Done();
        valueTuples.push_back(tuple);
        updatedColumns.insert(atom.Value());
    }

    // Returns index only if at least one of indexed columns for coresponding index has been updated.
    const auto& indexes = BuildSecondaryIndexVector(tableDesc, node.Pos(), ctx, &updatedColumns);
    const TVector<TString>& pk = tableDesc.Metadata->KeyColumnNames;

    const auto versionedTable = BuildVersionedTable(*tableDesc.Metadata, node.Pos(), ctx);
    YQL_ENSURE(versionedTable.Path() == node.Table());

    const auto dataColumnSet = CreateDataColumnSetToRead(indexes, updatedColumns);
    const TVector<TExprBase> columnsToSelect = NKikimr::NKqp::CreateColumnsToSelectToUpdateIndex(indexes, pk, dataColumnSet, node.Pos(), ctx);
    const auto& fetch = Build<TKiSelectRow>(ctx, node.Pos())
        .Cluster(cluster)
        .Table(versionedTable)
        .Key(ExtractNamedKeyTuples(itemArg, tableDesc, ctx))
        .Select<TCoAtomList>()
            .Add(columnsToSelect)
            .Build()
        .Done();

    TVector<TExprBase> updates;

    updates.emplace_back(Build<TKiUpdateRow>(ctx, node.Pos())
            .Cluster(cluster)
            .Table(versionedTable)
            .Key(ExtractNamedKeyTuples(itemArg, tableDesc, ctx))
            .Update<TCoNameValueTupleList>()
                .Add(valueTuples)
                .Build()
            .Done()
        );

    if (indexes) {
        for (const auto& pair : indexes) {
            TVector<TString> indexTablePk;
            indexTablePk.reserve(pair.second->KeyColumns.size() + pk.size());
            for (const auto& col : pair.second->KeyColumns) {
                indexTablePk.push_back(col);
            }
            indexTablePk.insert(indexTablePk.end(), pk.begin(), pk.end());

            TVector<TExprBase> keyToAdd;
            TVector<TExprBase> keyToErase;
            TVector<TCoNameValueTuple> dataColumnUpdates;

            keyToAdd.reserve(indexTablePk.size());
            keyToErase.reserve(indexTablePk.size());
            dataColumnUpdates.reserve(pair.second->DataColumns.size());

            for (const TString& name : pair.second->DataColumns) {
                // Data column was specified in request - update it
                if (updatedColumns.contains(name)) {
                    const auto& tuple = Build<TCoNameValueTuple>(ctx, itemArg.Pos())
                        .Name().Build(name)
                        .Value<TCoMember>()
                            .Struct(itemArg)
                                .Name().Build(name)
                            .Build()
                        .Done();
                    dataColumnUpdates.push_back(tuple);
                } else {
                    const auto& tuple = Build<TCoNameValueTuple>(ctx, itemArg.Pos())
                        .Name().Build(name)
                        .Value<TCoMember>()
                            .Struct(fetch)
                            .Name().Build(name)
                        .Build()
                        .Done();
                    dataColumnUpdates.push_back(tuple);
                }
            }

            THashSet<TString> uniqColumns;
            for (const TString& name : indexTablePk) {
                if (!uniqColumns.insert(name).second) {
                    continue;
                }
                const auto& oldTuple = Build<TCoNameValueTuple>(ctx, itemArg.Pos())
                    .Name().Build(name)
                    .Value<TCoMember>()
                        .Struct(fetch)
                            .Name().Build(name)
                        .Build()
                    .Done();
                keyToErase.push_back(oldTuple);
                if (updatedColumns.contains(name)) {
                    const auto& tuple = Build<TCoNameValueTuple>(ctx, itemArg.Pos())
                        .Name().Build(name)
                        .Value<TCoMember>()
                            .Struct(itemArg)
                                .Name().Build(name)
                            .Build()
                        .Done();
                    keyToAdd.push_back(tuple);
                } else {
                    keyToAdd.push_back(oldTuple);
                }
            }

            const auto& erase = Build<TKiEraseRow>(ctx, node.Pos())
                .Cluster(cluster)
                .Table(pair.first)
                .Key<TCoNameValueTupleList>()
                    .Add(keyToErase)
                    .Build()
                .Done();

            updates.push_back(erase);

            const auto& updateRowSecondaryIndex = Build<TKiUpdateRow>(ctx, node.Pos())
                .Cluster(cluster)
                .Table(pair.first)
                .Key<TCoNameValueTupleList>()
                    .Add(keyToAdd)
                    .Build()
                .Update<TCoNameValueTupleList>()
                    .Add(dataColumnUpdates)
                    .Build()
                .Done();

            updates.push_back(updateRowSecondaryIndex);

        }
    }

    const auto& updateLambda = Build<TCoLambda>(ctx, node.Pos())
        .Args(itemArg)
        .Body<TCoIf>()
            .Predicate<TCoHasItems>()
                .List<TCoToList>()
                    .Optional(fetch)
                    .Build()
                .Build()
            .ThenValue<TCoAsList>()
                .Add(updates)
                .Build()
            .ElseValue<TCoList>()
                .ListType<TCoListType>()
                    .ItemType<TCoVoidType>()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();

    effect = Build<TCoFlatMap>(ctx, node.Pos())
        .Input(node.Input())
        .Lambda(updateLambda)
        .Done()
        .Ptr();

    return Build<TCoWorld>(ctx, node.Pos())
        .Done()
        .Ptr();
}

TExprNode::TPtr KiWriteTableToKql(TKiWriteTable write, TExprContext& ctx,
    const TKikimrTablesData& tablesData, TExprNode::TPtr& effect)
{
    auto op = GetTableOp(write);

    auto cluster = write.DataSink().Cluster().Value();
    auto table = write.Table().Value();
    auto& tableDesc = tablesData.ExistingTable(TString(cluster), TString(table));

    switch (op) {
        case TYdbOperation::Upsert: 
        case TYdbOperation::Replace: 
            return KiUpsertTableToKql(write, ctx, tableDesc, op == TYdbOperation::Replace, false, effect); 
        case TYdbOperation::InsertRevert: 
        case TYdbOperation::InsertAbort: 
            return KiInsertTableToKql(write, ctx, tableDesc, op, effect);
        case TYdbOperation::DeleteOn: 
            return KiDeleteOnTableToKql(write, ctx, tableDesc, effect);
        case TYdbOperation::UpdateOn: 
            return KiUpdateOnTableToKql(write, ctx, tableDesc, effect);
        default:
            return nullptr;
    }
}

TExprNode::TPtr KiUpdateTableToKql(TKiUpdateTable update, TExprContext& ctx,
    const TKikimrTablesData& tablesData, TExprNode::TPtr& effect, bool withSystemColumns) 
{
    YQL_ENSURE(update.Update().Ref().GetTypeAnn());

    const auto& cluster = update.DataSink().Cluster();
    const auto& table = update.Table();
    const auto& tableDesc = tablesData.ExistingTable(TString(cluster.Value()), TString(table.Value()));
    const TVector<TString>& pk = tableDesc.Metadata->KeyColumnNames;

    const auto versionedTable = BuildVersionedTable(*tableDesc.Metadata, update.Pos(), ctx);
    YQL_ENSURE(versionedTable.Path() == update.Table());

    auto schemaVersion = TStringBuilder() << tableDesc.Metadata->SchemaVersion;

    TKikimrKeyRange range(ctx, tableDesc);
    const auto& selectRange = Build<TKiSelectRange>(ctx, update.Pos())
        .Cluster(cluster)
        .Table(versionedTable)
        .Range(range.ToRangeExpr(update, ctx))
        .Select(BuildColumnsList(tableDesc, update.Pos(), ctx, withSystemColumns))
        .Settings().Build()
        .Done();

    const auto& filter = Build<TCoFilter>(ctx, update.Pos())
        .Input(selectRange)
        .Lambda(update.Filter())
        .Done();

    const auto& itemArg = Build<TCoArgument>(ctx, update.Pos())
        .Name("item")
        .Done();

    TVector<TCoNameValueTuple> valueTuples;
    const auto& updateResultType = update.Update().Ref().GetTypeAnn()->Cast<TStructExprType>();
    THashSet<TStringBuf> updatedColumns;

    for (const auto& item : updateResultType->GetItems()) {
        const auto& name = item->GetName();
        updatedColumns.insert(name);

        const auto& tuple = Build<TCoNameValueTuple>(ctx, update.Pos())
            .Name().Build(name)
            .Value<TCoMember>()
                .Struct<TExprApplier>()
                    .Apply(update.Update())
                    .With(0, itemArg)
                    .Build()
                .Name().Build(name)
                .Build()
            .Done();
        valueTuples.push_back(tuple);
    }

    const auto& indexes = BuildSecondaryIndexVector(tableDesc, update.Pos(), ctx, &updatedColumns);

    TVector<TExprBase> updates;
    updates.emplace_back(Build<TKiUpdateRow>(ctx, update.Pos())
            .Cluster(cluster)
            .Table(versionedTable)
            .Key(ExtractNamedKeyTuples(itemArg, tableDesc, ctx))
            .Update<TCoNameValueTupleList>()
                .Add(valueTuples)
                .Build()
            .Done()
        );

    if (indexes) {
        for (const auto& pair : indexes) {
            TVector<TString> indexTablePk;
            indexTablePk.reserve(pair.second->KeyColumns.size() + pk.size());
            for (const auto& col : pair.second->KeyColumns) {
                indexTablePk.push_back(col);
            }
            indexTablePk.insert(indexTablePk.end(), pk.begin(), pk.end());

            TVector<TExprBase> keyToAdd;
            TVector<TExprBase> keyToErase;
            TVector<TCoNameValueTuple> dataColumnsUpdates;

            keyToAdd.reserve(indexTablePk.size());
            keyToErase.reserve(indexTablePk.size());
            dataColumnsUpdates.reserve(pair.second->DataColumns.size());

            for (const TString& name : pair.second->DataColumns) {
                if (updatedColumns.contains(name)) {
                    const auto& tuple = Build<TCoNameValueTuple>(ctx, itemArg.Pos())
                        .Name().Build(name)
                        .Value<TCoMember>()
                            .Struct<TExprApplier>()
                                .Apply(update.Update())
                                .With(0, itemArg)
                                .Build()
                            .Name().Build(name)
                        .Build()
                        .Done();
                    dataColumnsUpdates.push_back(tuple);
                } else  {
                    const auto& oldTuple = Build<TCoNameValueTuple>(ctx, itemArg.Pos())
                        .Name().Build(name)
                        .Value<TCoMember>()
                            .Struct(itemArg)
                                .Name().Build(name)
                            .Build()
                        .Done();
                    dataColumnsUpdates.push_back(oldTuple);
                }
            }

            THashSet<TString> uniqColumns;
            for (const TString& name : indexTablePk) {
                if (!uniqColumns.insert(name).second) {
                    continue;
                }
                const auto& oldTuple = Build<TCoNameValueTuple>(ctx, itemArg.Pos())
                    .Name().Build(name)
                    .Value<TCoMember>()
                        .Struct(itemArg)
                            .Name().Build(name)
                        .Build()
                    .Done();

                keyToErase.push_back(oldTuple);

                if (updatedColumns.contains(name)) {
                    const auto& tuple = Build<TCoNameValueTuple>(ctx, itemArg.Pos())
                        .Name().Build(name)
                        .Value<TCoMember>()
                            .Struct<TExprApplier>()
                                .Apply(update.Update())
                                .With(0, itemArg)
                                .Build()
                            .Name().Build(name)
                        .Build()
                    .Done();
                    keyToAdd.push_back(tuple);
                } else {
                    keyToAdd.push_back(oldTuple);
                }
            }

            const auto& erase = Build<TKiEraseRow>(ctx, update.Pos())
                .Cluster(cluster)
                .Table(pair.first)
                .Key<TCoNameValueTupleList>()
                    .Add(keyToErase)
                .Build()
                .Done();
            updates.push_back(erase);

            const auto& updateRowSecondaryIndex = Build<TKiUpdateRow>(ctx, update.Pos())
                .Cluster(cluster)
                .Table(pair.first)
                .Key<TCoNameValueTupleList>()
                    .Add(keyToAdd)
                    .Build()
                .Update<TCoNameValueTupleList>()
                    .Add(dataColumnsUpdates)
                    .Build()
                .Done();

            updates.push_back(updateRowSecondaryIndex);
        }
    }

    effect = Build<TCoFlatMap>(ctx, update.Pos())
        .Input(filter)
        .Lambda<TCoLambda>()
            .Args({itemArg})
            .Body<TCoAsList>()
                .Add(updates)
                .Build()
            .Build()
        .Done()
        .Ptr();

    return Build<TCoWorld>(ctx, update.Pos())
        .Done()
        .Ptr();
}

TExprNode::TPtr KiDeleteTableToKql(TKiDeleteTable del, TExprContext& ctx,
    const TKikimrTablesData& tablesData, TExprNode::TPtr& effect, bool withSystemColumns) 
{
    const auto& cluster = del.DataSink().Cluster();
    const auto& table = del.Table();
    const auto& tableDesc = tablesData.ExistingTable(TString(cluster.Value()), TString(table.Value()));

    const auto versionedTable = BuildVersionedTable(*tableDesc.Metadata, del.Pos(), ctx);
    YQL_ENSURE(versionedTable.Path() == table);

    TKikimrKeyRange range(ctx, tableDesc);
    const auto& selectRange = Build<TKiSelectRange>(ctx, del.Pos())
        .Cluster(cluster)
        .Table(versionedTable)
        .Range(range.ToRangeExpr(del, ctx))
        .Select(BuildColumnsList(tableDesc, del.Pos(), ctx, withSystemColumns))
        .Settings().Build()
        .Done();

    const auto& filter = Build<TCoFilter>(ctx, del.Pos())
        .Input(selectRange)
        .Lambda(del.Filter())
        .Done();

    const auto& itemArg = Build<TCoArgument>(ctx, del.Pos())
        .Name("item")
        .Done();

    const auto& indexes = BuildSecondaryIndexVector(tableDesc, del.Pos(), ctx);

    TVector<TExprBase> updates;
    updates.reserve(indexes.size() + 1);

    const auto& tablePk = ExtractNamedKeyTuples(itemArg, tableDesc, ctx);
    updates.emplace_back(Build<TKiEraseRow>(ctx, del.Pos())
            .Cluster(cluster)
            .Table(versionedTable)
            .Key(tablePk)
            .Done()
        );

    const TVector<TString>& pk = tableDesc.Metadata->KeyColumnNames;
    if (indexes) {
        TVector<TExprBase> keyToErase;
        for (const auto& pair : indexes) {
            keyToErase.reserve(pair.second->KeyColumns.size() + pk.size());

            THashSet<TString> uniqColumns;
            for (const auto& col : pair.second->KeyColumns) {
                if (!uniqColumns.insert(col).second) {
                    continue;
                }
                const auto& member = Build<TCoNameValueTuple>(ctx, del.Pos())
                    .Name().Build(col)
                    .Value<TCoMember>()
                        .Struct(itemArg)
                        .Name().Build(col)
                        .Build()
                     .Done();
                 keyToErase.emplace_back(TExprBase(member));
            }

            for (const auto& k : pk) {
                if (!uniqColumns.insert(k).second) {
                    continue;
                }
                const auto& member = Build<TCoNameValueTuple>(ctx, del.Pos())
                    .Name().Build(k)
                    .Value<TCoMember>()
                        .Struct(itemArg)
                        .Name().Build(k)
                        .Build()
                    .Done();
                keyToErase.emplace_back(TExprBase(member));
            }

            const auto& erase = Build<TKiEraseRow>(ctx, del.Pos())
                .Cluster(cluster)
                .Table(pair.first)
                .Key<TCoNameValueTupleList>()
                    .Add(keyToErase)
                    .Build()
                .Done();

            keyToErase.clear();
            updates.push_back(erase);
        }
    }

    effect = Build<TCoFlatMap>(ctx, del.Pos())
            .Input(filter)
                .template Lambda<TCoLambda>()
                    .Args({itemArg})
                    .template Body<TCoAsList>()
                        .Add(updates)
                    .Build()
                .Build()
            .Done()
            .Ptr();

    return Build<TCoWorld>(ctx, del.Pos())
        .Done()
        .Ptr();
}

} // namespace

TKiProgram BuildKiProgram(TKiDataQuery query, const TKikimrTablesData& tablesData,
    TExprContext& ctx, bool withSystemColumns) 
{
    TExprNode::TPtr optResult;
    TOptimizeExprSettings optSettings(nullptr);
    optSettings.VisitChanges = true;
    IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
    status = OptimizeExpr(query.Effects().Ptr(), optResult,
        [&tablesData, withSystemColumns](const TExprNode::TPtr& input, TExprContext& ctx) { 
            auto node = TExprBase(input);
            auto ret = input;

            if (auto maybeWrite = node.Maybe<TKiWriteTable>()) {
                KiWriteTableToKql(maybeWrite.Cast(), ctx, tablesData, ret);
            } else if (auto maybeUpdate = node.Maybe<TKiUpdateTable>()) {
                KiUpdateTableToKql(maybeUpdate.Cast(), ctx, tablesData, ret, withSystemColumns); 
            } else if (auto maybeDelete = node.Maybe<TKiDeleteTable>()) {
                KiDeleteTableToKql(maybeDelete.Cast(), ctx, tablesData, ret, withSystemColumns); 
            }

            return ret;
        }, ctx, optSettings);

    YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);

    YQL_ENSURE(TMaybeNode<TKiEffects>(optResult));
    TVector<TExprBase> effectsList(TKiEffects(optResult).begin(), TKiEffects(optResult).end());

    TMaybeNode<TExprBase> effects;
    if (effectsList.empty()) {
        effects = Build<TCoList>(ctx, query.Pos())
            .ListType<TCoListType>()
                .ItemType<TCoVoidType>()
                    .Build()
                .Build()
            .Done();
    } else {
        effects = Build<TCoExtend>(ctx, query.Pos())
            .Add(effectsList)
            .Done();
    }

    TVector<TExprBase> results;
    for (const auto& kiResult : query.Results()) {
        results.push_back(kiResult.Value());
    }

    auto program = Build<TKiProgram>(ctx, query.Pos())
        .Results()
            .Add(results)
            .Build()
        .Effects(effects.Cast())
        .Done();

    TExprNode::TPtr newProgram;
    status = OptimizeExpr(program.Ptr(), newProgram,
        [&tablesData, withSystemColumns](const TExprNode::TPtr& input, TExprContext& ctx) { 
            auto node = TExprBase(input);

            if (node.Maybe<TCoRight>().Input().Maybe<TKiReadTable>()) {
                return KiReadTableToKql(node.Cast<TCoRight>(), ctx, tablesData, withSystemColumns); 
            }

            return input;
        }, ctx, optSettings);

    YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);
    YQL_ENSURE(TMaybeNode<TKiProgram>(newProgram));

    return TKiProgram(newProgram);
}

TExprBase UnwrapKiReadTableValues(TExprBase input, const TKikimrTableDescription& tableDesc,
    const TCoAtomList columns, TExprContext& ctx)
{
    TCoArgument itemArg = Build<TCoArgument>(ctx, input.Pos())
        .Name("item")
        .Done();

    TVector<TExprBase> structItems;
    for (auto atom : columns) {
        auto columnType = tableDesc.GetColumnType(TString(atom.Value()));
        YQL_ENSURE(columnType); 

        auto item = Build<TCoNameValueTuple>(ctx, input.Pos())
            .Name(atom)
            .Value<TCoCoalesce>()
                .Predicate<TCoMember>()
                    .Struct(itemArg)
                    .Name(atom)
                    .Build()
                .Value<TCoDefault>()
                    .Type(ExpandType(atom.Pos(), *columnType->Cast<TOptionalExprType>()->GetItemType(), ctx))
                    .Build()
                .Build()
            .Done();

        structItems.push_back(item);
    }

    return Build<TCoMap>(ctx, input.Pos())
        .Input(input)
        .Lambda()
            .Args({itemArg})
            .Body<TCoAsStruct>()
                .Add(structItems)
                .Build()
            .Build()
        .Done();
}

bool IsKeySelectorPkPrefix(NNodes::TCoLambda keySelector, const TKikimrTableDescription& tableDesc, TVector<TString>* columns) {
    auto checkKey = [keySelector, &tableDesc, columns] (const TExprBase& key, ui32 index) {
        if (!key.Maybe<TCoMember>()) {
            return false;
        }

        auto member = key.Cast<TCoMember>();
        if (member.Struct().Raw() != keySelector.Args().Arg(0).Raw()) {
            return false;
        }

        auto column = member.Name().StringValue();
        auto columnIndex = tableDesc.GetKeyColumnIndex(column);
        if (!columnIndex || *columnIndex != index) {
            return false;
        }

        if (columns) {
            columns->emplace_back(std::move(column));
        }

        return true;
    };

    auto lambdaBody = keySelector.Body();
    if (auto maybeTuple = lambdaBody.Maybe<TExprList>()) {
        auto tuple = maybeTuple.Cast();
        for (size_t i = 0; i < tuple.Size(); ++i) {
            if (!checkKey(tuple.Item(i), i)) {
                return false;
            }
        }
    } else {
        if (!checkKey(lambdaBody, 0)) {
            return false;
        }
    }

    return true;
}

} // namespace NYql
