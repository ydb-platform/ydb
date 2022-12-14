#include "yql_kikimr_provider_impl.h"
#include "yql_kikimr_gateway.h"

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>

namespace NYql {
namespace {

using namespace NNodes;
using namespace NCommon;

TKiOperation BuildTableOpNode(const TCoAtom& cluster, const TStringBuf& table, TYdbOperation op, TPositionHandle pos,
    TExprContext& ctx)
{
    return Build<TKiOperation>(ctx, pos)
        .Cluster().Build(cluster)
        .Table().Build(table)
        .Operation<TCoAtom>().Build(ToString(op))
        .Done();
}

TKiOperation BuildYdbOpNode(const TCoAtom& cluster, TYdbOperation op, TPositionHandle pos, TExprContext& ctx) {
    return Build<TKiOperation>(ctx, pos)
        .Cluster().Build(cluster)
        .Table().Build("")
        .Operation<TCoAtom>().Build(ToString(op))
        .Done();
}

TCoAtomList GetResultColumns(const TResWriteBase& resWrite, TExprContext& ctx) {
    TExprNode::TListType columns;
    auto columnsSetting = GetSetting(resWrite.Settings().Ref(), "columns");
    if (columnsSetting) {
        YQL_ENSURE(TMaybeNode<TCoNameValueTuple>(columnsSetting));
        TCoNameValueTuple tuple(columnsSetting);
        YQL_ENSURE(tuple.Value().Maybe<TCoAtomList>());
        auto list = tuple.Value().Cast<TCoAtomList>();
        for (const auto& column : list) {
            columns.push_back(column.Ptr());
        }
    }

    return Build<TCoAtomList>(ctx, resWrite.Pos())
        .Add(columns)
        .Done();
}

ui64 GetResultRowsLimit(const TResWriteBase& resWrite) {
    auto takeSetting = GetSetting(resWrite.Settings().Ref(), "take");
    if (takeSetting) {
        YQL_ENSURE(TMaybeNode<TCoNameValueTuple>(takeSetting));
        TCoNameValueTuple tuple(takeSetting);
        YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());

        ui64 rowsLimit;
        if (TryFromString<ui64>(tuple.Value().Cast<TCoAtom>().Value(), rowsLimit)) {
            return rowsLimit;
        }
    }

    return 0;
}

enum class TPrimitiveYdbOperation : ui32 {
    Read = 1 << 0,
    Write = 1 << 1
};

Y_DECLARE_FLAGS(TPrimitiveYdbOperations, TPrimitiveYdbOperation)
Y_DECLARE_OPERATORS_FOR_FLAGS(TPrimitiveYdbOperations)

struct TKiExploreTxResults {
    struct TKiQueryBlock {
        TVector<TExprBase> Results;
        TVector<TExprBase> Effects;
        THashMap<TString, TPrimitiveYdbOperations> TableOperations;
        bool HasUncommittedChangesRead = false;
    };

    THashSet<const TExprNode*> Ops;
    TVector<TExprBase> Sync;
    TVector<TKiQueryBlock> QueryBlocks;
    TVector<TKiOperation> TableOperations;
    bool HasExecute;

    THashSet<const TExprNode*> GetSyncSet() const {
        THashSet<const TExprNode*> syncSet;
        for (auto node : Sync) {
            syncSet.insert(node.Raw());
        }

        return syncSet;
    }

    void GetTableOperations(bool& hasScheme, bool& hasData) {
        hasScheme = false;
        hasData = false;
        for (auto& node : TableOperations) {
            auto op = FromString<TYdbOperation>(TString(node.Operation()));
            hasScheme = hasScheme || (op & KikimrSchemeOps());
            hasData = hasData || (op & KikimrDataOps());
        }
    }

    void AddReadOpToQueryBlock(const TKikimrKey& key, const TCoAtomList& readColumns, TKikimrTableMetadataPtr tableMeta) {
        YQL_ENSURE(tableMeta, "Empty table metadata");

        bool uncommittedChangesRead = false;
        if (key.GetView()) {
            const auto& indexName = key.GetView().GetRef();
            const auto indexTablePath = IKikimrGateway::CreateIndexTablePath(tableMeta->Name, indexName);

            auto indexIt = std::find_if(tableMeta->Indexes.begin(), tableMeta->Indexes.end(), [&indexName](const auto& index){
                return index.Name == indexName;
            });
            YQL_ENSURE(indexIt != tableMeta->Indexes.end(), "Index not found");

            THashSet<TString> indexColumns;
            indexColumns.reserve(indexIt->KeyColumns.size() + indexIt->DataColumns.size());
            for (const auto& keyColumn : indexIt->KeyColumns) {
                indexColumns.insert(keyColumn);
            }

            for (const auto& column : indexIt->DataColumns) {
                indexColumns.insert(column);
            }

            bool needMainTableRead = false;
            for (const auto& col : readColumns) {
                if (!indexColumns.contains(col.StringValue())) {
                    needMainTableRead = true;
                    break;
                }
            }

            uncommittedChangesRead = HasWriteOps(indexTablePath) || (needMainTableRead && HasWriteOps(tableMeta->Name));
        } else {
            uncommittedChangesRead = HasWriteOps(tableMeta->Name);
        }

        if (uncommittedChangesRead) {
            AddQueryBlock();
            SetBlockHasUncommittedChangesRead();
        }
    }

    void AddWriteOpToQueryBlock(const TExprBase& effect, TKikimrTableMetadataPtr tableMeta, bool needMainTableRead) {
        YQL_ENSURE(tableMeta, "Empty table metadata");

        THashMap<TString, TPrimitiveYdbOperations> ops;
        if (needMainTableRead) {
            ops[tableMeta->Name] |= TPrimitiveYdbOperation::Read;
        }
        ops[tableMeta->Name] |= TPrimitiveYdbOperation::Write;

        for (const auto& index : tableMeta->Indexes) {
            if (!index.ItUsedForWrite()) {
                continue;
            }

            const auto indexTable = IKikimrGateway::CreateIndexTablePath(tableMeta->Name, index.Name);

            ops[tableMeta->Name] |= TPrimitiveYdbOperation::Read;
            ops[indexTable] = TPrimitiveYdbOperation::Write;
        }

        AddEffect(effect, ops);
    }

    void AddUpdateOpToQueryBlock(const TExprBase& effect, TKikimrTableMetadataPtr tableMeta,
        const THashSet<std::string_view>& updateColumns) {
        YQL_ENSURE(tableMeta, "Empty table metadata");

        THashMap<TString, TPrimitiveYdbOperations> ops;
        // read and upsert rows into main table
        ops[tableMeta->Name] = TPrimitiveYdbOperation::Read | TPrimitiveYdbOperation::Write;

        for (const auto& index : tableMeta->Indexes) {
            if (!index.ItUsedForWrite()) {
                continue;
            }

            const auto indexTable = IKikimrGateway::CreateIndexTablePath(tableMeta->Name, index.Name);
            for (const auto& column : index.KeyColumns) {
                if (updateColumns.contains(column)) {
                    // delete old index values and upsert rows into index table
                    ops[indexTable] = TPrimitiveYdbOperation::Write;
                    break;
                }
            }

            for (const auto& column : index.DataColumns) {
                if (updateColumns.contains(column)) {
                    // upsert rows into index table
                    ops[indexTable] = TPrimitiveYdbOperation::Write;
                    break;
                }
            }
        }

        AddEffect(effect, ops);
    }

    void AddEffect(const TExprBase& effect, const THashMap<TString, TPrimitiveYdbOperations>& ops) {
        bool uncommittedChangesRead = false;
        for (const auto& [table, op] : ops) {
            auto readOp = op & TPrimitiveYdbOperation::Read;
            if (readOp && HasWriteOps(table)) {
                uncommittedChangesRead = true;
                break;
            }
        }

        if (QueryBlocks.empty() || uncommittedChangesRead) {
            AddQueryBlock();
        }

        auto& curBlock = QueryBlocks.back();
        curBlock.Effects.push_back(effect);
        curBlock.HasUncommittedChangesRead = uncommittedChangesRead;

        for (const auto& [table, op] : ops) {
            auto& currentOps = curBlock.TableOperations[table];
            currentOps |= op;
        }
    }

    void AddResult(const TExprBase& result) {
        if (QueryBlocks.empty()) {
            AddQueryBlock();
        }

        auto& curBlock = QueryBlocks.back();
        curBlock.Results.push_back(result);
    }

    bool HasWriteOps(std::string_view table) {
        if (QueryBlocks.empty()) {
            return false;
        }

        auto& curBlock = QueryBlocks.back();
        auto currentOps = curBlock.TableOperations[table];
        return currentOps & TPrimitiveYdbOperation::Write;
    }

    void AddQueryBlock() {
        QueryBlocks.emplace_back();
    }

    void SetBlockHasUncommittedChangesRead() {
        YQL_ENSURE(!QueryBlocks.empty());
        auto& curBlock = QueryBlocks.back();
        curBlock.HasUncommittedChangesRead = true;
    }

    TKiExploreTxResults()
        : HasExecute(false) {}
};

bool ExploreTx(TExprBase node, TExprContext& ctx, const TKiDataSink& dataSink, TKiExploreTxResults& txRes,
    TIntrusivePtr<TKikimrTablesData> tablesData) {

    if (txRes.Ops.cend() != txRes.Ops.find(node.Raw())) {
        return true;
    }

    if (node.Maybe<TCoWorld>()) {
        txRes.Ops.insert(node.Raw());
        return true;
    }

    if (auto maybeLeft = node.Maybe<TCoLeft>()) {
        txRes.Ops.insert(node.Raw());
        return ExploreTx(maybeLeft.Cast().Input(), ctx, dataSink, txRes, tablesData);
    }

    auto checkDataSource = [dataSink] (const TKiDataSource& ds) {
        return dataSink.Cluster().Raw() == ds.Cluster().Raw();
    };

    auto checkDataSink = [dataSink] (const TKiDataSink& ds) {
        return dataSink.Raw() == ds.Raw();
    };

    auto cluster = dataSink.Cluster();

    if (auto maybeRead = node.Maybe<TKiReadTable>()) {
        auto read = maybeRead.Cast();
        if (!checkDataSource(read.DataSource())) {
            return false;
        }

        TKikimrKey key(ctx);
        YQL_ENSURE(key.Extract(read.TableKey().Ref()));
        YQL_ENSURE(key.GetKeyType() == TKikimrKey::Type::Table);
        auto table = key.GetTablePath();
        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(maybeRead.Cast().World(), ctx, dataSink, txRes, tablesData);

        txRes.TableOperations.push_back(BuildTableOpNode(cluster, table, TYdbOperation::Select, read.Pos(), ctx));

        YQL_ENSURE(tablesData);
        const auto& tableData = tablesData->ExistingTable(cluster, table);
        YQL_ENSURE(tableData.Metadata);
        auto readColumns = read.GetSelectColumns(ctx, tableData);
        txRes.AddReadOpToQueryBlock(key, readColumns, tableData.Metadata);

        return result;
    }

    if (auto maybeWrite = node.Maybe<TKiWriteTable>()) {
        auto write = maybeWrite.Cast();
        if (!checkDataSink(write.DataSink())) {
            return false;
        }

        auto table = write.Table().Value();
        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(write.World(), ctx, dataSink, txRes, tablesData);
        auto tableOp = GetTableOp(write);
        txRes.TableOperations.push_back(BuildTableOpNode(cluster, table, tableOp, write.Pos(), ctx));

        YQL_ENSURE(tablesData);
        const auto& tableData = tablesData->ExistingTable(cluster, table);
        YQL_ENSURE(tableData.Metadata);

        if (tableOp == TYdbOperation::UpdateOn) {
            auto inputColumnsSetting = GetSetting(write.Settings().Ref(), "input_columns");
            YQL_ENSURE(inputColumnsSetting);
            auto inputColumns = TCoNameValueTuple(inputColumnsSetting).Value().Cast<TCoAtomList>();
            THashSet<std::string_view> updateColumns;
            for (const auto& column : inputColumns) {
                updateColumns.emplace(column);
            }
            txRes.AddUpdateOpToQueryBlock(node, tableData.Metadata, updateColumns);
        } else {
            txRes.AddWriteOpToQueryBlock(node, tableData.Metadata, tableOp & KikimrReadOps());
        }

        return result;
    }

    if (auto maybeUpdate = node.Maybe<TKiUpdateTable>()) {
        auto update = maybeUpdate.Cast();
        if (!checkDataSink(update.DataSink())) {
            return false;
        }

        auto table = update.Table().Value();
        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(update.World(), ctx, dataSink, txRes, tablesData);
        const auto tableOp = TYdbOperation::Update;
        txRes.TableOperations.push_back(BuildTableOpNode(cluster, table, tableOp, update.Pos(), ctx));

        YQL_ENSURE(tablesData);
        const auto& tableData = tablesData->ExistingTable(cluster, table);
        YQL_ENSURE(tableData.Metadata);
        
        THashSet<std::string_view> updateColumns;
        const auto& updateStructType = update.Update().Ref().GetTypeAnn()->Cast<TStructExprType>();
        for (const auto& item : updateStructType->GetItems()) {
            updateColumns.emplace(item->GetName());
        }
        txRes.AddUpdateOpToQueryBlock(node, tableData.Metadata, updateColumns);

        return result;
    }

    if (auto maybeDelete = node.Maybe<TKiDeleteTable>()) {
        auto del = maybeDelete.Cast();
        if (!checkDataSink(del.DataSink())) {
            return false;
        }

        auto table = del.Table().Value();
        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(del.World(), ctx, dataSink, txRes, tablesData);
        const auto tableOp = TYdbOperation::Delete;
        txRes.TableOperations.push_back(BuildTableOpNode(cluster, table, tableOp, del.Pos(), ctx));

        YQL_ENSURE(tablesData);
        const auto& tableData = tablesData->ExistingTable(cluster, table);
        YQL_ENSURE(tableData.Metadata);
        txRes.AddWriteOpToQueryBlock(node, tableData.Metadata, tableOp & KikimrReadOps());

        return result;
    }

    if (auto maybeCreate = node.Maybe<TKiCreateTable>()) {
        auto create = maybeCreate.Cast();
        if (!checkDataSink(create.DataSink())) {
            return false;
        }

        auto table = create.Table().Value();
        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(create.World(), ctx, dataSink, txRes, tablesData);
        txRes.TableOperations.push_back(BuildTableOpNode(cluster, table, TYdbOperation::CreateTable, create.Pos(), ctx));
        return result;
    }

    if (auto maybeDrop = node.Maybe<TKiDropTable>()) {
        auto drop = maybeDrop.Cast();
        if (!checkDataSink(drop.DataSink())) {
            return false;
        }

        auto table = drop.Table().Value();
        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(drop.World(), ctx, dataSink, txRes, tablesData);
        txRes.TableOperations.push_back(BuildTableOpNode(cluster, table, TYdbOperation::DropTable, drop.Pos(), ctx));
        return result;
    }

    if (auto maybeAlter = node.Maybe<TKiAlterTable>()) {
        auto alter = maybeAlter.Cast();
        if (!checkDataSink(alter.DataSink())) {
            return false;
        }

        auto table = alter.Table().Value();
        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(alter.World(), ctx, dataSink, txRes, tablesData);
        txRes.TableOperations.push_back(BuildTableOpNode(cluster, table, TYdbOperation::AlterTable, alter.Pos(), ctx));
        return result;
    }

    if (auto maybeCreateUser = node.Maybe<TKiCreateUser>()) {
        auto createUser = maybeCreateUser.Cast();
        if (!checkDataSink(createUser.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(createUser.World(), ctx, dataSink, txRes, tablesData);
        txRes.TableOperations.push_back(BuildYdbOpNode(cluster, TYdbOperation::CreateUser, createUser.Pos(), ctx));
        return result;
    }

    if (auto maybeAlterUser = node.Maybe<TKiAlterUser>()) {
        auto alterUser = maybeAlterUser.Cast();
        if (!checkDataSink(alterUser.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(alterUser.World(), ctx, dataSink, txRes, tablesData);
        txRes.TableOperations.push_back(BuildYdbOpNode(cluster, TYdbOperation::AlterUser, alterUser.Pos(), ctx));
        return result;
    }

    if (auto maybeDropUser = node.Maybe<TKiDropUser>()) {
        auto dropUser = maybeDropUser.Cast();
        if (!checkDataSink(dropUser.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(dropUser.World(), ctx, dataSink, txRes, tablesData);
        txRes.TableOperations.push_back(BuildYdbOpNode(cluster, TYdbOperation::DropUser, dropUser.Pos(), ctx));
        return result;
    }

    if (auto maybeCreateGroup = node.Maybe<TKiCreateGroup>()) {
        auto createGroup = maybeCreateGroup.Cast();
        if (!checkDataSink(createGroup.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(createGroup.World(), ctx, dataSink, txRes, tablesData);
        txRes.TableOperations.push_back(BuildYdbOpNode(cluster, TYdbOperation::CreateGroup, createGroup.Pos(), ctx));
        return result;
    }

    if (auto maybeAlterGroup = node.Maybe<TKiAlterGroup>()) {
        auto alterGroup = maybeAlterGroup.Cast();
        if (!checkDataSink(alterGroup.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(alterGroup.World(), ctx, dataSink, txRes, tablesData);
        txRes.TableOperations.push_back(BuildYdbOpNode(cluster, TYdbOperation::AlterGroup, alterGroup.Pos(), ctx));
        return result;
    }

    if (auto maybeDropGroup = node.Maybe<TKiDropGroup>()) {
        auto dropGroup = maybeDropGroup.Cast();
        if (!checkDataSink(dropGroup.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(dropGroup.World(), ctx, dataSink, txRes, tablesData);
        txRes.TableOperations.push_back(BuildYdbOpNode(cluster, TYdbOperation::DropGroup, dropGroup.Pos(), ctx));
        return result;
    }

    if (auto maybeExecQuery = node.Maybe<TKiExecDataQuery>()) {
        auto execQuery = maybeExecQuery.Cast();
        if (!checkDataSink(execQuery.DataSink())) {
            return false;
        }

        txRes.HasExecute = true;
        return true;
    }

    if (auto maybeCommit = node.Maybe<TCoCommit>()) {
        auto commit = maybeCommit.Cast();

        if (commit.DataSink().Maybe<TKiDataSink>() && checkDataSink(commit.DataSink().Cast<TKiDataSink>())) {
            txRes.Sync.push_back(commit);
            return true;
        }

        return ExploreTx(commit.World(), ctx, dataSink, txRes, tablesData);
    }

    if (auto maybeSync = node.Maybe<TCoSync>()) {
        txRes.Ops.insert(node.Raw());
        for (auto child : maybeSync.Cast()) {
            if (!ExploreTx(child, ctx, dataSink, txRes, tablesData)) {
                return false;
            }

            return true;
        }
    }

    if (node.Maybe<TResWrite>() ||
        node.Maybe<TResPull>())
    {
        txRes.Ops.insert(node.Raw());
        bool result = ExploreTx(TExprBase(node.Ref().ChildPtr(0)), ctx, dataSink, txRes, tablesData);
        txRes.AddResult(node);
        return result;
    }

    if (node.Ref().IsCallable(ConfigureName)) {
        txRes.Sync.push_back(node);
        return true;
    }

    return false;
}

bool IsKikimrPureNode(const TExprNode::TPtr& node) {
    if (node->IsCallable("TypeOf")) {
        return true;
    }

    if (TMaybeNode<TCoDataSource>(node) ||
        TMaybeNode<TCoDataSink>(node))
    {
        return true;
    }

    if (!node->GetTypeAnn()->IsComposable()) {
        return false;
    }

    return true;
}

bool CheckTx(TExprBase txStart, const TKiDataSink& dataSink, const THashSet<const TExprNode*>& txOps,
    const THashSet<const TExprNode*>& txSync)
{
    bool hasErrors = false;
    VisitExpr(txStart.Ptr(), [&txOps, &txSync, &hasErrors, dataSink] (const TExprNode::TPtr& node) {
        if (hasErrors) {
            return false;
        }

        if (txSync.find(node.Get()) != txSync.cend()) {
            return false;
        }

        if (auto maybeCommit = TMaybeNode<TCoCommit>(node)) {
            if (maybeCommit.Cast().DataSink().Raw() != dataSink.Raw()) {
                return true;
            }
        }

        if (!IsKikimrPureNode(node) && txOps.find(node.Get()) == txOps.cend()) {
            hasErrors = true;
            return false;
        }

        return true;
    });

    return !hasErrors;
}

TExprNode::TPtr MakeSchemeTx(TCoCommit commit, TExprContext& ctx) {
    auto settings = NCommon::ParseCommitSettings(commit, ctx);

    if (settings.Mode) {
        return commit.Ptr();
    }

    return Build<TCoCommit>(ctx, commit.Pos())
        .World(commit.World())
        .DataSink(commit.DataSink())
        .Settings()
            .Add()
                .Name().Build("mode")
                .Value<TCoAtom>().Build(KikimrCommitModeScheme())
                .Build()
            .Build()
        .Done()
        .Ptr();
}

TKiDataQuery MakeKiDataQuery(TExprBase node, const TKiExploreTxResults& txExplore, TExprContext& ctx) {
    TVector<TKiDataQueryBlock> queryBlocks;
    queryBlocks.reserve(txExplore.QueryBlocks.size());

    for (const auto& block : txExplore.QueryBlocks) {
        TKiDataQueryBlockSettings settings;
        settings.HasUncommittedChangesRead = block.HasUncommittedChangesRead;

        TExprNode::TListType queryResults;
        for (auto& result : block.Results) {
            auto resWrite = result.Cast<TResWriteBase>();

            auto kiResult = Build<TKiResult>(ctx, node.Pos())
                .Value(resWrite.Data())
                .Columns(GetResultColumns(resWrite, ctx))
                .RowsLimit().Build(GetResultRowsLimit(resWrite))
                .Done();

            queryResults.push_back(kiResult.Ptr());
        }
        queryBlocks.emplace_back(Build<TKiDataQueryBlock>(ctx, node.Pos())
            .Results()
                .Add(queryResults)
                .Build()
            .Effects()
                .Add(block.Effects)
                .Build()
            .Settings(settings.BuildNode(ctx, node.Pos()))
            .Done());
    }

    auto query = Build<TKiDataQuery>(ctx, node.Pos())
        .Operations()
            .Add(txExplore.TableOperations)
            .Build()
        .Blocks()
            .Add(queryBlocks)
            .Build()
        .Done();


    auto txSyncSet = txExplore.GetSyncSet();
    auto world = Build<TCoWorld>(ctx, node.Pos()).Done();

    TExprNode::TPtr optResult;
    TOptimizeExprSettings optSettings(nullptr);
    optSettings.VisitChanges = true;
    auto status = OptimizeExpr(query.Ptr(), optResult,
        [world, &txSyncSet](const TExprNode::TPtr& input, TExprContext& ctx) {
            auto node = TExprBase(input);

            if (txSyncSet.contains(node.Raw())) {
                return world.Ptr();
            }

            if (node.Maybe<TKiReadTable>() ||
                node.Maybe<TKiWriteTable>() ||
                node.Maybe<TKiUpdateTable>() ||
                node.Maybe<TKiDeleteTable>())
            {
                return ctx.ChangeChild(node.Ref(), 0, world.Ptr());
            }

            return node.Ptr();
        }, ctx, optSettings);

    YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);
    YQL_ENSURE(TMaybeNode<TKiDataQuery>(optResult));
    return TKiDataQuery(optResult);
}

} // namespace

TExprNode::TPtr KiBuildQuery(TExprBase node, TExprContext& ctx, TIntrusivePtr<TKikimrTablesData> tablesData) {
    if (!node.Maybe<TCoCommit>().DataSink().Maybe<TKiDataSink>()) {
        return node.Ptr();
    }

    auto commit = node.Cast<TCoCommit>();
    auto settings = NCommon::ParseCommitSettings(commit, ctx);
    auto kiDataSink = commit.DataSink().Cast<TKiDataSink>();

    TKiExploreTxResults txExplore;
    if (!ExploreTx(commit.World(), ctx, kiDataSink, txExplore, tablesData)) {
        return node.Ptr();
    }

    if (txExplore.HasExecute) {
        return node.Ptr();
    }

    auto txSyncSet = txExplore.GetSyncSet();
    if (!CheckTx(commit.World(), kiDataSink, txExplore.Ops, txSyncSet)) {
        return node.Ptr();
    }

    bool hasScheme;
    bool hasData;
    txExplore.GetTableOperations(hasScheme, hasData);

    if (hasData && hasScheme) {
        ctx.AddError(YqlIssue(ctx.GetPosition(commit.Pos()), TIssuesIds::KIKIMR_MIXED_SCHEME_DATA_TX));
        return nullptr;
    }

    if (hasScheme) {
        return MakeSchemeTx(commit, ctx);
    }

    auto dataQuery = MakeKiDataQuery(commit.World(), txExplore, ctx);

    TKiExecDataQuerySettings execSettings;
    if (settings.Mode) {
        auto value = settings.Mode.Cast().Value();
        if (!value.empty()) {
            execSettings.Mode = value;
        }
    }

    auto execQuery = Build<TKiExecDataQuery>(ctx, node.Pos())
        .World<TCoSync>()
            .Add<TCoWorld>().Build()
            .Add(txExplore.Sync)
            .Build()
        .DataSink(commit.DataSink().Cast<TKiDataSink>())
        .Query(dataQuery)
        .Settings(execSettings.BuildNode(ctx, node.Pos()))
        .Ast<TCoVoid>().Build()
        .Done();

    TExprBase execWorld = Build<TCoLeft>(ctx, node.Pos())
        .Input(execQuery)
        .Done();


    bool hasResults = false;
    for (const auto& block : txExplore.QueryBlocks) {
        hasResults = hasResults || !block.Results.empty();
    }

    if (hasResults) {
        auto execRight = Build<TCoRight>(ctx, node.Pos())
            .Input(execQuery)
            .Done()
            .Ptr();

        for (auto& block : txExplore.QueryBlocks) {
            for (size_t i = 0; i < block.Results.size(); ++i) {
                auto result = block.Results[i].Cast<TResWriteBase>();

                auto extractValue = Build<TCoNth>(ctx, node.Pos())
                    .Tuple(execRight)
                    .Index().Build(i)
                    .Done()
                    .Ptr();

                auto newResult = ctx.ChangeChild(
                    *ctx.ChangeChild(
                        result.Ref(),
                        TResWriteBase::idx_World,
                        execWorld.Ptr()
                    ),
                    TResWriteBase::idx_Data,
                    std::move(extractValue)
                );

                execWorld = Build<TCoCommit>(ctx, node.Pos())
                    .World(newResult)
                    .DataSink<TResultDataSink>()
                        .Build()
                    .Done();
            }
        }
    }

    auto ret = Build<TCoCommit>(ctx, node.Pos())
        .World(execWorld)
        .DataSink(commit.DataSink())
        .Settings(settings.BuildNode(ctx))
        .Done()
        .Ptr();

    YQL_CLOG(INFO, ProviderKikimr) << "KiBuildQuery";
    return ret;
}

TExprNode::TPtr KiBuildResult(TExprBase node,  const TString& cluster, TExprContext& ctx) {
    if (!node.Maybe<TResFill>()) {
        return node.Ptr();
    }

    auto resFill = node.Cast<TResFill>();

    if (resFill.DelegatedSource().Value() != KikimrProviderName) {
        return node.Ptr();
    }

    if (resFill.Data().Maybe<TCoNth>().Tuple().Maybe<TCoRight>().Input().Maybe<TKiExecDataQuery>()) {
        return node.Ptr();
    }

    auto queryBlock = Build<TKiDataQueryBlock>(ctx, node.Pos())
        .Results()
            .Add()
                .Value(resFill.Data())
                .Columns(GetResultColumns(resFill, ctx))
                .RowsLimit().Build(GetResultRowsLimit(resFill))
                .Build()
            .Build()
        .Effects()
            .Build()
        .Done();

    auto dataQuery = Build<TKiDataQuery>(ctx, node.Pos())
        .Operations()
            .Build()
        .Blocks()
            .Add(queryBlock)
            .Build()
        .Done();

    auto exec = Build<TKiExecDataQuery>(ctx, node.Pos())
        .World(resFill.World())
        .DataSink<TKiDataSink>()
            .Category().Build(KikimrProviderName)
            .Cluster().Build(cluster)
            .Build()
        .Query(dataQuery)
        .Settings()
            .Build()
        .Ast<TCoVoid>().Build()
        .Done();

    auto data = Build<TCoNth>(ctx, node.Pos())
        .Tuple<TCoRight>()
            .Input(exec)
            .Build()
        .Index().Build(0)
        .Done();

    auto world = Build<TCoLeft>(ctx, node.Pos())
        .Input(exec)
        .Done();

    return ctx.ChangeChild(*ctx.ChangeChild(resFill.Ref(), 0, world.Ptr()), 3, data.Ptr());
}

TYdbOperation GetTableOp(const TKiWriteTable& write) {
    auto mode = write.Mode().Value();
    if (mode == "upsert") {
        return TYdbOperation::Upsert;
    } else if (mode == "replace") {
        return TYdbOperation::Replace;
    } else if (mode == "insert_revert") {
        return TYdbOperation::InsertRevert;
    } else if (mode == "insert_abort") {
        return TYdbOperation::InsertAbort;
    } else if (mode == "delete_on") {
        return TYdbOperation::DeleteOn;
    } else if (mode == "update_on") {
        return TYdbOperation::UpdateOn;
    }

    YQL_ENSURE(false, "Unexpected TKiWriteTable mode: " << mode);
}

} // namespace NYql
