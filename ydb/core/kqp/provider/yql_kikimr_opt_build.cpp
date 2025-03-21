#include "yql_kikimr_provider_impl.h"
#include "yql_kikimr_gateway.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <yql/essentials/providers/pg/expr_nodes/yql_pg_expr_nodes.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

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

Y_DECLARE_FLAGS(TPrimitiveYdbOperations, TPrimitiveYdbOperation);
Y_DECLARE_OPERATORS_FOR_FLAGS(TPrimitiveYdbOperations);

struct TKiExploreTxResults {
    struct TKiQueryBlock {
        TVector<TExprBase> Results;
        TVector<TExprBase> Effects;
        THashMap<TString, TPrimitiveYdbOperations> TablePrimitiveOps; // needed to split query into blocks
        TVector<TKiOperation> TableOperations;
        bool HasUncommittedChangesRead = false;
    };

    bool ConcurrentResults = true;

    THashSet<const TExprNode*> Ops;
    TVector<TExprBase> Sync;
    TVector<TKiQueryBlock> QueryBlocks;
    bool HasExecute;
    bool HasErrors;

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
        for (auto& queryBlock : QueryBlocks) {
            for (auto& node : queryBlock.TableOperations) {
                auto op = FromString<TYdbOperation>(TString(node.Operation()));
                hasScheme = hasScheme || (op & KikimrSchemeOps());
                hasData = hasData || (op & KikimrDataOps());
            }
        }
    }

    void AddTableOperation(TKiOperation&& op) {
        if (QueryBlocks.empty()) {
            AddQueryBlock();
        }

        auto& curBlock = QueryBlocks.back();
        curBlock.TableOperations.emplace_back(std::move(op));
    }

    void AddReadOpToQueryBlock(const TKikimrKey& key, const TCoAtomList& readColumns, TKikimrTableMetadataPtr tableMeta) {
        YQL_ENSURE(tableMeta, "Empty table metadata");

        bool uncommittedChangesRead = false;
        auto view = key.GetView();
        if (view && view->Name) {
            const auto& indexName = view->Name;

            auto indexIt = std::find_if(tableMeta->Indexes.begin(), tableMeta->Indexes.end(), [&indexName](const auto& index){
                return index.Name == indexName;
            });
            YQL_ENSURE(indexIt != tableMeta->Indexes.end(), "Index not found");

            const auto indexTablePaths = NKikimr::NKqp::NSchemeHelpers::CreateIndexTablePath(tableMeta->Name, *indexIt);

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

            uncommittedChangesRead = needMainTableRead && HasWriteOps(tableMeta->Name);
            for (auto& indexTablePath : indexTablePaths) {
                if (uncommittedChangesRead) {
                    break;
                }
                uncommittedChangesRead = HasWriteOps(indexTablePath);
            }
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

            const auto indexTables = NKikimr::NKqp::NSchemeHelpers::CreateIndexTablePath(tableMeta->Name, index);
            YQL_ENSURE(indexTables.size() == 1, "Only index with one impl table is supported");
            const auto indexTable = indexTables[0];

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

            const auto indexTables = NKikimr::NKqp::NSchemeHelpers::CreateIndexTablePath(tableMeta->Name, index);
            YQL_ENSURE(indexTables.size() == 1, "Only index with one impl table is supported");
            const auto indexTable = indexTables[0];

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
            auto& currentOps = curBlock.TablePrimitiveOps[table];
            currentOps |= op;
        }
    }

    void PrepareForResult() {
        if (QueryBlocks.empty()) {
            AddQueryBlock();
        }

        if (!ConcurrentResults && QueryBlocks.back().Results.size() > 0) {
            AddQueryBlock();
        }
    }

    void AddResult(const TExprBase& result) {
        PrepareForResult();

        auto& curBlock = QueryBlocks.back();
        curBlock.Results.push_back(result);
    }

    bool HasWriteOps(std::string_view table) {
        if (QueryBlocks.empty()) {
            return false;
        }

        auto& curBlock = QueryBlocks.back();
        auto currentOps = curBlock.TablePrimitiveOps[table];
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
        : HasExecute(false)
        , HasErrors(false) {}
};

bool IsDqRead(const TExprBase& node, TExprContext& ctx, TTypeAnnotationContext& types, bool estimateReadSize, bool* hasErrors = nullptr) {
    if (node.Ref().ChildrenSize() <= 1) {
        return false;
    }

    TExprBase providerArg(node.Ref().Child(1));
    if (auto maybeDataSource = providerArg.Maybe<TCoDataSource>()) {
        TStringBuf dataSourceCategory = maybeDataSource.Cast().Category();
        if (dataSourceCategory == NYql::PgProviderName) {
            // All pg reads should be replaced on TPgTableContent
            return false;
        }

        auto dataSourceProviderIt = types.DataSourceMap.find(dataSourceCategory);
        if (dataSourceProviderIt != types.DataSourceMap.end()) {
            if (auto* dqIntegration = dataSourceProviderIt->second->GetDqIntegration()) {
                if (!dqIntegration->CanRead(*node.Ptr(), ctx)) {
                    if (!node.Ref().IsCallable(ConfigureName) && hasErrors) {
                        *hasErrors = true;
                    }
                    return false;
                }
                if (!estimateReadSize || dqIntegration->EstimateReadSize(
                        TDqSettings::TDefault::DataSizePerJob,
                        TDqSettings::TDefault::MaxTasksPerStage,
                        {node.Raw()},
                        ctx)) {
                    return true;
                }
            }
        }
    }
    return false;
}

bool IsPgRead(const TExprBase& node, TTypeAnnotationContext& types) {
    if (auto maybePgRead = node.Maybe<TPgTableContent>()) {
        auto dataSourceProviderIt = types.DataSourceMap.find(NYql::PgProviderName);
        if (dataSourceProviderIt != types.DataSourceMap.end()) {
            return true;
        }
    }
    return false;
}

bool IsDqWrite(const TExprBase& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    if (node.Ref().ChildrenSize() <= 1) {
        return false;
    }

    TExprBase providerArg(node.Ref().Child(1));
    if (auto maybeDataSink = providerArg.Maybe<TCoDataSink>()) {
        TStringBuf dataSinkCategory = maybeDataSink.Cast().Category();
        auto dataSinkProviderIt = types.DataSinkMap.find(dataSinkCategory);
        if (dataSinkProviderIt != types.DataSinkMap.end()) {
            if (auto* dqIntegration = dataSinkProviderIt->second->GetDqIntegration()) {
                if (auto canWrite = dqIntegration->CanWrite(*node.Ptr(), ctx)) {
                    YQL_ENSURE(*canWrite, "Errors handling write");
                    return true;
                }
            }
        }
    }
    return false;
}

bool ExploreNode(TExprBase node, TExprContext& ctx, const TKiDataSink& dataSink, TKiExploreTxResults& txRes,
    TIntrusivePtr<TKikimrTablesData> tablesData, TTypeAnnotationContext& types) {

    if (txRes.Ops.cend() != txRes.Ops.find(node.Raw())) {
        return true;
    }

    if (node.Maybe<TCoWorld>()) {
        txRes.Ops.insert(node.Raw());
        return true;
    }

    if (node.Maybe<TCoLeft>()) {
        txRes.Ops.insert(node.Raw());
        return true;
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

        YQL_ENSURE(tablesData);
        const auto& tableData = tablesData->ExistingTable(cluster, table);
        YQL_ENSURE(tableData.Metadata);
        auto readColumns = read.GetSelectColumns(ctx, tableData);
        txRes.AddReadOpToQueryBlock(key, readColumns, tableData.Metadata);
        txRes.AddTableOperation(BuildTableOpNode(cluster, table, TYdbOperation::Select, read.Pos(), ctx));
        return true;
    }

    if (IsDqRead(node, ctx, types, true, &txRes.HasErrors)) {
        txRes.Ops.insert(node.Raw());
        return true;
    }

    if (IsPgRead(node, types)) {
        txRes.Ops.insert(node.Raw());
        return true;
    }

    if (auto maybeWrite = node.Maybe<TKiWriteTable>()) {
        auto write = maybeWrite.Cast();
        if (!checkDataSink(write.DataSink())) {
            return false;
        }

        auto table = write.Table().Value();
        txRes.Ops.insert(node.Raw());
        auto tableOp = GetTableOp(write);

        YQL_ENSURE(tablesData);
        const auto& tableData = tablesData->ExistingTable(cluster, table);
        YQL_ENSURE(tableData.Metadata);

        if (!write.ReturningColumns().Empty()) {
            txRes.PrepareForResult();
        }

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

        if (!write.ReturningColumns().Empty()) {
            txRes.AddResult(
                Build<TResWrite>(ctx, write.Pos())
                .World(write.World())
                .DataSink<TResultDataSink>().Build()
                .Key<TCoKey>().Build()
                .Data<TKiReturningList>()
                    .Update(node)
                    .Columns(write.ReturningColumns())
                    .Build()
                .Settings()
                    .Add().Name().Value("columns").Build().Value(write.ReturningColumns()).Build()
                .Build()
                .Done());
        }

        txRes.AddTableOperation(BuildTableOpNode(cluster, table, tableOp, write.Pos(), ctx));
        return true;
    }

    if (IsDqWrite(node, ctx, types)) {
        txRes.Ops.insert(node.Raw());
        txRes.AddEffect(node, THashMap<TString, TPrimitiveYdbOperations>{});
        return true;
    }

    if (auto maybeUpdate = node.Maybe<TKiUpdateTable>()) {
        auto update = maybeUpdate.Cast();
        if (!checkDataSink(update.DataSink())) {
            return false;
        }

        auto table = update.Table().Value();
        txRes.Ops.insert(node.Raw());
        const auto tableOp = TYdbOperation::Update;

        YQL_ENSURE(tablesData);
        const auto& tableData = tablesData->ExistingTable(cluster, table);
        YQL_ENSURE(tableData.Metadata);

        THashSet<std::string_view> updateColumns;
        const auto& updateStructType = update.Update().Ref().GetTypeAnn()->Cast<TStructExprType>();
        for (const auto& item : updateStructType->GetItems()) {
            updateColumns.emplace(item->GetName());
        }

        if (!update.ReturningColumns().Empty()) {
            txRes.PrepareForResult();
        }

        txRes.AddUpdateOpToQueryBlock(node, tableData.Metadata, updateColumns);
        if (!update.ReturningColumns().Empty()) {
            txRes.AddResult(
                Build<TResWrite>(ctx, update.Pos())
                .World(update.World())
                .DataSink<TResultDataSink>().Build()
                .Key<TCoKey>().Build()
                .Data<TKiReturningList>()
                    .Update(node)
                    .Columns(update.ReturningColumns())
                    .Build()
                .Settings()
                    .Add().Name().Value("columns").Build().Value(update.ReturningColumns()).Build()
                .Build()
                .Done());
        }

        txRes.AddTableOperation(BuildTableOpNode(cluster, table, tableOp, update.Pos(), ctx));
        return true;
    }

    if (auto maybeDelete = node.Maybe<TKiDeleteTable>()) {
        auto del = maybeDelete.Cast();
        if (!checkDataSink(del.DataSink())) {
            return false;
        }

        auto table = del.Table().Value();
        txRes.Ops.insert(node.Raw());
        const auto tableOp = TYdbOperation::Delete;

        YQL_ENSURE(tablesData);
        const auto& tableData = tablesData->ExistingTable(cluster, table);
        YQL_ENSURE(tableData.Metadata);
        if (!del.ReturningColumns().Empty()) {
            txRes.PrepareForResult();
        }

        txRes.AddWriteOpToQueryBlock(node, tableData.Metadata, tableOp & KikimrReadOps());
        if (!del.ReturningColumns().Empty()) {
            txRes.AddResult(
                Build<TResWrite>(ctx, del.Pos())
                .World(del.World())
                .DataSink<TResultDataSink>().Build()
                .Key<TCoKey>().Build()
                .Data<TKiReturningList>()
                    .Update(node)
                    .Columns(del.ReturningColumns())
                    .Build()
                .Settings()
                    .Add().Name().Value("columns").Build().Value(del.ReturningColumns()).Build()
                .Build()
                .Done());
        }

        txRes.AddTableOperation(BuildTableOpNode(cluster, table, tableOp, del.Pos(), ctx));
        return true;
    }

    if (auto maybeCreate = node.Maybe<TKiCreateTable>()) {
        auto create = maybeCreate.Cast();
        if (!checkDataSink(create.DataSink())) {
            return false;
        }

        auto table = create.Table().Value();
        txRes.Ops.insert(node.Raw());
        txRes.AddTableOperation(BuildTableOpNode(cluster, table, TYdbOperation::CreateTable, create.Pos(), ctx));
        return true;
    }

    if (auto maybeDrop = node.Maybe<TKiDropTable>()) {
        auto drop = maybeDrop.Cast();
        if (!checkDataSink(drop.DataSink())) {
            return false;
        }

        auto table = drop.Table().Value();
        txRes.Ops.insert(node.Raw());
        txRes.AddTableOperation(BuildTableOpNode(cluster, table, TYdbOperation::DropTable, drop.Pos(), ctx));
        return true;
    }

    if (auto maybeAlter = node.Maybe<TKiAlterTable>()) {
        auto alter = maybeAlter.Cast();
        if (!checkDataSink(alter.DataSink())) {
            return false;
        }

        auto table = alter.Table().Value();
        txRes.Ops.insert(node.Raw());
        txRes.AddTableOperation(BuildTableOpNode(cluster, table, TYdbOperation::AlterTable, alter.Pos(), ctx));
        return true;
    }

    if (auto maybeCreateUser = node.Maybe<TKiCreateUser>()) {
        auto createUser = maybeCreateUser.Cast();
        if (!checkDataSink(createUser.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        txRes.AddTableOperation(BuildYdbOpNode(cluster, TYdbOperation::CreateUser, createUser.Pos(), ctx));
        return true;
    }

    if (auto maybeAlterUser = node.Maybe<TKiAlterUser>()) {
        auto alterUser = maybeAlterUser.Cast();
        if (!checkDataSink(alterUser.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        txRes.AddTableOperation(BuildYdbOpNode(cluster, TYdbOperation::AlterUser, alterUser.Pos(), ctx));
        return true;
    }

    if (auto maybeDropUser = node.Maybe<TKiDropUser>()) {
        auto dropUser = maybeDropUser.Cast();
        if (!checkDataSink(dropUser.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        txRes.AddTableOperation(BuildYdbOpNode(cluster, TYdbOperation::DropUser, dropUser.Pos(), ctx));
        return true;
    }

    if (auto maybeCreateGroup = node.Maybe<TKiCreateGroup>()) {
        auto createGroup = maybeCreateGroup.Cast();
        if (!checkDataSink(createGroup.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        txRes.AddTableOperation(BuildYdbOpNode(cluster, TYdbOperation::CreateGroup, createGroup.Pos(), ctx));
        return true;
    }

    if (auto maybeAlterGroup = node.Maybe<TKiAlterGroup>()) {
        auto alterGroup = maybeAlterGroup.Cast();
        if (!checkDataSink(alterGroup.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        txRes.AddTableOperation(BuildYdbOpNode(cluster, TYdbOperation::AlterGroup, alterGroup.Pos(), ctx));
        return true;
    }

    if (auto maybeRenameGroup = node.Maybe<TKiRenameGroup>()) {
        auto renameGroup = maybeRenameGroup.Cast();
        if (!checkDataSink(renameGroup.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        txRes.AddTableOperation(BuildYdbOpNode(cluster, TYdbOperation::RenameGroup, renameGroup.Pos(), ctx));
        return true;
    }

    if (auto maybeDropGroup = node.Maybe<TKiDropGroup>()) {
        auto dropGroup = maybeDropGroup.Cast();
        if (!checkDataSink(dropGroup.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        txRes.AddTableOperation(BuildYdbOpNode(cluster, TYdbOperation::DropGroup, dropGroup.Pos(), ctx));
        return true;
    }

    if (auto maybeExecQuery = node.Maybe<TKiExecDataQuery>()) {
        auto execQuery = maybeExecQuery.Cast();
        if (!checkDataSink(execQuery.DataSink())) {
            return false;
        }

        txRes.HasExecute = true;
        return true;
    }

    if (node.Maybe<TCoCommit>()) {
        return true;
    }

    if (node.Maybe<TCoSync>()) {
        txRes.Ops.insert(node.Raw());
        return true;
    }

    if (node.Maybe<TResWrite>() ||
        node.Maybe<TResPull>())
    {
        txRes.Ops.insert(node.Raw());
        txRes.AddResult(node);
        return true;
    }

    if (node.Ref().IsCallable(ConfigureName)) {
        return true;
    }

    if (node.Maybe<TCoCons>()) {
        txRes.Ops.insert(node.Raw());
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

bool ExploreTx(TExprBase root, TExprContext& ctx, const TKiDataSink& dataSink, TKiExploreTxResults& txRes,
    TIntrusivePtr<TKikimrTablesData> tablesData, TTypeAnnotationContext& types)
{
    const auto preFunc = [&dataSink, &txRes](const TExprNode::TPtr& node) {
        if (const auto maybeCommit = TExprBase(node).Maybe<TCoCommit>()) {
            const auto commit = maybeCommit.Cast();
            if (commit.DataSink().Maybe<TKiDataSink>() && commit.DataSink().Cast<TKiDataSink>().Raw() == dataSink.Raw()) {
                txRes.Sync.push_back(commit);
                return false;
            }
            return true;
        }

        if (node->IsCallable(ConfigureName)) {
            txRes.Sync.push_back(TExprBase(node));
            return false;
        }

        return true;
    };

    bool hasErrors = false;
    const auto postFunc = [&hasErrors, &ctx, &dataSink, &txRes, tablesData, &types](const TExprNode::TPtr& node) {
        if (hasErrors) {
            return false;
        }

        if (!ExploreNode(TExprBase(node), ctx, dataSink, txRes, tablesData, types) && !IsKikimrPureNode(node)) {
            hasErrors = true;
            return false;
        }

        return true;
    };

    VisitExpr(root.Ptr(), preFunc, postFunc);

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

TVector<TKiDataQueryBlock> MakeKiDataQueryBlocks(TExprBase node, const TKiExploreTxResults& txExplore, TExprContext& ctx, TTypeAnnotationContext& types) {
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
            .Operations()
                .Add(block.TableOperations)
                .Build()
            .Settings(settings.BuildNode(ctx, node.Pos()))
            .Done());
    }

    auto txSyncSet = txExplore.GetSyncSet();
    auto world = Build<TCoWorld>(ctx, node.Pos()).Done();

    for (auto& queryBlock : queryBlocks) {
        TExprNode::TPtr optResult;
        TOptimizeExprSettings optSettings(nullptr);
        optSettings.VisitChanges = true;
        auto status = OptimizeExpr(queryBlock.Ptr(), optResult,
            [world, &txSyncSet, &types](const TExprNode::TPtr& input, TExprContext& ctx) {
                auto node = TExprBase(input);

                if (txSyncSet.contains(node.Raw())) {
                    return world.Ptr();
                }

                if (node.Maybe<TKiReadTable>() ||
                    node.Maybe<TKiWriteTable>() ||
                    node.Maybe<TKiUpdateTable>() ||
                    node.Maybe<TKiDeleteTable>() ||
                    IsDqRead(node, ctx, types, false) ||
                    IsDqWrite(node, ctx, types))
                {
                    return ctx.ChangeChild(node.Ref(), 0, world.Ptr());
                }

                return node.Ptr();
            }, ctx, optSettings);

        YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);
        YQL_ENSURE(TMaybeNode<TKiDataQueryBlock>(optResult));
        queryBlock = TMaybeNode<TKiDataQueryBlock>(optResult).Cast();
    }

    return queryBlocks;
}

} // namespace

TExprNode::TPtr KiBuildQuery(TExprBase node, TExprContext& ctx, TStringBuf database, TIntrusivePtr<TKikimrTablesData> tablesData,
    TTypeAnnotationContext& types, bool concurrentResults) {
    if (!node.Maybe<TCoCommit>().DataSink().Maybe<TKiDataSink>()) {
        return node.Ptr();
    }

    auto commit = node.Cast<TCoCommit>();
    auto settings = NCommon::ParseCommitSettings(commit, ctx);
    auto kiDataSink = commit.DataSink().Cast<TKiDataSink>();

    TNodeOnNodeOwnedMap replaces;
    std::unordered_set<std::string> pgDynTables = {"pg_tables", "tables", "pg_class"};
    VisitExpr(node.Ptr(), [&replaces, &pgDynTables](const TExprNode::TPtr& input) -> bool {
        if (input->IsCallable("PgTableContent")) {
            TPgTableContent content(input);
            if (pgDynTables.contains(content.Table().StringValue())) {
                replaces[input.Get()] = nullptr;
            }
        }
        return true;
    });
    if (!replaces.empty()) {
        for (auto& [input, _] : replaces) {
            TPgTableContent content(input);

            TExprNode::TPtr path = ctx.NewCallable(
                node.Pos(),
                "String",
                { ctx.NewAtom(node.Pos(), TStringBuilder() << "/" << database << "/.sys/" << content.Table().StringValue()) }
            );
            auto table = ctx.NewList(node.Pos(), {ctx.NewAtom(node.Pos(), "table"), path});
            auto newKey = ctx.NewCallable(node.Pos(), "Key", {table});

            auto ydbSysTableRead = Build<TCoRead>(ctx, node.Pos())
                .World<TCoWorld>().Build()
                .DataSource<TCoDataSource>()
                    .Category(ctx.NewAtom(node.Pos(), KikimrProviderName))
                    .FreeArgs()
                        .Add(ctx.NewAtom(node.Pos(), "db"))
                    .Build()
                .Build()
                .FreeArgs()
                    .Add(newKey)
                    .Add(ctx.NewCallable(node.Pos(), "Void", {}))
                    .Add(ctx.NewList(node.Pos(), {}))
                .Build()
            .Done().Ptr();


            auto readData = Build<TCoRight>(ctx, node.Pos())
                .Input(ydbSysTableRead)
            .Done().Ptr();

            if (auto v = content.Columns().Maybe<TCoVoid>()) {
                replaces[input] = readData;
            } else {
                auto extractMembers = Build<TCoExtractMembers>(ctx, node.Pos())
                    .Input(readData)
                    .Members(content.Columns().Ptr())
                .Done().Ptr();

                replaces[input] = extractMembers;
            }
        }
        ctx.Step
            .Repeat(TExprStep::ExprEval)
            .Repeat(TExprStep::DiscoveryIO)
            .Repeat(TExprStep::Epochs)
            .Repeat(TExprStep::Intents)
            .Repeat(TExprStep::LoadTablesMetadata)
            .Repeat(TExprStep::RewriteIO);
        auto res = ctx.ReplaceNodes(std::move(node.Ptr()), replaces);
        return res;
    }

    TNodeOnNodeOwnedMap showCreateTableReadReplaces;
    VisitExpr(node.Ptr(), [&showCreateTableReadReplaces](const TExprNode::TPtr& input) -> bool {
        TExprBase currentNode(input);
        if (auto maybeReadTable = currentNode.Maybe<TKiReadTable>()) {
            auto readTable = maybeReadTable.Cast();
            for (auto setting : readTable.Settings()) {
                auto name = setting.Name().Value();
                if (name == "showCreateTable") {
                    showCreateTableReadReplaces[input.Get()] = nullptr;
                }
            }
        }
        return true;
    });

    if (!showCreateTableReadReplaces.empty()) {
        for (auto& [input, _] : showCreateTableReadReplaces) {
            TKiReadTable content(input);

            TExprNode::TPtr path = ctx.NewCallable(
                node.Pos(),
                "String",
                { ctx.NewAtom(node.Pos(), NKikimr::CanonizePath(NKikimr::JoinPath({TString(database), ".sys/show_create"}))) }
            );
            auto table = ctx.NewList(node.Pos(), {ctx.NewAtom(node.Pos(), "table"), path});
            auto newKey = ctx.NewCallable(node.Pos(), "Key", {table});

            TKikimrKey key(ctx);
            YQL_ENSURE(key.Extract(content.TableKey().Ref()));

            auto sysViewRewrittenValue = Build<TCoNameValueTuple>(ctx, node.Pos())
                .Name()
                    .Build("sysViewRewritten")
                .Value<TCoAtom>()
                    .Value(key.GetTablePath())
                    .Build()
                .Done();

            auto showCreateTableValue = Build<TCoNameValueTuple>(ctx, node.Pos())
                .Name()
                    .Build("showCreateTable")
                .Done();

            auto showCreateTableRead = Build<TCoRead>(ctx, node.Pos())
                .World<TCoWorld>().Build()
                .DataSource<TCoDataSource>()
                    .Category(ctx.NewAtom(node.Pos(), KikimrProviderName))
                    .FreeArgs()
                        .Add(ctx.NewAtom(node.Pos(), "db"))
                    .Build()
                .Build()
                .FreeArgs()
                    .Add(newKey)
                    .Add(ctx.NewCallable(node.Pos(), "Void", {}))
                    .Add(ctx.NewList(node.Pos(), {}))
                    .Add(sysViewRewrittenValue)
                    .Add(showCreateTableValue)
                .Build()
            .Done().Ptr();

            showCreateTableReadReplaces[input] = showCreateTableRead;
        }
        auto res = ctx.ReplaceNodes(std::move(node.Ptr()), showCreateTableReadReplaces);

        TExprBase resNode(res);

        TNodeOnNodeOwnedMap showCreateTableRightReplaces;
        VisitExpr(resNode.Ptr(), [&showCreateTableRightReplaces](const TExprNode::TPtr& input) -> bool {
            TExprBase currentNode(input);
            if (auto rightMaybe = currentNode.Maybe<TCoRight>()) {
                auto right = rightMaybe.Cast();
                if (auto maybeRead = right.Input().Maybe<TCoRead>()) {
                    auto read = maybeRead.Cast();
                    bool isSysViewRewritten = false;
                    bool isShowCreateTable = false;
                    for (auto arg : read.FreeArgs()) {
                        if (auto tuple = arg.Maybe<TCoNameValueTuple>()) {
                            auto name = tuple.Cast().Name().Value();
                            if (name == "sysViewRewritten") {
                                isSysViewRewritten = true;
                            } else if (name == "showCreateTable") {
                                isShowCreateTable = true;
                            }
                        }
                    }
                    if (isShowCreateTable && isSysViewRewritten) {
                        showCreateTableRightReplaces[input.Get()] = nullptr;
                    }
                }
            }
            return true;
        });

        for (auto& [input, _] : showCreateTableRightReplaces) {
            TCoRight right(input);
            TCoRead read(right.Input().Ptr());

            TString tablePath;
            for (auto arg : read.FreeArgs()) {
                if (auto tuple = arg.Maybe<TCoNameValueTuple>()) {
                    auto name = tuple.Cast().Name().Value();
                    if (name == "sysViewRewritten") {
                        tablePath = tuple.Cast().Value().Cast().Cast<TCoAtom>().StringValue();
                    }
                }
            }
            YQL_ENSURE(!tablePath.empty(), "Unexpected empty table path for SHOW CREATE TABLE");

            auto tempTablePath = tablesData->GetTempTablePath(tablePath);
            if (tempTablePath) {
                tablePath = tempTablePath.value();
            }

            auto showCreateArg = Build<TCoArgument>(ctx, resNode.Pos())
                .Name("_show_create_arg")
                .Done();

            TCoAtom columnPathAtom(ctx.NewAtom(resNode.Pos(), "Path"));
            auto columnPathArg = Build<TCoArgument>(ctx, resNode.Pos())
                .Name("_column_path_arg")
                .Done();
            auto columnPath = Build<TCoMember>(ctx, resNode.Pos())
                    .Struct(showCreateArg)
                    .Name(columnPathAtom)
                    .Done().Ptr();

            auto pathCondition = Build<TCoCmpEqual>(ctx, resNode.Pos())
                .Left(columnPath)
                .Right<TCoString>()
                    .Literal().Build(tablePath)
                .Build()
                .Done();

            TCoAtom columnPathTypeAtom(ctx.NewAtom(resNode.Pos(), "PathType"));
            auto columnPathType = Build<TCoMember>(ctx, resNode.Pos())
                    .Struct(showCreateArg)
                    .Name(columnPathTypeAtom)
                    .Done().Ptr();

            auto pathTypeCondition = Build<TCoCmpEqual>(ctx, resNode.Pos())
                .Left(columnPathType)
                .Right<TCoString>()
                    .Literal().Build("Table")
                .Build()
                .Done();

            auto lambda = Build<TCoLambda>(ctx, resNode.Pos())
                .Args({showCreateArg})
                .Body<TCoCoalesce>()
                    .Predicate<TCoAnd>()
                        .Add(pathCondition)
                        .Add(pathTypeCondition)
                        .Build()
                    .Value<TCoBool>()
                        .Literal().Build("false")
                        .Build()
                    .Build()
                .Done().Ptr();

            auto readData = Build<TCoRight>(ctx, resNode.Pos())
                .Input(right.Input().Ptr())
            .Done().Ptr();

            auto filterData = Build<TCoFilter>(ctx, resNode.Pos())
                .Input(readData)
                .Lambda(lambda)
            .Done().Ptr();

            showCreateTableRightReplaces[input] = filterData;
        }

        ctx.Step
            .Repeat(TExprStep::RewriteIO)
            .Repeat(TExprStep::ExprEval)
            .Repeat(TExprStep::DiscoveryIO)
            .Repeat(TExprStep::Epochs)
            .Repeat(TExprStep::Intents)
            .Repeat(TExprStep::LoadTablesMetadata)
            .Repeat(TExprStep::RewriteIO);

        return ctx.ReplaceNodes(std::move(resNode.Ptr()), showCreateTableRightReplaces);
    }

    TKiExploreTxResults txExplore;
    txExplore.ConcurrentResults = concurrentResults;
    if (!ExploreTx(commit.World(), ctx, kiDataSink, txExplore, tablesData, types) || txExplore.HasErrors) {
        return txExplore.HasErrors ? nullptr : node.Ptr();
    }

    if (txExplore.HasExecute) {
        return node.Ptr();
    }

    bool hasScheme;
    bool hasData;
    txExplore.GetTableOperations(hasScheme, hasData);

    if (hasData && hasScheme) {
        TString message = TStringBuilder() << "Queries with mixed data and scheme operations "
            << "are not supported. Use separate queries for different types of operations.";

        ctx.AddError(YqlIssue(ctx.GetPosition(commit.Pos()), TIssuesIds::KIKIMR_MIXED_SCHEME_DATA_TX, message));
        return nullptr;
    }

    if (hasScheme) {
        return MakeSchemeTx(commit, ctx);
    }

    auto dataQueryBlocks = MakeKiDataQueryBlocks(commit.World(), txExplore, ctx, types);

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
        .QueryBlocks()
            .Add(dataQueryBlocks)
            .Build()
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

        int resultIndex = 0;
        for (auto& block : txExplore.QueryBlocks) {
            for (size_t i = 0; i < block.Results.size(); ++i) {
                auto result = block.Results[i].Cast<TResWriteBase>();

                auto extractValue = Build<TCoNth>(ctx, node.Pos())
                    .Tuple(execRight)
                    .Index().Build(resultIndex)
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

                ++resultIndex;
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

TExprNode::TPtr KiBuildResult(TExprBase node, const TString& cluster, TExprContext& ctx) {
    if (auto maybeCommit = node.Maybe<TCoCommit>()) {
        auto world = maybeCommit.Cast().World();
        if (!world.Maybe<TResFill>()) {
            return node.Ptr();
        } else {
            node = world;
        }
    } else {
        return node.Ptr();
    }

    TKiExecDataQuerySettings execSettings;
    execSettings.Mode = KikimrCommitModeFlush(); /*because it is a pure query*/

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
        .Operations()
            .Build()
        .Settings()
            .Build()
        .Done();

    auto exec = Build<TKiExecDataQuery>(ctx, node.Pos())
        .World(resFill.World())
        .DataSink<TKiDataSink>()
            .Category().Build(KikimrProviderName)
            .Cluster().Build(cluster)
            .Build()
        .QueryBlocks()
            .Add({queryBlock})
            .Build()
        .Settings(execSettings.BuildNode(ctx, node.Pos()))
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

    auto newResFill = ctx.ChangeChild(*ctx.ChangeChild(resFill.Ref(), 0, world.Ptr()), 3, data.Ptr());
    auto resCommit = Build<TCoCommit>(ctx, node.Pos())
        .World(newResFill)
        .DataSink<TResultDataSink>()
            .Build()
        .Done();

    return Build<TCoCommit>(ctx, node.Pos())
        .World(resCommit)
        .DataSink<TKiDataSink>()
            .Category().Build(KikimrProviderName)
            .Cluster().Build(cluster)
            .Build()
        .Settings(execSettings.BuildNode(ctx, node.Pos()))
        .Done().Ptr();
}

TYdbOperation GetTableOp(const TKiWriteTable& write) {
    auto mode = write.Mode().Value();
    if (mode == "upsert") {
        return TYdbOperation::Upsert;
    } else if (mode == "replace") {
        return TYdbOperation::Replace;
    } else if (mode == "insert_revert") {
        return TYdbOperation::InsertRevert;
    } else if (mode == "insert_abort" || mode == "append") {
        return TYdbOperation::InsertAbort;
    } else if (mode == "delete_on") {
        return TYdbOperation::DeleteOn;
    } else if (mode == "update_on") {
        return TYdbOperation::UpdateOn;
    }

    YQL_ENSURE(false, "Unexpected TKiWriteTable mode: " << mode);
}

} // namespace NYql
