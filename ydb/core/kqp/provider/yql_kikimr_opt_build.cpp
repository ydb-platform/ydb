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

struct TKiExploreTxResults {
    THashSet<const TExprNode*> Ops;
    TVector<TExprBase> Sync;
    TVector<TExprBase> Results;
    TVector<TExprBase> Effects;
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

    TKiExploreTxResults()
        : HasExecute(false) {}
};

bool ExploreTx(TExprBase node, TExprContext& ctx, const TKiDataSink& dataSink, TKiExploreTxResults& txRes) {
    if (txRes.Ops.cend() != txRes.Ops.find(node.Raw())) {
        return true;
    }

    if (node.Maybe<TCoWorld>()) {
        txRes.Ops.insert(node.Raw());
        return true;
    }

    if (auto maybeLeft = node.Maybe<TCoLeft>()) {
        txRes.Ops.insert(node.Raw());
        return ExploreTx(maybeLeft.Cast().Input(), ctx, dataSink, txRes);
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
        auto result = ExploreTx(maybeRead.Cast().World(), ctx, dataSink, txRes);

        if (const auto& view = key.GetView()) {
            auto indexTable = IKikimrGateway::CreateIndexTablePath(table, view.GetRef());
            txRes.TableOperations.push_back(BuildTableOpNode(cluster, indexTable, TYdbOperation::Select, read.Pos(), ctx));
        }
        txRes.TableOperations.push_back(BuildTableOpNode(cluster, table, TYdbOperation::Select, read.Pos(), ctx));
        return result;
    }

    if (auto maybeWrite = node.Maybe<TKiWriteTable>()) {
        auto write = maybeWrite.Cast();
        if (!checkDataSink(write.DataSink())) {
            return false;
        }

        auto table = write.Table().Value();
        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(write.World(), ctx, dataSink, txRes);
        auto tableOp = GetTableOp(write);
        txRes.TableOperations.push_back(BuildTableOpNode(cluster, table, tableOp, write.Pos(), ctx));
        txRes.Effects.push_back(node);
        return result;
    }

    if (auto maybeUpdate = node.Maybe<TKiUpdateTable>()) {
        auto update = maybeUpdate.Cast();
        if (!checkDataSink(update.DataSink())) {
            return false;
        }

        auto table = update.Table().Value();
        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(update.World(), ctx, dataSink, txRes);
        txRes.TableOperations.push_back(BuildTableOpNode(cluster, table, TYdbOperation::Update, update.Pos(), ctx));
        txRes.Effects.push_back(node);
        return result;
    }

    if (auto maybeDelete = node.Maybe<TKiDeleteTable>()) {
        auto del = maybeDelete.Cast();
        if (!checkDataSink(del.DataSink())) {
            return false;
        }

        auto table = del.Table().Value();
        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(del.World(), ctx, dataSink, txRes);
        txRes.TableOperations.push_back(BuildTableOpNode(cluster, table, TYdbOperation::Delete, del.Pos(), ctx));
        txRes.Effects.push_back(node);
        return result;
    }

    if (auto maybeCreate = node.Maybe<TKiCreateTable>()) {
        auto create = maybeCreate.Cast();
        if (!checkDataSink(create.DataSink())) {
            return false;
        }

        auto table = create.Table().Value();
        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(create.World(), ctx, dataSink, txRes);
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
        auto result = ExploreTx(drop.World(), ctx, dataSink, txRes);
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
        auto result = ExploreTx(alter.World(), ctx, dataSink, txRes);
        txRes.TableOperations.push_back(BuildTableOpNode(cluster, table, TYdbOperation::AlterTable, alter.Pos(), ctx));
        return result;
    }

    if (auto maybeCreateUser = node.Maybe<TKiCreateUser>()) {
        auto createUser = maybeCreateUser.Cast();
        if (!checkDataSink(createUser.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(createUser.World(), ctx, dataSink, txRes);
        txRes.TableOperations.push_back(BuildYdbOpNode(cluster, TYdbOperation::CreateUser, createUser.Pos(), ctx));
        return result;
    }

    if (auto maybeAlterUser = node.Maybe<TKiAlterUser>()) {
        auto alterUser = maybeAlterUser.Cast();
        if (!checkDataSink(alterUser.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(alterUser.World(), ctx, dataSink, txRes);
        txRes.TableOperations.push_back(BuildYdbOpNode(cluster, TYdbOperation::AlterUser, alterUser.Pos(), ctx));
        return result;
    }

    if (auto maybeDropUser = node.Maybe<TKiDropUser>()) {
        auto dropUser = maybeDropUser.Cast();
        if (!checkDataSink(dropUser.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(dropUser.World(), ctx, dataSink, txRes);
        txRes.TableOperations.push_back(BuildYdbOpNode(cluster, TYdbOperation::DropUser, dropUser.Pos(), ctx));
        return result;
    }

    if (auto maybeCreateGroup = node.Maybe<TKiCreateGroup>()) {
        auto createGroup = maybeCreateGroup.Cast();
        if (!checkDataSink(createGroup.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(createGroup.World(), ctx, dataSink, txRes);
        txRes.TableOperations.push_back(BuildYdbOpNode(cluster, TYdbOperation::CreateGroup, createGroup.Pos(), ctx));
        return result;
    }

    if (auto maybeAlterGroup = node.Maybe<TKiAlterGroup>()) {
        auto alterGroup = maybeAlterGroup.Cast();
        if (!checkDataSink(alterGroup.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(alterGroup.World(), ctx, dataSink, txRes);
        txRes.TableOperations.push_back(BuildYdbOpNode(cluster, TYdbOperation::AlterGroup, alterGroup.Pos(), ctx));
        return result;
    }

    if (auto maybeDropGroup = node.Maybe<TKiDropGroup>()) {
        auto dropGroup = maybeDropGroup.Cast();
        if (!checkDataSink(dropGroup.DataSink())) {
            return false;
        }

        txRes.Ops.insert(node.Raw());
        auto result = ExploreTx(dropGroup.World(), ctx, dataSink, txRes);
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

        return ExploreTx(commit.World(), ctx, dataSink, txRes);
    }

    if (auto maybeSync = node.Maybe<TCoSync>()) {
        txRes.Ops.insert(node.Raw());
        for (auto child : maybeSync.Cast()) {
            if (!ExploreTx(child, ctx, dataSink, txRes)) {
                return false;
            }

            return true;
        }
    }

    if (node.Maybe<TResWrite>() ||
        node.Maybe<TResPull>())
    {
        txRes.Ops.insert(node.Raw());
        bool result = ExploreTx(TExprBase(node.Ref().ChildPtr(0)), ctx, dataSink, txRes);
        txRes.Results.push_back(node);
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
    TExprNode::TListType queryResults;
    for (auto& result : txExplore.Results) {
        auto resWrite = result.Cast<TResWriteBase>();

        auto kiResult = Build<TKiResult>(ctx, node.Pos())
            .Value(resWrite.Data())
            .Columns(GetResultColumns(resWrite, ctx))
            .RowsLimit().Build(GetResultRowsLimit(resWrite))
            .Done();

        queryResults.push_back(kiResult.Ptr());
    }

    auto query = Build<TKiDataQuery>(ctx, node.Pos())
        .Operations()
            .Add(txExplore.TableOperations)
            .Build()
        .Results()
            .Add(queryResults)
            .Build()
        .Effects()
            .Add(txExplore.Effects)
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

TExprNode::TPtr KiBuildQuery(TExprBase node, const TMaybe<bool>& useNewEngine, TExprContext& ctx) {
    if (!node.Maybe<TCoCommit>().DataSink().Maybe<TKiDataSink>()) {
        return node.Ptr();
    }

    auto commit = node.Cast<TCoCommit>();
    auto settings = NCommon::ParseCommitSettings(commit, ctx);
    auto kiDataSink = commit.DataSink().Cast<TKiDataSink>();

    TKiExploreTxResults txExplore;
    if (!ExploreTx(commit.World(), ctx, kiDataSink, txExplore)) {
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
    execSettings.UseNewEngine = useNewEngine;

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

    if (!txExplore.Results.empty()) {
        auto execRight = Build<TCoRight>(ctx, node.Pos())
            .Input(execQuery)
            .Done()
            .Ptr();

        for (size_t i = 0; i < txExplore.Results.size(); ++i) {
            auto result = txExplore.Results[i].Cast<TResWriteBase>();

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

    auto dataQuery = Build<TKiDataQuery>(ctx, node.Pos())
        .Operations()
            .Build()
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
