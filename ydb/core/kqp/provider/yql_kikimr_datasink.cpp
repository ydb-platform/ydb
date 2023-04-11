#include "yql_kikimr_provider_impl.h"

#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>

namespace NYql {
namespace {

using namespace NKikimr;
using namespace NNodes;

class TKiSinkIntentDeterminationTransformer: public TKiSinkVisitorTransformer {
public:
    TKiSinkIntentDeterminationTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx)
        : SessionCtx(sessionCtx) {}

private:
    TStatus HandleWriteTable(TKiWriteTable node, TExprContext& ctx) override {
        Y_UNUSED(ctx);

        auto cluster = node.DataSink().Cluster();
        auto table = node.Table();

        SessionCtx->Tables().GetOrAddTable(TString(cluster), SessionCtx->GetDatabase(), TString(table));
        return TStatus::Ok;
    }

    TStatus HandleUpdateTable(TKiUpdateTable node, TExprContext& ctx) override {
        Y_UNUSED(ctx);

        auto cluster = node.DataSink().Cluster();
        auto table = node.Table();

        SessionCtx->Tables().GetOrAddTable(TString(cluster), SessionCtx->GetDatabase(), TString(table));
        return TStatus::Ok;
    }

    TStatus HandleDeleteTable(TKiDeleteTable node, TExprContext& ctx) override {
        Y_UNUSED(ctx);

        auto cluster = node.DataSink().Cluster();
        auto table = node.Table();

        SessionCtx->Tables().GetOrAddTable(TString(cluster), SessionCtx->GetDatabase(), TString(table));
        return TStatus::Ok;
    }

    TStatus HandleCreateTable(TKiCreateTable node, TExprContext& ctx) override {
        Y_UNUSED(ctx);

        auto cluster = node.DataSink().Cluster();
        auto table = node.Table();

        SessionCtx->Tables().GetOrAddTable(TString(cluster), SessionCtx->GetDatabase(), TString(table));
        return TStatus::Ok;
    }

    TStatus HandleAlterTable(TKiAlterTable node, TExprContext& ctx) override {
        Y_UNUSED(ctx);

        auto cluster = node.DataSink().Cluster();
        auto table = node.Table();

        SessionCtx->Tables().GetOrAddTable(TString(cluster), SessionCtx->GetDatabase(), TString(table));
        return TStatus::Ok;
    }

    TStatus HandleDropTable(TKiDropTable node, TExprContext& ctx) override {
        Y_UNUSED(ctx);

        auto cluster = node.DataSink().Cluster();
        auto table = node.Table();

        SessionCtx->Tables().GetOrAddTable(TString(cluster), SessionCtx->GetDatabase(), TString(table));
        return TStatus::Ok;
    }

    TStatus HandleCreateTopic(TKiCreateTopic node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        Y_UNUSED(node);
        return TStatus::Ok;
    }

    TStatus HandleAlterTopic(TKiAlterTopic node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        Y_UNUSED(node);
        return TStatus::Ok;
    }

    TStatus HandleDropTopic(TKiDropTopic node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        Y_UNUSED(node);
        return TStatus::Ok;
    }

    TStatus HandleCreateUser(TKiCreateUser node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "CreateUser is not yet implemented for intent determination transformer"));
        return TStatus::Error;
    }

    TStatus HandleAlterUser(TKiAlterUser node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "AlterUser is not yet implemented for intent determination transformer"));
        return TStatus::Error;
    }

    TStatus HandleDropUser(TKiDropUser node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "DropUser is not yet implemented for intent determination transformer"));
        return TStatus::Error;
    }

    TStatus HandleCreateObject(TKiCreateObject node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "CreateObject is not yet implemented for intent determination transformer"));
        return TStatus::Error;
    }

    TStatus HandleAlterObject(TKiAlterObject node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "AlterObject is not yet implemented for intent determination transformer"));
        return TStatus::Error;
    }

    TStatus HandleDropObject(TKiDropObject node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "DropObject is not yet implemented for intent determination transformer"));
        return TStatus::Error;
    }

    TStatus HandleCreateGroup(TKiCreateGroup node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "CreateGroup is not yet implemented for intent determination transformer"));
        return TStatus::Error;
    }

    TStatus HandleAlterGroup(TKiAlterGroup node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "AlterGroup is not yet implemented for intent determination transformer"));
        return TStatus::Error;
    }

    TStatus HandleDropGroup(TKiDropGroup node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "DropGroup is not yet implemented for intent determination transformer"));
        return TStatus::Error;
    }

    TStatus HandleWrite(TExprBase node, TExprContext& ctx) override {
        auto cluster = node.Ref().Child(1)->Child(1)->Content();
        TKikimrKey key(ctx);
        if (!key.Extract(*node.Ref().Child(2))) {
            return TStatus::Error;
        }

        switch (key.GetKeyType()) {
            case TKikimrKey::Type::Table: {
                NCommon::TWriteTableSettings settings = NCommon::ParseWriteTableSettings(
                    TExprList(node.Ref().ChildPtr(4)), ctx);
                if (!settings.Mode) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                        << "Mode option is required for Kikimr table writes."));
                    return TStatus::Error;
                }
                auto mode = settings.Mode.Cast();

                if (mode == "drop") {
                    auto tableType = settings.TableType.IsValid()
                        ? GetTableTypeFromString(settings.TableType.Cast())
                        : ETableType::Table; // v0 support
                    SessionCtx->Tables().GetOrAddTable(TString(cluster), SessionCtx->GetDatabase(), key.GetTablePath(), tableType);
                    return TStatus::Ok;
                } else if (
                    mode == "upsert" ||
                    mode == "replace" ||
                    mode == "insert_revert" ||
                    mode == "insert_abort" ||
                    mode == "delete_on" ||
                    mode == "update_on")
                {
                    SessionCtx->Tables().GetOrAddTable(TString(cluster), SessionCtx->GetDatabase(), key.GetTablePath());
                    return TStatus::Ok;
                } else if (mode == "insert_ignore") {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                        << "INSERT OR IGNORE is not yet supported for Kikimr."));
                    return TStatus::Error;
                } else if (mode == "update") {
                    if (!settings.Filter) {
                        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Filter option is required for table update."));
                        return TStatus::Error;
                    }
                    if (!settings.Update) {
                        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Update option is required for table update."));
                        return TStatus::Error;
                    }
                    SessionCtx->Tables().GetOrAddTable(TString(cluster), SessionCtx->GetDatabase(), key.GetTablePath());
                    return TStatus::Ok;
                } else if (mode == "delete") {
                    if (!settings.Filter) {
                        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Filter option is required for table delete."));
                        return TStatus::Error;
                    }
                    SessionCtx->Tables().GetOrAddTable(TString(cluster), SessionCtx->GetDatabase(), key.GetTablePath());
                    return TStatus::Ok;
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                        << "Unsupported Kikimr table write mode: " << settings.Mode.Cast().Value()));
                    return TStatus::Error;
                }
            }

            case TKikimrKey::Type::TableScheme: {
                NCommon::TWriteTableSettings settings = NCommon::ParseWriteTableSettings(
                    TExprList(node.Ref().ChildPtr(4)), ctx);
                if (!settings.Mode) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                        << "Mode option is required for Kikimr scheme writes."));
                    return TStatus::Error;
                }

                auto mode = settings.Mode.Cast();

                auto tableType = settings.TableType.IsValid()
                    ? GetTableTypeFromString(settings.TableType.Cast())
                    : ETableType::Table; // v0 support

                if (mode == "create") {
                    if (!settings.Columns) {
                        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                            << "No columns provided for create mode."));
                        return TStatus::Error;
                    }

                    SessionCtx->Tables().GetOrAddTable(TString(cluster), SessionCtx->GetDatabase(), key.GetTablePath(), tableType);
                    return TStatus::Ok;
                } else if (mode == "alter") {
                    if (!settings.AlterActions) {
                        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                            << "No actions provided for alter mode."));
                        return TStatus::Error;
                    }

                    SessionCtx->Tables().GetOrAddTable(TString(cluster), SessionCtx->GetDatabase(), key.GetTablePath(), tableType);
                    return TStatus::Ok;
                }

                ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                    << "Unsupported Kikimr scheme write mode: " << settings.Mode.Cast().Value()));
                return TStatus::Error;
            }

            case TKikimrKey::Type::TableList:
                break;

            case TKikimrKey::Type::Role:
                return TStatus::Ok;
            case TKikimrKey::Type::Object:
                return TStatus::Ok;
            case TKikimrKey::Type::Topic:
                return TStatus::Ok;
        }

        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid table key type."));
        return TStatus::Error;
    }

    TStatus HandleCommit(TCoCommit node, TExprContext& ctx) override {
        Y_UNUSED(node);
        Y_UNUSED(ctx);

        return TStatus::Ok;
    }

    TStatus HandleDataQueryBlock(TKiDataQueryBlock node, TExprContext& ctx) override {
        Y_UNUSED(ctx);

        for (const auto& op : node.Operations()) {
            SessionCtx->Tables().GetOrAddTable(TString(op.Cluster()), SessionCtx->GetDatabase(), TString(op.Table()));
        }

        return TStatus::Ok;
    }

    TStatus HandleDataQueryBlocks(TKiDataQueryBlocks node, TExprContext& ctx) override {
        Y_UNUSED(node);
        Y_UNUSED(ctx);

        return TStatus::Ok;
    }

    TStatus HandleExecDataQuery(TKiExecDataQuery node, TExprContext& ctx) override {
        Y_UNUSED(node);
        Y_UNUSED(ctx);

        return TStatus::Ok;
    }

    TStatus HandleEffects(TKiEffects node, TExprContext& ctx) override {
        Y_UNUSED(node);
        Y_UNUSED(ctx);

        return TStatus::Ok;
    }

private:
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
};

class TKikimrDataSink : public TDataProviderBase
{
public:
    TKikimrDataSink(
        const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
        TTypeAnnotationContext& types,
        TIntrusivePtr<IKikimrGateway> gateway,
        TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        TIntrusivePtr<IKikimrQueryExecutor> queryExecutor)
        : FunctionRegistry(functionRegistry)
        , Types(types)
        , Gateway(gateway)
        , SessionCtx(sessionCtx)
        , IntentDeterminationTransformer(CreateKiSinkIntentDeterminationTransformer(sessionCtx))
        , TypeAnnotationTransformer(CreateKiSinkTypeAnnotationTransformer(gateway, sessionCtx))
        , LogicalOptProposalTransformer(CreateKiLogicalOptProposalTransformer(sessionCtx, types))
        , PhysicalOptProposalTransformer(CreateKiPhysicalOptProposalTransformer(sessionCtx))
        , CallableExecutionTransformer(CreateKiSinkCallableExecutionTransformer(gateway, sessionCtx, queryExecutor))
        , PlanInfoTransformer(CreateKiSinkPlanInfoTransformer(queryExecutor))
    {
        Y_UNUSED(FunctionRegistry);
        Y_UNUSED(Types);

        Y_VERIFY_DEBUG(gateway);
        Y_VERIFY_DEBUG(sessionCtx);
        Y_VERIFY_DEBUG(queryExecutor);
    }

    ~TKikimrDataSink() {}

    TStringBuf GetName() const override {
        return KikimrProviderName;
    }

    TExprNode::TPtr GetClusterInfo(const TString& cluster, TExprContext& ctx) override {
        Y_UNUSED(cluster);
        Y_UNUSED(ctx);
        return {};
    }

    IGraphTransformer& GetIntentDeterminationTransformer() override {
        return *IntentDeterminationTransformer;
    }

    IGraphTransformer& GetLogicalOptProposalTransformer() override {
        return *LogicalOptProposalTransformer;
    }

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        return *PhysicalOptProposalTransformer;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotationTransformer;
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *CallableExecutionTransformer;
    }

    IGraphTransformer& GetPlanInfoTransformer() override {
        return *PlanInfoTransformer;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSink::CallableName())) {
            if (node.Child(0)->Content() == YdbProviderName) {
                node.ChildRef(0) = ctx.RenameNode(*node.Child(0), KikimrProviderName);
            }

            if (node.Child(0)->Content() == KikimrProviderName) {
                if (node.Child(1)->Content().empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()), "Empty cluster name"));
                    return false;
                }

                cluster = TString(node.Child(1)->Content());
                return true;
            }
        }

        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Kikimr DataSink parameters."));
        return false;
    }

    bool CanParse(const TExprNode& node) override {
        if (node.IsCallable(WriteName)) {
            return node.Child(1)->Child(0)->Content() == KikimrProviderName;
        }

        if (KikimrDataSinkFunctions().contains(node.Content())) {
            return true;
        }

        return false;
    }

    bool CanExecute(const TExprNode& node) override {
        if (node.IsCallable(TKiExecDataQuery::CallableName())) {
            return true;
        }

        if (node.IsCallable(TKiCreateTable::CallableName())
            || node.IsCallable(TKiDropTable::CallableName())
            || node.IsCallable(TKiAlterTable::CallableName())) {
            return true;
        }
        if (node.IsCallable(TKiCreateTopic::CallableName())
            || node.IsCallable(TKiAlterTopic::CallableName())
            || node.IsCallable(TKiDropTopic::CallableName())
        ) {
            return true;
        }

        if (node.IsCallable(TKiCreateUser::CallableName())
            || node.IsCallable(TKiAlterUser::CallableName())
            || node.IsCallable(TKiDropUser::CallableName())
            || node.IsCallable(TKiCreateGroup::CallableName())
            || node.IsCallable(TKiAlterGroup::CallableName())
            || node.IsCallable(TKiDropGroup::CallableName())
            || node.IsCallable(TKiCreateObject::CallableName())
            || node.IsCallable(TKiAlterObject::CallableName())
            || node.IsCallable(TKiDropObject::CallableName()))
{
            return true;
        }

        if (auto maybeRight = TMaybeNode<TCoNth>(&node).Tuple().Maybe<TCoRight>()) {
            if (maybeRight.Input().Maybe<TKiExecDataQuery>()) {
                return true;
            }
        }

        return false;
    }

    bool CollectDiagnostics(NYson::TYsonWriter& writer) override {
        Y_UNUSED(writer);
        return false;
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        YQL_ENSURE(node->IsCallable(WriteName), "Expected Write!, got: " << node->Content());

        TKikimrKey key(ctx);
        YQL_ENSURE(key.Extract(*node->Child(2)), "Failed to extract ydb key.");

        switch (key.GetKeyType()) {
            case TKikimrKey::Type::Table: {
                NCommon::TWriteTableSettings settings = NCommon::ParseWriteTableSettings(TExprList(node->Child(4)), ctx);
                YQL_ENSURE(settings.Mode);
                auto mode = settings.Mode.Cast();

                if (mode == "drop") {
                    YQL_ENSURE(!settings.Columns);
                    auto tableType = settings.TableType.IsValid()
                        ? settings.TableType.Cast()
                        : Build<TCoAtom>(ctx, node->Pos()).Value("table").Done(); // v0 support
                    return Build<TKiDropTable>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .Table().Build(key.GetTablePath())
                        .Settings(settings.Other)
                        .TableType(tableType)
                        .Done()
                        .Ptr();
                } else if (mode == "update") {
                    YQL_ENSURE(settings.Filter);
                    YQL_ENSURE(settings.Update);
                    return Build<TKiUpdateTable>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .Table().Build(key.GetTablePath())
                        .Filter(settings.Filter.Cast())
                        .Update(settings.Update.Cast())
                        .Done()
                        .Ptr();
                } else if (mode == "delete") {
                    YQL_ENSURE(settings.Filter);
                    return Build<TKiDeleteTable>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .Table().Build(key.GetTablePath())
                        .Filter(settings.Filter.Cast())
                        .Done()
                        .Ptr();
                } else {
                    return Build<TKiWriteTable>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .Table().Build(key.GetTablePath())
                        .Input(node->Child(3))
                        .Mode(mode)
                        .Settings(settings.Other)
                        .Done()
                        .Ptr();
                }
            }

            case TKikimrKey::Type::TableScheme: {
                NCommon::TWriteTableSettings settings = NCommon::ParseWriteTableSettings(TExprList(node->Child(4)), ctx);
                YQL_ENSURE(settings.Mode);
                auto tableType = settings.TableType.IsValid()
                    ? settings.TableType.Cast()
                    : Build<TCoAtom>(ctx, node->Pos()).Value("table").Done(); // v0 support
                auto mode = settings.Mode.Cast();
                if (mode == "create") {
                    YQL_ENSURE(settings.Columns);
                    YQL_ENSURE(!settings.Columns.Cast().Empty());

                    const bool isExternalTable = settings.TableType && settings.TableType.Cast() == "externalTable";
                    if (!isExternalTable && !settings.PrimaryKey) {
                        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Primary key is required for ydb tables."));
                        return nullptr;
                    }

                    if (!settings.PrimaryKey.IsValid()) {
                        settings.PrimaryKey = Build<TCoAtomList>(ctx, node->Pos()).Done();
                    }

                    if (!settings.PartitionBy.IsValid()) {
                        settings.PartitionBy = Build<TCoAtomList>(ctx, node->Pos()).Done();
                    }

                    if (!settings.NotNullColumns.IsValid()) {
                        settings.NotNullColumns = Build<TCoAtomList>(ctx, node->Pos()).Done();
                    }

                    return Build<TKiCreateTable>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .Table().Build(key.GetTablePath())
                        .Columns(settings.Columns.Cast())
                        .PrimaryKey(settings.PrimaryKey.Cast())
                        .NotNullColumns(settings.NotNullColumns.Cast())
                        .Settings(settings.Other)
                        .Indexes(settings.Indexes.Cast())
                        .Changefeeds(settings.Changefeeds.Cast())
                        .PartitionBy(settings.PartitionBy.Cast())
                        .ColumnFamilies(settings.ColumnFamilies.Cast())
                        .TableSettings(settings.TableSettings.Cast())
                        .TableType(tableType)
                        .Done()
                        .Ptr();
                } else if (mode == "alter") {
                    for (auto setting : settings.Other) {
                        if (setting.Name().Value() == "intent") {
                            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Old AST format for AlterTable"));
                            return nullptr;
                        }
                    }

                    YQL_ENSURE(settings.AlterActions);
                    YQL_ENSURE(!settings.AlterActions.Cast().Empty());

                    return Build<TKiAlterTable>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .Table().Build(key.GetTablePath())
                        .Actions(settings.AlterActions.Cast())
                        .TableType(tableType)
                        .Done()
                        .Ptr();
                } else {
                    YQL_ENSURE(false, "unknown TableScheme mode \"" << TString(mode) << "\"");
                }
            }

            case TKikimrKey::Type::TableList:
                break;

            case TKikimrKey::Type::Topic: {
                NCommon::TWriteTopicSettings settings = NCommon::ParseWriteTopicSettings(TExprList(node->Child(4)), ctx);
                YQL_ENSURE(settings.Mode);
                auto mode = settings.Mode.Cast();

                if (mode == "create") {
                    return Build<TKiCreateTopic>(ctx, node->Pos())
                            .World(node->Child(0))
                            .DataSink(node->Child(1))
                            .Topic().Build(key.GetTopicPath())
                            .TopicSettings(settings.TopicSettings.Cast())
                            .Consumers(settings.Consumers.Cast())
                            .Settings(settings.Other)
                            .Done()
                            .Ptr();
                } else if (mode == "alter") {
                    return Build<TKiAlterTopic>(ctx, node->Pos())
                            .World(node->Child(0))
                            .DataSink(node->Child(1))
                            .Topic().Build(key.GetTopicPath())
                            .TopicSettings(settings.TopicSettings.Cast())
                            .AddConsumers(settings.AddConsumers.Cast())
                            .AlterConsumers(settings.AlterConsumers.Cast())
                            .DropConsumers(settings.DropConsumers.Cast())
                            .Settings(settings.Other)
                            .Done()
                            .Ptr();
                } else if (mode == "drop") {
                        return Build<TKiDropTopic>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .Topic().Build(key.GetTopicPath())
                        .Settings(settings.Other)
                        .Done()
                        .Ptr();
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Unknown operation type for topic"));
                    return nullptr;
                }
                break;
            }
            case TKikimrKey::Type::Object:
            {
                NCommon::TWriteObjectSettings settings = NCommon::ParseWriteObjectSettings(TExprList(node->Child(4)), ctx);
                YQL_ENSURE(settings.Mode);
                auto mode = settings.Mode.Cast();

                if (mode == "createObject") {
                    return Build<TKiCreateObject>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .ObjectId().Build(key.GetObjectId())
                        .TypeId().Build(key.GetObjectType())
                        .Features(settings.Features)
                        .Done()
                        .Ptr();
                } else if (mode == "alterObject") {
                    return Build<TKiAlterObject>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .ObjectId().Build(key.GetObjectId())
                        .TypeId().Build(key.GetObjectType())
                        .Features(settings.Features)
                        .Done()
                        .Ptr();
                } else if (mode == "dropObject") {
                    return Build<TKiDropObject>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .ObjectId().Build(key.GetObjectId())
                        .TypeId().Build(key.GetObjectType())
                        .Features(settings.Features)
                        .Done()
                        .Ptr();
                } else {
                    YQL_ENSURE(false, "unknown Object operation mode \"" << TString(mode) << "\"");
                }
                break;
            }

            case TKikimrKey::Type::Role: {
                NCommon::TWriteRoleSettings settings = NCommon::ParseWriteRoleSettings(TExprList(node->Child(4)), ctx);
                YQL_ENSURE(settings.Mode);
                auto mode = settings.Mode.Cast();

                if (mode == "createUser") {
                    return Build<TKiCreateUser>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .UserName().Build(key.GetRoleName())
                        .Settings(settings.Other)
                        .Done()
                        .Ptr();
                } else if (mode == "alterUser") {
                    return Build<TKiAlterUser>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .UserName().Build(key.GetRoleName())
                        .Settings(settings.Other)
                        .Done()
                        .Ptr();
                } else if (mode == "dropUser") {
                    return Build<TKiDropUser>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .UserName().Build(key.GetRoleName())
                        .Settings(settings.Other)
                        .Done()
                        .Ptr();
                } else if (mode == "createGroup") {
                    return Build<TKiCreateGroup>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .GroupName().Build(key.GetRoleName())
                        .Done()
                        .Ptr();
                } else if (mode == "addUsersToGroup" || mode == "dropUsersFromGroup") {
                    return Build<TKiAlterGroup>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .GroupName().Build(key.GetRoleName())
                        .Action().Build(mode)
                        .Roles(settings.Roles.Cast())
                        .Done()
                        .Ptr();
                } else if (mode == "dropGroup") {
                    return Build<TKiDropGroup>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .GroupName().Build(key.GetRoleName())
                        .Settings(settings.Other)
                        .Done()
                        .Ptr();
                } else {
                    YQL_ENSURE(false, "unknown Role mode \"" << TString(mode) << "\"");
                }
                break;
            }
        }

        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Failed to rewrite IO."));
        return nullptr;
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(compact);
        if (CanExecute(node)) {
            children.push_back(node.ChildPtr(0));
            return true;
        }

        return false;
    }

    void GetInputs(const TExprNode&, TVector<TPinInfo>&) override {
    }

    void GetOutputs(const TExprNode&, TVector<TPinInfo>&) override {
    }

    void WritePlanDetails(const TExprNode&, NYson::TYsonWriter&) override {
    }

    TString GetProviderPath(const TExprNode&) override {
        return TString(KikimrProviderName);
    }

private:
    const NKikimr::NMiniKQL::IFunctionRegistry& FunctionRegistry;
    const TTypeAnnotationContext& Types;
    TIntrusivePtr<IKikimrGateway> Gateway;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;

    TAutoPtr<IGraphTransformer> IntentDeterminationTransformer;
    TAutoPtr<IGraphTransformer> TypeAnnotationTransformer;
    TAutoPtr<IGraphTransformer> LogicalOptProposalTransformer;
    TAutoPtr<IGraphTransformer> PhysicalOptProposalTransformer;
    TAutoPtr<IGraphTransformer> CallableExecutionTransformer;
    TAutoPtr<IGraphTransformer> PlanInfoTransformer;
};

} // namespace

IGraphTransformer::TStatus TKiSinkVisitorTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output,
    TExprContext& ctx)
{
    YQL_ENSURE(input->Type() == TExprNode::Callable);
    output = input;

    auto callable = TCallable(input);

    if (auto node = callable.Maybe<TKiWriteTable>()) {
        return HandleWriteTable(node.Cast(), ctx);
    }

    if (auto node = callable.Maybe<TKiUpdateTable>()) {
        return HandleUpdateTable(node.Cast(), ctx);
    }

    if (auto node = callable.Maybe<TKiDeleteTable>()) {
        return HandleDeleteTable(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiCreateTable>(input)) {
        return HandleCreateTable(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiAlterTable>(input)) {
        return HandleAlterTable(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiDropTable>(input)) {
        return HandleDropTable(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiCreateTopic>(input)) {
        return HandleCreateTopic(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiAlterTopic>(input)) {
        return HandleAlterTopic(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiDropTopic>(input)) {
        return HandleDropTopic(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiCreateObject>(input)) {
        return HandleCreateObject(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiAlterObject>(input)) {
        return HandleAlterObject(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiDropObject>(input)) {
        return HandleDropObject(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiCreateUser>(input)) {
        return HandleCreateUser(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiAlterUser>(input)) {
        return HandleAlterUser(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiDropUser>(input)) {
        return HandleDropUser(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiCreateGroup>(input)) {
        return HandleCreateGroup(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiAlterGroup>(input)) {
        return HandleAlterGroup(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiDropGroup>(input)) {
        return HandleDropGroup(node.Cast(), ctx);
    }

    if (input->IsCallable(WriteName)) {
        return HandleWrite(TExprBase(input), ctx);
    }

    if (auto node = callable.Maybe<TCoCommit>()) {
        return HandleCommit(node.Cast(), ctx);
    }

    if (auto node = callable.Maybe<TKiDataQueryBlock>()) {
        return HandleDataQueryBlock(node.Cast(), ctx);
    }

    if (auto node = callable.Maybe<TKiDataQueryBlocks>()) {
        return HandleDataQueryBlocks(node.Cast(), ctx);
    }

    if (auto node = callable.Maybe<TKiExecDataQuery>()) {
        return HandleExecDataQuery(node.Cast(), ctx);
    }

    if (auto node = callable.Maybe<TKiEffects>()) {
        return HandleEffects(node.Cast(), ctx);
    }

    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "(Kikimr DataSink) Unsupported function: "
        << callable.CallableName()));
    return TStatus::Error;
}

TIntrusivePtr<IDataProvider> CreateKikimrDataSink(
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    TTypeAnnotationContext& types,
    TIntrusivePtr<IKikimrGateway> gateway,
    TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    TIntrusivePtr<IKikimrQueryExecutor> queryExecutor)
{
    return new TKikimrDataSink(functionRegistry, types, gateway, sessionCtx, queryExecutor);
}

TAutoPtr<IGraphTransformer> CreateKiSinkIntentDeterminationTransformer(
    TIntrusivePtr<TKikimrSessionContext> sessionCtx)
{
    return new TKiSinkIntentDeterminationTransformer(sessionCtx);
}

} // namespace NYql
