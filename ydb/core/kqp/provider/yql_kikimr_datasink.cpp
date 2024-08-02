#include "yql_kikimr_provider_impl.h"

#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>

#include <ydb/library/yql/utils/log/log.h>

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

    TStatus HandleCreateReplication(TKiCreateReplication node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        Y_UNUSED(node);
        return TStatus::Ok;
    }

    TStatus HandleAlterReplication(TKiAlterReplication node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        Y_UNUSED(node);
        return TStatus::Ok;
    }

    TStatus HandleDropReplication(TKiDropReplication node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        Y_UNUSED(node);
        return TStatus::Ok;
    }

    TStatus HandleCreateSequence(NNodes::TKiCreateSequence node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        Y_UNUSED(node);
        return TStatus::Ok;
    }

    TStatus HandleDropSequence(NNodes::TKiDropSequence node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        Y_UNUSED(node);
        return TStatus::Ok;
    }

    TStatus HandleAlterSequence(NNodes::TKiAlterSequence node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        Y_UNUSED(node);
        return TStatus::Ok;
    }

    TStatus HandleModifyPermissions(TKiModifyPermissions node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "ModifyPermissions is not yet implemented for intent determination transformer"));
        return TStatus::Error;
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

    TStatus HandleUpsertObject(TKiUpsertObject node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "UpsertObject is not yet implemented for intent determination transformer"));
        return TStatus::Error;
    }

    TStatus HandleCreateObject(TKiCreateObject node, TExprContext& ctx) override {
        Y_UNUSED(node);
        Y_UNUSED(ctx);
        return TStatus::Ok;
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

    TStatus HandleRenameGroup(TKiRenameGroup node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "RenameGroup is not yet implemented for intent determination transformer"));
        return TStatus::Error;
    }

    TStatus HandleDropGroup(TKiDropGroup node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "DropGroup is not yet implemented for intent determination transformer"));
        return TStatus::Error;
    }

    TStatus HandlePgDropObject(TPgDropObject node, TExprContext& ctx) override {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                << "PgDropObject is not yet implemented for intent determination transformer"));
        return TStatus::Error;
    }

    static void HandleDropTable(TIntrusivePtr<TKikimrSessionContext>& ctx, const NCommon::TWriteTableSettings& settings,
        const TKikimrKey& key, const TStringBuf& cluster)
    {
        auto tableType = settings.TableType.IsValid()
            ? GetTableTypeFromString(settings.TableType.Cast())
            : ETableType::Table; // v0, pg support
        ctx->Tables().GetOrAddTable(TString(cluster), ctx->GetDatabase(), key.GetTablePath(), tableType).DisableAuthInfo();
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

                if (mode == "drop" || mode == "drop_if_exists") {
                    HandleDropTable(SessionCtx, settings, key, cluster);
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
                    if (!settings.PgFilter) {
                        if (!settings.Filter) {
                            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Filter option is required for table update."));
                            return TStatus::Error;
                        }
                        if (!settings.Update) {
                            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Update option is required for table update."));
                            return TStatus::Error;
                        }
                    }
                    SessionCtx->Tables().GetOrAddTable(TString(cluster), SessionCtx->GetDatabase(), key.GetTablePath());
                    return TStatus::Ok;
                } else if (mode == "delete") {
                    if (!settings.Filter && !settings.PgFilter) {
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

                if (mode == "create" || mode == "create_if_not_exists" || mode == "create_or_replace") {
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
                } else if (mode == "drop" || mode == "drop_if_exists") {
                    HandleDropTable(SessionCtx, settings, key, cluster);
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
            case TKikimrKey::Type::Permission:
                return TStatus::Ok;
            case TKikimrKey::Type::PGObject:
                return TStatus::Ok;
            case TKikimrKey::Type::Replication:
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

    TStatus HandleReturningList(TKiReturningList node, TExprContext& ctx) override {
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
        const NExternalSource::IExternalSourceFactory::TPtr& externalSourceFactory,
        TIntrusivePtr<IKikimrQueryExecutor> queryExecutor)
        : FunctionRegistry(functionRegistry)
        , Types(types)
        , Gateway(gateway)
        , SessionCtx(sessionCtx)
        , ExternalSourceFactory(externalSourceFactory)
        , IntentDeterminationTransformer(CreateKiSinkIntentDeterminationTransformer(sessionCtx))
        , TypeAnnotationTransformer(CreateKiSinkTypeAnnotationTransformer(gateway, sessionCtx, types))
        , LogicalOptProposalTransformer(CreateKiLogicalOptProposalTransformer(sessionCtx, types))
        , PhysicalOptProposalTransformer(CreateKiPhysicalOptProposalTransformer(sessionCtx))
        , CallableExecutionTransformer(CreateKiSinkCallableExecutionTransformer(gateway, sessionCtx, queryExecutor))
        , PlanInfoTransformer(CreateKiSinkPlanInfoTransformer(queryExecutor))
    {
        Y_UNUSED(FunctionRegistry);
        Y_UNUSED(Types);

        Y_DEBUG_ABORT_UNLESS(gateway);
        Y_DEBUG_ABORT_UNLESS(sessionCtx);
        Y_DEBUG_ABORT_UNLESS(queryExecutor);
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

        if (node.IsCallable(TKiCreateReplication::CallableName())
            || node.IsCallable(TKiAlterReplication::CallableName())
            || node.IsCallable(TKiDropReplication::CallableName())
        ) {
            return true;
        }

        if (node.IsCallable(TKiCreateSequence::CallableName())
            || node.IsCallable(TKiDropSequence::CallableName())
            || node.IsCallable(TKiAlterSequence::CallableName())) {
            return true;
        }

        if (node.IsCallable(TKiCreateUser::CallableName())
            || node.IsCallable(TKiAlterUser::CallableName())
            || node.IsCallable(TKiDropUser::CallableName())
            || node.IsCallable(TKiCreateGroup::CallableName())
            || node.IsCallable(TKiAlterGroup::CallableName())
            || node.IsCallable(TKiRenameGroup::CallableName())
            || node.IsCallable(TKiDropGroup::CallableName())
            || node.IsCallable(TKiUpsertObject::CallableName())
            || node.IsCallable(TKiCreateObject::CallableName())
            || node.IsCallable(TKiAlterObject::CallableName())
            || node.IsCallable(TKiDropObject::CallableName()))
        {
            return true;
        }

        if (node.IsCallable(TKiModifyPermissions::CallableName())) {
            return true;
        }

        if(node.IsCallable(TPgDropObject::CallableName())) {
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

    bool IsEvaluatedDefaultNode(TExprNode::TPtr input) {
        auto node = TExprBase(input);

        if (node.Maybe<TCoNothing>()) {
            return true;
        }

        if (input->IsCallable()
            && input->Content() == "PgCast"
            && input->ChildrenSize() >= 1
            && TExprBase(input->Child(0)).Maybe<TCoNull>())
        {
            return true;
        }

        if (node.Maybe<TCoAtom>()) {
            return true;
        }

        if (node.Maybe<TCoNull>()) {
            return true;
        }

        if (auto maybeJust = node.Maybe<TCoJust>()) {
            return IsEvaluatedDefaultNode(maybeJust.Cast().Input().Ptr());
        }

        if (node.Maybe<TCoPgConst>()) {
            return true;
        }

        if (node.Maybe<TCoDataCtor>()) {
            return true;
        }

        if (node.Maybe<TCoDataType>()) {
            return true;
        }

        if (node.Maybe<TCoPgType>()) {
            return true;
        }

        return false;
    }

    bool CanEvaluateDefaultValue(TExprNode::TPtr input) {
        if (IsEvaluatedDefaultNode(input)) {
            return true;
        }

        static const THashSet<TString> allowedNodes = {
            "Concat", "Just", "Optional", "SafeCast", "AsList",
            "+", "-", "*", "/", "%", "PgCast"};

        if (input->IsCallable("PgCast") && input->Child(0)->IsCallable("PgConst")) {
            return true;
        }

        if (input->IsCallable(allowedNodes)) {
            for (size_t i = 0; i < input->ChildrenSize(); i++) {
                auto callableInput = input->Child(i);
                if (!CanEvaluateDefaultValue(callableInput)) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    bool ShouldEvaluateDefaultValue(TExprNode::TPtr input) {
        if (IsEvaluatedDefaultNode(input)) {
            return false;
        }

        return CanEvaluateDefaultValue(input);
    }

    bool EvaluateDefaultValuesIfNeeded(TExprContext& ctx, TExprList columns) {
        bool exprEvalNeeded = false;
        for(auto item: columns) {
            auto columnTuple = item.Cast<TExprList>();
            if (columnTuple.Size() > 2) {
                const auto& columnConstraints = columnTuple.Item(2).Cast<TCoNameValueTuple>();
                for(const auto& constraint: columnConstraints.Value().Cast<TCoNameValueTupleList>()) {
                if (constraint.Name().Value() != "default")
                    continue;

                YQL_ENSURE(constraint.Value().IsValid());

                if (ShouldEvaluateDefaultValue(constraint.Value().Cast().Ptr())) {
                    auto evaluatedExpr = ctx.Builder(constraint.Value().Cast().Ptr()->Pos())
                        .Callable("EvaluateExpr")
                        .Add(0, constraint.Value().Cast().Ptr())
                        .Seal()
                        .Build();

                        constraint.Ptr()->ChildRef(TCoNameValueTuple::idx_Value) = evaluatedExpr;
                        exprEvalNeeded = true;
                    }
                }
            }
        }
        return exprEvalNeeded;
    }

    static TExprNode::TPtr MakeKiDropTable(const TExprNode::TPtr& node, const NCommon::TWriteTableSettings& settings,
        const TKikimrKey& key, TExprContext& ctx)
    {
        YQL_ENSURE(!settings.Columns);
        auto tableType = settings.TableType.IsValid()
            ? settings.TableType.Cast()
            : Build<TCoAtom>(ctx, node->Pos()).Value("table").Done(); // v0, pg support
        bool missingOk = (settings.Mode.Cast().Value() == "drop_if_exists");

        return Build<TKiDropTable>(ctx, node->Pos())
            .World(node->Child(0))
            .DataSink(node->Child(1))
            .Table().Build(key.GetTablePath())
            .Settings(settings.Other)
            .TableType(tableType)
            .MissingOk<TCoAtom>()
                .Value(missingOk)
            .Build()
            .Done()
            .Ptr();
    }

    static TExprNode::TPtr MakePgDropObject(const TExprNode::TPtr& node, const NCommon::TPgObjectSettings& settings,
                                           const TKikimrKey& key, TExprContext& ctx)
    {
        bool missingOk = (settings.IfExists.Cast().Value() == "true");

        return Build<TPgDropObject>(ctx, node->Pos())
                .World(node->Child(0))
                .DataSink(node->Child(1))
                .ObjectId().Build(key.GetPGObjectId())
                .TypeId().Build(key.GetPGObjectType())
                .MissingOk<TCoAtom>()
                    .Value(missingOk)
                .Build()
                .Done()
                .Ptr();
    }

    static TExprNode::TPtr MakeCreateSequence(const TExprNode::TPtr& node,
            const NCommon::TWriteSequenceSettings& settings, const TKikimrKey& key, TExprContext& ctx)
    {
        YQL_ENSURE(settings.Mode);
        if (node->Child(3)->Content() != "Void") {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Creating sequence with data is not supported."));
            return nullptr;
        }

        auto valueType = settings.ValueType.IsValid()
            ? settings.ValueType.Cast()
            : Build<TCoAtom>(ctx, node->Pos()).Value("int8").Done();

        auto temporary = settings.Temporary.IsValid()
            ? settings.Temporary.Cast()
            : Build<TCoAtom>(ctx, node->Pos()).Value("false").Done();

        auto existringOk = (settings.Mode.Cast().Value() == "create_if_not_exists");

        return Build<TKiCreateSequence>(ctx, node->Pos())
            .World(node->Child(0))
            .DataSink(node->Child(1))
            .Sequence().Build(key.GetPGObjectId())
            .ValueType(valueType)
            .Temporary(temporary)
            .ExistingOk<TCoAtom>()
                .Value(existringOk)
                .Build()
            .SequenceSettings(settings.SequenceSettings.Cast())
            .Settings(settings.Other)
            .Done()
            .Ptr();
    }

    static TExprNode::TPtr MakeDropSequence(const TExprNode::TPtr& node,
            const NCommon::TWriteSequenceSettings& settings, const TKikimrKey& key, TExprContext& ctx)
    {
        bool missingOk = (settings.Mode.Cast().Value() == "drop_if_exists");

        return Build<TKiDropSequence>(ctx, node->Pos())
            .World(node->Child(0))
            .DataSink(node->Child(1))
            .Sequence().Build(key.GetPGObjectId())
            .Settings(settings.Other)
            .MissingOk<TCoAtom>()
                .Value(missingOk)
            .Build()
            .Done()
            .Ptr();
    }

    static TExprNode::TPtr MakeAlterSequence(const TExprNode::TPtr& node,
            const NCommon::TWriteSequenceSettings& settings, const TKikimrKey& key, TExprContext& ctx)
    {
        YQL_ENSURE(settings.Mode);
        bool missingOk = (settings.Mode.Cast().Value() == "alter_if_exists");

        if (node->Child(3)->Content() != "Void") {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Alter sequence with data is not supported."));
            return nullptr;
        }

        auto valueType = settings.ValueType.IsValid()
            ? settings.ValueType.Cast()
            : Build<TCoAtom>(ctx, node->Pos()).Value("Null").Done();

        return Build<TKiAlterSequence>(ctx, node->Pos())
            .World(node->Child(0))
            .DataSink(node->Child(1))
            .Sequence().Build(key.GetPGObjectId())
            .ValueType(valueType)
            .SequenceSettings(settings.SequenceSettings.Cast())
            .Settings(settings.Other)
            .MissingOk<TCoAtom>()
                .Value(missingOk)
            .Build()
            .Done()
            .Ptr();
    }

    bool RewriteIOExternal(const TKikimrKey& key, const TExprNode::TPtr& node, const TCoAtom& mode, TExprContext& ctx, TExprNode::TPtr& resultNode) {
        TKiDataSink dataSink(node->ChildPtr(1));
        auto& tableDesc = SessionCtx->Tables().GetTable(TString{dataSink.Cluster()}, key.GetTablePath());
        if (!tableDesc.Metadata || tableDesc.Metadata->Kind != EKikimrTableKind::External) {
            return true;
        }

        if (tableDesc.Metadata->ExternalSource.SourceType != ESourceType::ExternalDataSource && tableDesc.Metadata->ExternalSource.SourceType != ESourceType::ExternalTable) {
            YQL_CVLOG(NLog::ELevel::ERROR, NLog::EComponent::ProviderKikimr) << "Skip RewriteIO for external entity: unknown entity type: " << (int)tableDesc.Metadata->ExternalSource.SourceType;
            return true;
        }

        if (mode != "insert_abort") {
            if (mode == "drop" || mode == "drop_if_exists") {
                TString dropHint;
                if (tableDesc.Metadata->ExternalSource.SourceType == ESourceType::ExternalDataSource) {
                    dropHint = "DROP EXTERNAL DATA SOURCE";
                } else if (tableDesc.Metadata->ExternalSource.SourceType == ESourceType::ExternalTable) {
                    dropHint = "DROP EXTERNAL TABLE";
                }
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Cannot drop external entity by using DROP TABLE" << (dropHint ?  ". Please use " : "") << dropHint));
            } else {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Write mode '" << static_cast<TStringBuf>(mode) << "' is not supported for external entities"));
            }
            return false;
        }

        if (tableDesc.Metadata->ExternalSource.SourceType == ESourceType::ExternalDataSource && tableDesc.Metadata->TableType == NYql::ETableType::Unknown) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Attempt to write to external data source \"" << key.GetTablePath() << "\" without table. Please specify table to write to"));
            return false;
        }

        ctx.Step.Repeat(TExprStep::DiscoveryIO)
                .Repeat(TExprStep::Epochs)
                .Repeat(TExprStep::Intents)
                .Repeat(TExprStep::LoadTablesMetadata)
                .Repeat(TExprStep::RewriteIO);

        const auto& externalSource = ExternalSourceFactory->GetOrCreate(tableDesc.Metadata->ExternalSource.Type);
        if (tableDesc.Metadata->ExternalSource.SourceType == ESourceType::ExternalDataSource) {
            auto writeArgs = node->ChildrenList();
            writeArgs[1] = Build<TCoDataSink>(ctx, node->Pos())
                            .Category(ctx.NewAtom(node->Pos(), externalSource->GetName()))
                            .FreeArgs()
                                .Add(writeArgs[1]->ChildrenList()[1])
                            .Build()
                            .Done().Ptr();
            resultNode = ctx.ChangeChildren(*node, std::move(writeArgs));
            return true;
        }

        // tableDesc.Metadata->ExternalSource.SourceType == ESourceType::ExternalTable
        TExprNode::TPtr path = ctx.NewCallable(node->Pos(), "String", { ctx.NewAtom(node->Pos(), tableDesc.Metadata->ExternalSource.TableLocation) });
        auto table = ctx.NewList(node->Pos(), {ctx.NewAtom(node->Pos(), "table"), path});
        auto keyNode = ctx.NewCallable(node->Pos(), "Key", {table});
        resultNode = Build<TCoWrite>(ctx, node->Pos())
            .World(node->Child(0))
            .DataSink()
                .Category(ctx.NewAtom(node->Pos(), externalSource->GetName()))
                .FreeArgs()
                    .Add(ctx.NewAtom(node->Pos(), tableDesc.Metadata->ExternalSource.DataSourcePath))
                    .Build()
                .Build()
            .FreeArgs()
                .Add(keyNode)
                .Add(node->Child(3))
                .Add(BuildExternalTableSettings(node->Pos(), ctx, tableDesc.Metadata->Columns, externalSource, tableDesc.Metadata->ExternalSource.TableContent))
            .Build()
            .Done().Ptr();
        return true;
    }

    bool CheckIOSinkTable(const TKikimrKey& key, const TExprNode::TPtr& node, const TCoAtom& mode, TExprContext& ctx) {
        TKiDataSink dataSink(node->ChildPtr(1));
        auto& tableDesc = SessionCtx->Tables().GetTable(TString{dataSink.Cluster()}, key.GetTablePath());
        if (!tableDesc.Metadata) {
            return false;
        }

        if (tableDesc.Metadata->Kind == EKikimrTableKind::Olap && mode != "replace" && mode != "drop" && mode != "drop_if_exists" && mode != "insert_abort" && mode != "update" && mode != "upsert" && mode != "delete" && mode != "update_on" && mode != "delete_on") {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Write mode '" << static_cast<TStringBuf>(mode) << "' is not supported for olap tables."));
            return true;
        }

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

                TExprNode::TPtr resultNode;
                if (!RewriteIOExternal(key, node, mode, ctx, resultNode)) {
                    return nullptr;
                }

                if (resultNode) {
                    return resultNode;
                }

                if (CheckIOSinkTable(key, node, mode, ctx)) {
                    return nullptr;
                }

                if (!settings.ReturningList.IsValid()) {
                    settings.ReturningList = Build<TExprList>(ctx, node->Pos()).Done();
                }

                auto returningColumns = Build<TCoAtomList>(ctx, node->Pos()).Done();

                auto fillStar = [&]() {
                    bool sysColumnsEnabled = SessionCtx->Config().SystemColumnsEnabled();

                    auto dataSinkNode = node->Child(1);
                    TKiDataSink dataSink(dataSinkNode);
                    auto table = SessionCtx->Tables().EnsureTableExists(
                        TString(dataSink.Cluster()),
                        key.GetTablePath(), node->Pos(), ctx);

                    returningColumns = BuildColumnsList(*table, node->Pos(), ctx, sysColumnsEnabled, true /*ignoreWriteOnlyColumns*/);
                };

                TVector<TExprBase> columnsToReturn;
                for (const auto item : settings.ReturningList.Cast()) {
                    if (item.Maybe<TCoPgResultItem>()) {
                        auto pgResultNode = item.Cast<TCoPgResultItem>();
                        const auto value = pgResultNode.ExpandedColumns().Cast<TCoAtom>().Value();
                        if (value.empty()) {
                            fillStar();
                            break;
                        } else {
                            auto atom = Build<TCoAtom>(ctx, node->Pos())
                                .Value(value)
                                .Done();
                            columnsToReturn.emplace_back(std::move(atom));
                        }
                    } else if (auto returningItem = item.Maybe<TCoReturningListItem>()) {
                        columnsToReturn.push_back(returningItem.Cast().ColumnRef());
                    } else if (auto returningStar = item.Maybe<TCoReturningStar>()) {
                        fillStar();
                        break;
                    }
                }

                if (!columnsToReturn.empty()) {
                    returningColumns = Build<TCoAtomList>(ctx, node->Pos())
                        .Add(columnsToReturn)
                        .Done();
                }

                if (mode == "drop" || mode == "drop_if_exists") {
                    return MakeKiDropTable(node, settings, key, ctx);
                } else if (mode == "update") {
                    if (settings.Filter) {
                        YQL_ENSURE(settings.Update);
                        return Build<TKiUpdateTable>(ctx, node->Pos())
                            .World(node->Child(0))
                            .DataSink(node->Child(1))
                            .Table().Build(key.GetTablePath())
                            .Filter(settings.Filter.Cast())
                            .Update(settings.Update.Cast())
                            .ReturningColumns(returningColumns)
                            .Done()
                            .Ptr();
                    } else {
                        YQL_ENSURE(settings.PgFilter);
                        return Build<TKiWriteTable>(ctx, node->Pos())
                            .World(node->Child(0))
                            .DataSink(node->Child(1))
                            .Table().Build(key.GetTablePath())
                            .Input(settings.PgFilter.Cast())
                            .Mode()
                                .Value("update_on")
                            .Build()
                            .Settings(settings.Other)
                            .ReturningColumns(returningColumns)
                            .Done()
                            .Ptr();
                    }
                } else if (mode == "delete") {
                    YQL_ENSURE(settings.Filter || settings.PgFilter);
                    if (settings.Filter) {
                        return Build<TKiDeleteTable>(ctx, node->Pos())
                            .World(node->Child(0))
                            .DataSink(node->Child(1))
                            .Table().Build(key.GetTablePath())
                            .Filter(settings.Filter.Cast())
                            .ReturningColumns(returningColumns)
                            .Done()
                            .Ptr();
                    } else {
                        return Build<TKiWriteTable>(ctx, node->Pos())
                            .World(node->Child(0))
                            .DataSink(node->Child(1))
                            .Table().Build(key.GetTablePath())
                            .Input(settings.PgFilter.Cast())
                            .Mode()
                                .Value("delete_on")
                            .Build()
                            .Settings(settings.Other)
                            .ReturningColumns(returningColumns)
                            .Done()
                            .Ptr();
                    }
                } else {
                    return Build<TKiWriteTable>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .Table().Build(key.GetTablePath())
                        .Input(node->Child(3))
                        .Mode(mode)
                        .Settings(settings.Other)
                        .ReturningColumns(returningColumns)
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
                if (mode == "create" || mode == "create_if_not_exists" || mode == "create_or_replace") {
                    if (node->Child(3)->Content() != "Void") {
                        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Creating table with data is not supported."));
                        return nullptr;
                    }
                    YQL_ENSURE(settings.Columns);
                    if (settings.Columns.Cast().Empty()) {
                        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Creating table without columns is not supported."));
                        return nullptr;
                    }
                    for (const auto& column : settings.Columns.Cast()) {
                        if (column.Ptr()->ChildrenSize() < 2) {
                            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Creating table with columns without type is not supported."));
                            return nullptr;
                        }
                    }

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

                    auto temporary = settings.Temporary.IsValid()
                        ? settings.Temporary.Cast()
                        : Build<TCoAtom>(ctx, node->Pos()).Value("false").Done();

                    auto replaceIfExists = (settings.Mode.Cast().Value() == "create_or_replace");
                    auto existringOk = (settings.Mode.Cast().Value() == "create_if_not_exists");

                    auto createTable = Build<TKiCreateTable>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .Table().Build(key.GetTablePath())
                        .Temporary(temporary)
                        .Columns(settings.Columns.Cast())
                        .PrimaryKey(settings.PrimaryKey.Cast())
                        .Settings(settings.Other)
                        .Indexes(settings.Indexes.Cast())
                        .Changefeeds(settings.Changefeeds.Cast())
                        .PartitionBy(settings.PartitionBy.Cast())
                        .ColumnFamilies(settings.ColumnFamilies.Cast())
                        .TableSettings(settings.TableSettings.Cast())
                        .TableType(tableType)
                        .ReplaceIfExists<TCoAtom>()
                            .Value(replaceIfExists)
                            .Build()
                        .ExistingOk<TCoAtom>()
                            .Value(existringOk)
                            .Build()
                        .Done();

                    bool exprEvalNeeded = EvaluateDefaultValuesIfNeeded(ctx, createTable.Cast<TKiCreateTable>().Columns());
                    if (exprEvalNeeded) {
                        ctx.Step.Repeat(TExprStep::ExprEval);
                    }

                    return createTable.Ptr();

                } else if (mode == "alter") {
                    for (auto setting : settings.Other) {
                        if (setting.Name().Value() == "intent") {
                            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Old AST format for AlterTable"));
                            return nullptr;
                        }
                    }

                    YQL_ENSURE(settings.AlterActions);
                    YQL_ENSURE(!settings.AlterActions.Cast().Empty());
                    auto alterTable = Build<TKiAlterTable>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .Table().Build(key.GetTablePath())
                        .Actions(settings.AlterActions.Cast())
                        .TableType(tableType)
                        .Done();

                    bool exprEvalNeeded = false;
                    for (const auto& action : alterTable.Actions()) {
                        auto name = action.Name().Value();
                        if (name == "addColumns") {
                            exprEvalNeeded |= EvaluateDefaultValuesIfNeeded(ctx, action.Value().Cast<TExprList>());
                        }
                    }

                    if (exprEvalNeeded) {
                        ctx.Step.Repeat(TExprStep::ExprEval);
                    }

                    return alterTable.Ptr();

                } else if (mode == "drop" || mode == "drop_if_exists") {
                    return MakeKiDropTable(node, settings, key, ctx);
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

                if (mode == "upsertObject") {
                    return Build<TKiUpsertObject>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .ObjectId().Build(key.GetObjectId())
                        .TypeId().Build(key.GetObjectType())
                        .Features(settings.Features)
                        .Done()
                        .Ptr();
                } else if (mode == "createObject" || mode == "createObjectIfNotExists" || mode == "createObjectOrReplace") {
                    return Build<TKiCreateObject>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .ObjectId().Build(key.GetObjectId())
                        .TypeId().Build(key.GetObjectType())
                        .Features(settings.Features)
                        .ReplaceIfExists<TCoAtom>()
                            .Value(mode == "createObjectOrReplace")
                            .Build()
                        .ExistingOk<TCoAtom>()
                            .Value(mode == "createObjectIfNotExists")
                            .Build()
                        .Done()
                        .Ptr();
                } else if (mode == "alterObject") {
                    return Build<TKiAlterObject>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .ObjectId().Build(key.GetObjectId())
                        .TypeId().Build(key.GetObjectType())
                        .Features(settings.Features)
                        .ResetFeatures(settings.ResetFeatures)
                        .Done()
                        .Ptr();
                } else if (mode == "dropObject" || mode == "dropObjectIfExists") {
                    return Build<TKiDropObject>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .ObjectId().Build(key.GetObjectId())
                        .TypeId().Build(key.GetObjectType())
                        .Features(settings.Features)
                        .MissingOk<TCoAtom>()
                            .Value(mode == "dropObjectIfExists")
                        .Build()
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
                } else if (mode == "dropUser" || mode == "dropUserIfExists") {
                    return Build<TKiDropUser>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .UserName().Build(key.GetRoleName())
                        .Settings(settings.Other)
                        .MissingOk<TCoAtom>()
                            .Value(mode == "dropUserIfExists")
                        .Build()
                        .Done()
                        .Ptr();
                } else if (mode == "createGroup") {
                    return Build<TKiCreateGroup>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .GroupName().Build(key.GetRoleName())
                        .Roles(settings.Roles.Cast())
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
                } else if (mode == "renameGroup") {
                    return Build<TKiRenameGroup>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .GroupName().Build(key.GetRoleName())
                        .NewName(settings.NewName.Cast())
                        .Done()
                        .Ptr();
                } else if (mode == "dropGroup" || mode == "dropGroupIfExists") {
                    return Build<TKiDropGroup>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .GroupName().Build(key.GetRoleName())
                        .Settings(settings.Other)
                        .MissingOk<TCoAtom>()
                            .Value(mode == "dropGroupIfExists")
                        .Build()
                        .Done()
                        .Ptr();
                } else {
                    YQL_ENSURE(false, "unknown Role mode \"" << TString(mode) << "\"");
                }
                break;
            }

            case TKikimrKey::Type::Permission: {
                NCommon::TWritePermissionSettings settings = NCommon::ParseWritePermissionsSettings(TExprList(node->Child(4)), ctx);
                const auto& mode = key.GetPermissionAction();

                if (mode == "grant" || mode == "revoke") {
                    return Build<TKiModifyPermissions>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .Action().Build(mode)
                        .Permissions(settings.Permissions.Cast())
                        .Paths(settings.Paths.Cast())
                        .Roles(settings.RoleNames.Cast())
                        .Done()
                        .Ptr();
                } else {
                    YQL_ENSURE(false, "unknown Permission action \"" << TString(mode) << "\"");
                }
                break;
            }

            case TKikimrKey::Type::PGObject: {
                NCommon::TPgObjectSettings settings = NCommon::ParsePgObjectSettings(TExprList(node->Child(4)), ctx);

                YQL_ENSURE(settings.Mode);
                auto mode = settings.Mode.Cast();

                if (mode == "dropIndex") {
                    return MakePgDropObject(node, settings, key, ctx);
                } else if (key.GetPGObjectType() == "pgSequence") {
                    NCommon::TWriteSequenceSettings settings =
                        NCommon::ParseSequenceSettings(TExprList(node->Child(4)), ctx);
                    if (mode == "create" || mode == "create_if_not_exists") {
                        return MakeCreateSequence(node, settings, key, ctx);
                    } else if (mode == "drop" || mode == "drop_if_exists") {
                        return MakeDropSequence(node, settings, key, ctx);
                    } else if (mode == "alter" || mode == "alter_if_exists") {
                        return MakeAlterSequence(node, settings, key, ctx);
                    } else {
                        YQL_ENSURE(false, "unknown Sequence mode \"" << TString(mode) << "\"");
                    }
                } else {
                    YQL_ENSURE(false, "unknown PGObject with type: \"" << key.GetPGObjectType() << "\"");
                }
                break;
            }

            case TKikimrKey::Type::Replication: {
                auto settings = NCommon::ParseWriteReplicationSettings(TExprList(node->Child(4)), ctx);
                YQL_ENSURE(settings.Mode);
                auto mode = settings.Mode.Cast();

                if (mode == "create") {
                    return Build<TKiCreateReplication>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .Replication().Build(key.GetReplicationPath())
                        .Targets(settings.Targets.Cast())
                        .ReplicationSettings(settings.ReplicationSettings.Cast())
                        .Settings(settings.Other)
                        .Done()
                        .Ptr();
                } else if (mode == "alter") {
                    return Build<TKiAlterReplication>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .Replication().Build(key.GetReplicationPath())
                        .ReplicationSettings(settings.ReplicationSettings.Cast())
                        .Settings(settings.Other)
                        .Done()
                        .Ptr();
                } else if (mode == "drop" || mode == "dropCascade") {
                    return Build<TKiDropReplication>(ctx, node->Pos())
                        .World(node->Child(0))
                        .DataSink(node->Child(1))
                        .Replication().Build(key.GetReplicationPath())
                        .Cascade<TCoAtom>()
                            .Value(mode == "dropCascade")
                            .Build()
                        .Done()
                        .Ptr();
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Unknown operation type for replication"));
                    return nullptr;
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

    TString GetProviderPath(const TExprNode&) override {
        return TString(KikimrProviderName);
    }

private:
    const NKikimr::NMiniKQL::IFunctionRegistry& FunctionRegistry;
    const TTypeAnnotationContext& Types;
    TIntrusivePtr<IKikimrGateway> Gateway;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    NExternalSource::IExternalSourceFactory::TPtr ExternalSourceFactory;

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

    if (auto node = TMaybeNode<TKiCreateReplication>(input)) {
        return HandleCreateReplication(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiAlterReplication>(input)) {
        return HandleAlterReplication(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiDropReplication>(input)) {
        return HandleDropReplication(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiUpsertObject>(input)) {
        return HandleUpsertObject(node.Cast(), ctx);
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

    if (auto node = TMaybeNode<TKiModifyPermissions>(input)) {
        return HandleModifyPermissions(node.Cast(), ctx);
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

    if (auto node = TMaybeNode<TKiRenameGroup>(input)) {
        return HandleRenameGroup(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TKiDropGroup>(input)) {
        return HandleDropGroup(node.Cast(), ctx);
    }

    if (auto node = TMaybeNode<TPgDropObject>(input)) {
        return HandlePgDropObject(node.Cast(), ctx);
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

    if (auto node = callable.Maybe<TKiReturningList>()) {
        return HandleReturningList(node.Cast(), ctx);
    }

    if (auto node = callable.Maybe<TKiCreateSequence>()) {
        return HandleCreateSequence(node.Cast(), ctx);
    }

    if (auto node = callable.Maybe<TKiDropSequence>()) {
        return HandleDropSequence(node.Cast(), ctx);
    }

    if (auto node = callable.Maybe<TKiAlterSequence>()) {
        return HandleAlterSequence(node.Cast(), ctx);
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
    const NExternalSource::IExternalSourceFactory::TPtr& externalSourceFactory,
    TIntrusivePtr<IKikimrQueryExecutor> queryExecutor)
{
    return new TKikimrDataSink(functionRegistry, types, gateway, sessionCtx, externalSourceFactory, queryExecutor);
}

TAutoPtr<IGraphTransformer> CreateKiSinkIntentDeterminationTransformer(
    TIntrusivePtr<TKikimrSessionContext> sessionCtx)
{
    return new TKiSinkIntentDeterminationTransformer(sessionCtx);
}

} // namespace NYql
