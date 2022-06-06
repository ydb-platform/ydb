#include "yql_kikimr_provider_impl.h"

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/yql_execution.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>

namespace NYql {
namespace {

using namespace NNodes;
using namespace NCommon;
using namespace NThreading;

namespace {
    bool EnsureNotPrepare(const TString featureName, TPositionHandle pos, const TKikimrQueryContext& queryCtx,
        TExprContext& ctx)
    {
        if (queryCtx.PrepareOnly && !queryCtx.SuppressDdlChecks) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder()
                << "'" << featureName << "' not supported in query prepare mode."));
            return false;
        }

        return true;
    }

    void FillExecDataQueryAst(TKiExecDataQuery exec, const TString& ast, TExprContext& ctx) {
        auto astNode = Build<TCoAtom>(ctx, exec.Pos())
            .Value(ast)
            .Done();

        astNode.Ptr()->SetTypeAnn(ctx.MakeType<TUnitExprType>());

        exec.Ptr()->ChildRef(TKiExecDataQuery::idx_Ast) = astNode.Ptr();
    }

    TCreateUserSettings ParseCreateUserSettings(TKiCreateUser createUser) {
        TCreateUserSettings createUserSettings;
        createUserSettings.UserName = TString(createUser.UserName());

        for (auto setting : createUser.Settings()) {
            auto name = setting.Name().Value();
            if (name == "password") {
                createUserSettings.Password = setting.Value().Cast<TCoAtom>().StringValue();
            } else if (name == "nullPassword") {
                // Default value
            } else if (name == "passwordEncrypted") {
                createUserSettings.PasswordEncrypted = true;
            }
        }
        return createUserSettings;
    }

    TAlterUserSettings ParseAlterUserSettings(TKiAlterUser alterUser) {
        TAlterUserSettings alterUserSettings;
        alterUserSettings.UserName = TString(alterUser.UserName());

        for (auto setting : alterUser.Settings()) {
            auto name = setting.Name().Value();
            if (name == "password") {
                alterUserSettings.Password = setting.Value().Cast<TCoAtom>().StringValue();
            } else if (name == "nullPassword") {
                // Default value
            } else if (name == "passwordEncrypted") {
                alterUserSettings.PasswordEncrypted = true;
            }
        }
        return alterUserSettings;
    }

    TDropUserSettings ParseDropUserSettings(TKiDropUser dropUser) {
        TDropUserSettings dropUserSettings;
        dropUserSettings.UserName = TString(dropUser.UserName());

        for (auto setting : dropUser.Settings()) {
            auto name = setting.Name().Value();
            if (name == "force") {
                dropUserSettings.Force = true;
            }
        }
        return dropUserSettings;
    }

    TCreateGroupSettings ParseCreateGroupSettings(TKiCreateGroup createGroup) {
        TCreateGroupSettings createGroupSettings;
        createGroupSettings.GroupName = TString(createGroup.GroupName());
        return createGroupSettings;
    }

    TAlterGroupSettings ParseAlterGroupSettings(TKiAlterGroup alterGroup) {
        TAlterGroupSettings alterGroupSettings;
        alterGroupSettings.GroupName = TString(alterGroup.GroupName());

        TString action = TString(alterGroup.Action());
        if (action == "addUsersToGroup") {
            alterGroupSettings.Action = TAlterGroupSettings::EAction::AddRoles;
        } else if (action == "dropUsersFromGroup") {
            alterGroupSettings.Action = TAlterGroupSettings::EAction::RemoveRoles;
        }

        for (auto role : alterGroup.Roles()) {
            alterGroupSettings.Roles.push_back(role.Cast<TCoAtom>().StringValue());
        }
        return alterGroupSettings;
    }

    TDropGroupSettings ParseDropGroupSettings(TKiDropGroup dropGroup) {
        TDropGroupSettings dropGroupSettings;
        dropGroupSettings.GroupName = TString(dropGroup.GroupName());

        for (auto setting : dropGroup.Settings()) {
            auto name = setting.Name().Value();
            if (name == "force") {
                dropGroupSettings.Force = true;
            }
        }
        return dropGroupSettings;
    }
}

class TKiSinkPlanInfoTransformer : public TGraphTransformerBase {
public:
    TKiSinkPlanInfoTransformer(TIntrusivePtr<IKikimrQueryExecutor> queryExecutor)
        : QueryExecutor(queryExecutor) {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ) final {
        output = input;
        VisitExpr(input, [](const TExprNode::TPtr& node) {
            if (auto maybeExec = TMaybeNode<TKiExecDataQuery>(node)) {
                auto exec = maybeExec.Cast();
                if (exec.Ast().Maybe<TCoVoid>()) {
                    YQL_ENSURE(false);
                }
            }

            return true;
        });

        return TStatus::Ok;
    }

    TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        Y_UNUSED(input);
        return MakeFuture();
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) final {
        output = input;
        return TStatus::Ok;
    }

private:
    struct TExecInfo {
        TKiExecDataQuery Node;
        TIntrusivePtr<IKikimrQueryExecutor::TAsyncQueryResult> Result;
    };

private:
    TIntrusivePtr<IKikimrQueryExecutor> QueryExecutor;
};

class TKiSourceCallableExecutionTransformer : public TAsyncCallbackTransformer<TKiSourceCallableExecutionTransformer> {
public:
    TKiSourceCallableExecutionTransformer(TIntrusivePtr<IKikimrGateway> gateway,
        TIntrusivePtr<TKikimrSessionContext> sessionCtx)
        : Gateway(gateway)
        , SessionCtx(sessionCtx) {}

    std::pair<TStatus, TAsyncTransformCallbackFuture> CallbackTransform(const TExprNode::TPtr& input,
        TExprNode::TPtr& output, TExprContext& ctx)
    {
        YQL_ENSURE(input->Type() == TExprNode::Callable);
        output = input;

        if (input->Content() == "Pull") {
            auto pullInput = input->Child(0);

            if (auto maybeNth = TMaybeNode<TCoNth>(pullInput)) {
                ui32 index = FromString<ui32>(maybeNth.Cast().Index().Value());

                if (auto maybeExecQuery = maybeNth.Tuple().Maybe<TCoRight>().Input().Maybe<TKiExecDataQuery>()) {
                    return RunResOrPullForExec(TResOrPullBase(input), maybeExecQuery.Cast(), ctx, index);
                }
            }

            IDataProvider::TFillSettings fillSettings = NCommon::GetFillSettings(*input);

            if (auto maybeTableList = TMaybeNode<TCoRight>(pullInput).Input().Maybe<TKiReadTableList>()) {
                if (!EnsureNotPrepare("tablelist", pullInput->Pos(), SessionCtx->Query(), ctx)) {
                    return SyncError();
                }

                TKikimrKey key(ctx);
                YQL_ENSURE(key.Extract(maybeTableList.Cast().TableKey().Ref()));
                auto future = Gateway->ListPath(TString(maybeTableList.Cast().DataSource().Cluster()),
                    key.GetFolderPath());

                return WrapFuture(future, [fillSettings](const IKikimrGateway::TListPathResult& res,
                    const TExprNode::TPtr& input, TExprContext& ctx)
                {
                    auto result = GetTableListResult(res, fillSettings, ctx);
                    YQL_ENSURE(result);

                    return ctx.NewAtom(input->Pos(), *result);
                });
            }

            if (auto maybeTableScheme = TMaybeNode<TCoRight>(pullInput).Input().Maybe<TKiReadTableScheme>()) {
                if (!EnsureNotPrepare("tablescheme", pullInput->Pos(), SessionCtx->Query(), ctx)) {
                    return SyncError();
                }

                TKikimrKey key(ctx);
                auto cluster = maybeTableScheme.Cast().DataSource().Cluster();
                YQL_ENSURE(key.Extract(maybeTableScheme.Cast().TableKey().Ref()));

                auto& tableDesc = SessionCtx->Tables().ExistingTable(TString(cluster), key.GetTablePath());
                TKikimrTableDescription rawTableDesc;
                rawTableDesc.Metadata = tableDesc.Metadata;
                rawTableDesc.Load(ctx);

                auto result = GetTableMetadataResult(rawTableDesc, fillSettings, ctx);
                YQL_ENSURE(result);

                auto resultNode = ctx.NewAtom(input->Pos(), *result);

                return std::make_pair(
                    IGraphTransformer::TStatus::Async,
                    MakeFuture(TAsyncTransformCallback([resultNode](const TExprNode::TPtr& input,
                        TExprNode::TPtr& output, TExprContext& ctx)
                    {
                        Y_UNUSED(output);
                        Y_UNUSED(ctx);

                        input->SetState(TExprNode::EState::ExecutionComplete);
                        input->SetResult(TExprNode::TPtr(resultNode));
                        return IGraphTransformer::TStatus::Ok;
                    })));
            }
        }

        if (input->Content() == "Result") {
            auto resultInput = TExprBase(input->ChildPtr(0));
            auto exec = resultInput.Maybe<TCoNth>().Tuple().Maybe<TCoRight>().Input();
            YQL_ENSURE(exec.Maybe<TKiExecDataQuery>());

            ui32 index = FromString<ui32>(resultInput.Cast<TCoNth>().Index().Value());
            YQL_ENSURE(index == 0);

            return RunResOrPullForExec(TResOrPullBase(input), exec.Cast(), ctx, 0);
        }

        if (input->Content() == ConfigureName) {
            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            auto configure = TCoConfigure(input);
            auto clusterName = TString(configure.DataSource().Arg(1).Cast<TCoAtom>().Value());
            if (configure.Arg(2).Cast<TCoAtom>().Value() == TStringBuf("Attr")) {
                auto name = TString(configure.Arg(3).Cast<TCoAtom>().Value());
                TMaybe<TString> value;
                if (configure.Args().Count() == 5) {
                    value = TString(configure.Arg(4).Cast<TCoAtom>().Value());
                }

                SessionCtx->Config().Dispatch(clusterName, name, value, NCommon::TSettingDispatcher::EStage::RUNTIME);
            }

            input->SetState(TExprNode::EState::ExecutionComplete);
            input->SetResult(ctx.NewWorld(input->Pos()));
            return SyncOk();
        }

        if (input->Content() == "ClustersList") {
            if (!EnsureNotPrepare("ClustersList", input->Pos(), SessionCtx->Query(), ctx)) {
                return SyncError();
            }

            auto clusters = Gateway->GetClusters();
            Sort(clusters);

            auto ysonFormat = (NYson::EYsonFormat)FromString<ui32>(input->Child(0)->Content());
            TStringStream out;
            NYson::TYsonWriter writer(&out, ysonFormat);
            writer.OnBeginList();

            for (auto& cluster : clusters) {
                writer.OnListItem();
                writer.OnStringScalar(cluster);
            }

            writer.OnEndList();

            input->SetState(TExprNode::EState::ExecutionComplete);
            input->SetResult(ctx.NewAtom(input->Pos(), out.Str()));
            return SyncOk();
        }

        if (input->Content() == "KiReadTableList!" || input->Content() == "KiReadTableScheme!") {
            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            input->SetState(TExprNode::EState::ExecutionComplete);
            input->SetResult(ctx.NewWorld(input->Pos()));
            return SyncOk();
        }

        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
            << "(Kikimr DataSource) Failed to execute node: " << input->Content()));
        return SyncError();
    }

private:
    static TExprNode::TPtr GetResOrPullResult(const TExprNode& node, const IDataProvider::TFillSettings& fillSettings,
        const TTypeAnnotationNode* resultType, const NKikimrMiniKQL::TResult& resultValue, TExprContext& ctx)
    {
        TVector<TString> columnHints(NCommon::GetResOrPullColumnHints(node));

        auto protoValue = &resultValue;
        YQL_ENSURE(resultValue.GetArena());
        if (IsRawKikimrResult(resultValue)) {
            protoValue = KikimrResultToProto(resultValue, columnHints, fillSettings, resultValue.GetArena());
        }

        // TODO: Fix Void? and Null type difference
        // YQL_ENSURE(CheckKqpResultType(*protoValue, *resultType, ctx));
        Y_UNUSED(resultType);

        TExprNode::TPtr resultNode;
        if (fillSettings.Format == IDataProvider::EResultFormat::Yson) {
            NYson::EYsonFormat ysonFormat = NCommon::GetYsonFormat(fillSettings);

            auto yson = KqpResultToYson(*protoValue, ysonFormat, ctx);
            if (!yson) {
                return nullptr;
            }

            resultNode = ctx.NewAtom(node.Pos(), *yson);
        } else {
            YQL_ENSURE(fillSettings.Format == IDataProvider::EResultFormat::Custom);
            YQL_ENSURE(fillSettings.FormatDetails == KikimrMkqlProtoFormat);

            TVector<char> buffer(protoValue->ByteSize());
            Y_PROTOBUF_SUPPRESS_NODISCARD protoValue->SerializeToArray(buffer.data(), buffer.size());
            resultNode = ctx.NewAtom(node.Pos(), TStringBuf(buffer.data(), buffer.size()));
        }

        return resultNode;
    }

    std::pair<IGraphTransformer::TStatus, TAsyncTransformCallbackFuture> RunResOrPullForExec(TResOrPullBase res,
        TExprBase exec, TExprContext& ctx, ui32 resultIndex)
    {
        auto requireStatus = RequireChild(res.Ref(), 0);
        if (requireStatus.Level != TStatus::Ok) {
            return SyncStatus(requireStatus);
        }

        if (NCommon::HasResOrPullOption(res.Ref(), "ref")) {
            ctx.AddError(TIssue(ctx.GetPosition(res.Pos()), TStringBuilder() << "refselect isn't supported for Kikimr provider."));
            return SyncError();
        }

        IDataProvider::TFillSettings fillSettings = NCommon::GetFillSettings(res.Ref());

        auto* runResult = SessionCtx->Query().Results.FindPtr(exec.Ref().UniqueId());
        if (!runResult) {
            ctx.AddError(TIssue(ctx.GetPosition(exec.Pos()), TStringBuilder() << "KiExecute run result not found."));
            return SyncError();
        }

        if (SessionCtx->Query().PrepareOnly) {
            res.Ptr()->SetResult(ctx.NewWorld(res.Pos()));
            return SyncOk();
        }

        auto executeRightType = exec.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[1];
        auto resultType = executeRightType->Cast<TTupleExprType>()->GetItems()[resultIndex];

        YQL_ENSURE(resultIndex < runResult->Results.size());
        auto resultValue = runResult->Results[resultIndex];
        YQL_ENSURE(resultValue);

        auto resResultNode = GetResOrPullResult(res.Ref(), fillSettings, resultType, *resultValue, ctx);

        res.Ptr()->SetResult(std::move(resResultNode));
        return SyncOk();
    }

private:
    TIntrusivePtr<IKikimrGateway> Gateway;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
};

class TKiSinkCallableExecutionTransformer : public TAsyncCallbackTransformer<TKiSinkCallableExecutionTransformer> {
public:
    TKiSinkCallableExecutionTransformer(
        TIntrusivePtr<IKikimrGateway> gateway,
        TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        TIntrusivePtr<IKikimrQueryExecutor> queryExecutor)
        : Gateway(gateway)
        , SessionCtx(sessionCtx)
        , QueryExecutor(queryExecutor) {}

    std::pair<TStatus, TAsyncTransformCallbackFuture> CallbackTransform(const TExprNode::TPtr& input,
        TExprNode::TPtr& output, TExprContext& ctx)
    {
        YQL_ENSURE(input->Type() == TExprNode::Callable);
        output = input;

        if (auto maybeCommit = TMaybeNode<TCoCommit>(input)) {
            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            if (SessionCtx->HasTx()) {
                auto settings = NCommon::ParseCommitSettings(maybeCommit.Cast(), ctx);
                if (settings.Mode && settings.Mode.Cast().Value() == KikimrCommitModeScheme()) {
                    SessionCtx->Tx().Finish();
                }
            }

            input->SetState(TExprNode::EState::ExecutionComplete);
            input->SetResult(ctx.NewWorld(input->Pos()));
            return SyncOk();
        }

        if (auto maybeExecQuery = TMaybeNode<TKiExecDataQuery>(input)) {
            auto requireStatus = RequireChild(*input, TKiExecDataQuery::idx_World);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            return RunKiExecDataQuery(ctx, maybeExecQuery.Cast());
        }

        if (TMaybeNode<TCoNth>(input)) {
            input->SetState(TExprNode::EState::ExecutionComplete);
            input->SetResult(ctx.NewWorld(input->Pos()));
            return SyncOk();
        }

        if (auto maybeCreate = TMaybeNode<TKiCreateTable>(input)) {
            if (!EnsureNotPrepare("CREATE TABLE", input->Pos(), SessionCtx->Query(), ctx)) {
                return SyncError();
            }

            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            auto cluster = TString(maybeCreate.Cast().DataSink().Cluster());
            auto& table = SessionCtx->Tables().GetTable(cluster, TString(maybeCreate.Cast().Table()));

            if (!ApplyDdlOperation(cluster, input->Pos(), table.Metadata->Name, TYdbOperation::CreateTable, ctx)) {
                return SyncError();
            }

            auto future = Gateway->CreateTable(table.Metadata, true);

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                    Y_UNUSED(res);
                    auto resultNode = ctx.NewWorld(input->Pos());
                    return resultNode;
                }, "Executing CREATE TABLE");
        }

        if (auto maybeDrop = TMaybeNode<TKiDropTable>(input)) {
            if (!EnsureNotPrepare("DROP TABLE", input->Pos(), SessionCtx->Query(), ctx)) {
                return SyncError();
            }

            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            auto cluster = TString(maybeDrop.Cast().DataSink().Cluster());
            auto& table = SessionCtx->Tables().GetTable(cluster, TString(maybeDrop.Cast().Table()));

            if (!ApplyDdlOperation(cluster, input->Pos(), table.Metadata->Name, TYdbOperation::DropTable, ctx)) {
                return SyncError();
            }

            auto future = Gateway->DropTable(table.Metadata->Cluster, table.Metadata->Name);

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                    Y_UNUSED(res);
                    auto resultNode = ctx.NewWorld(input->Pos());
                    return resultNode;
                });

            input->SetState(TExprNode::EState::ExecutionComplete);
            input->SetResult(ctx.NewWorld(input->Pos()));
            return SyncOk();
        }

        if (auto maybeAlter = TMaybeNode<TKiAlterTable>(input)) {
            if (!EnsureNotPrepare("ALTER TABLE", input->Pos(), SessionCtx->Query(), ctx)) {
                return SyncError();
            }

            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            auto cluster = TString(maybeAlter.Cast().DataSink().Cluster());
            auto& table = SessionCtx->Tables().GetTable(cluster, TString(maybeAlter.Cast().Table()));

            if (!ApplyDdlOperation(cluster, input->Pos(), table.Metadata->Name, TYdbOperation::AlterTable, ctx)) {
                return SyncError();
            }

            Ydb::Table::AlterTableRequest alterTableRequest;
            alterTableRequest.set_path(table.Metadata->Name);

            for (auto action : maybeAlter.Cast().Actions()) {
                auto name = action.Name().Value();
                if (name == "renameTo") {
                    auto destination = action.Value().Cast<TCoAtom>().StringValue();
                    auto future = Gateway->RenameTable(table.Metadata->Name, destination, cluster);
                    return WrapFuture(future,
                        [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                            Y_UNUSED(res);
                            auto resultNode = ctx.NewWorld(input->Pos());
                            return resultNode;
                        });
                } else if (name == "addColumns") {
                    auto listNode = action.Value().Cast<TExprList>();
                    for (size_t i = 0; i < listNode.Size(); ++i) {
                        auto add_column = alterTableRequest.add_add_columns();
                        auto item = listNode.Item(i);
                        auto columnTuple = item.Cast<TExprList>();
                        auto columnName = columnTuple.Item(0).Cast<TCoAtom>();
                        add_column->set_name(TString(columnName));

                        auto typeNode = columnTuple.Item(1);
                        auto columnType = typeNode.Ref().GetTypeAnn();
                        auto type = columnType->Cast<TTypeExprType>()->GetType();
                        auto notNull = type->GetKind() != ETypeAnnotationKind::Optional;
                        auto actualType = notNull ? type : type->Cast<TOptionalExprType>()->GetItemType();
                        auto dataType = actualType->Cast<TDataExprType>();
                        SetColumnType(*add_column->mutable_type(), TString(dataType->GetName()), notNull);
                        if (columnTuple.Size() > 2) {
                            auto families = columnTuple.Item(2).Cast<TCoAtomList>();
                            if (families.Size() > 1) {
                                ctx.AddError(TIssue(ctx.GetPosition(families.Pos()),
                                    "Unsupported number of families"));
                                return SyncError();
                            }
                            for (auto family : families) {
                                add_column->set_family(TString(family.Value()));
                            }
                        }
                    }
                } else if (name == "dropColumns") {
                    auto listNode = action.Value().Cast<TCoAtomList>();
                    for (auto dropColumn : listNode) {
                        alterTableRequest.add_drop_columns(TString(dropColumn.Value()));
                    }
                } else if (name == "alterColumns") {
                    auto listNode = action.Value().Cast<TExprList>();
                    for (size_t i = 0; i < listNode.Size(); ++i) {
                        auto alter_columns = alterTableRequest.add_alter_columns();
                        auto item = listNode.Item(i);
                        auto columnTuple = item.Cast<TExprList>();
                        auto columnName = columnTuple.Item(0).Cast<TCoAtom>();
                        alter_columns->set_name(TString(columnName));

                        auto families = columnTuple.Item(1).Cast<TCoAtomList>();
                        if (families.Size() > 1) {
                            ctx.AddError(TIssue(ctx.GetPosition(families.Pos()),
                                "Unsupported number of families"));
                            return SyncError();
                        }
                        for (auto family : families) {
                            alter_columns->set_family(TString(family.Value()));
                        }
                    }
                } else if (name == "addColumnFamilies" || name == "alterColumnFamilies") {
                    auto listNode = action.Value().Cast<TExprList>();
                    for (size_t i = 0; i < listNode.Size(); ++i) {
                        auto item = listNode.Item(i);
                        if (auto maybeTupleList = item.Maybe<TCoNameValueTupleList>()) {
                            auto f = (name == "addColumnFamilies") ?
                                alterTableRequest.add_add_column_families() :
                                alterTableRequest.add_alter_column_families();

                            for (auto familySetting : maybeTupleList.Cast()) {
                                auto name = familySetting.Name().Value();
                                if (name == "name") {
                                    f->set_name(TString(familySetting.Value().Cast<TCoAtom>().Value()));
                                } else if (name == "data") {
                                    auto data = TString(
                                                familySetting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>()
                                                    .Value());
                                    f->mutable_data()->set_media(data);
                                } else if (name == "compression") {
                                    auto comp = TString(
                                        familySetting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>()
                                            .Value()
                                    );
                                    if (to_lower(comp) == "off") {
                                        f->set_compression(Ydb::Table::ColumnFamily::COMPRESSION_NONE);
                                    } else if (to_lower(comp) == "lz4") {
                                        f->set_compression(Ydb::Table::ColumnFamily::COMPRESSION_LZ4);
                                    } else {
                                        auto errText = TStringBuilder() << "Unknown compression '" << comp
                                            << "' for a column family";
                                        ctx.AddError(TIssue(ctx.GetPosition(familySetting.Name().Pos()),
                                            errText));
                                        return SyncError();
                                    }

                                } else {
                                    ctx.AddError(TIssue(ctx.GetPosition(familySetting.Name().Pos()),
                                        TStringBuilder() << "Unknown column family setting name: " << name));
                                    return SyncError();
                                }
                            }
                        }
                    }
                } else if (name == "setTableSettings") {
                    auto listNode = action.Value().Cast<TCoNameValueTupleList>();

                    for (const auto& setting : listNode) {
                        auto name = setting.Name().Value();
                        if (name == "compactionPolicy") {
                            alterTableRequest.set_set_compaction_policy(TString(
                                setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                            ));
                        } else if (name == "autoPartitioningBySize") {
                            auto partitioningSettings = alterTableRequest.mutable_alter_partitioning_settings();
                            auto val = to_lower(TString(setting.Value().Cast<TCoAtom>().Value()));
                            if (val == "enabled") {
                                partitioningSettings->set_partitioning_by_size(Ydb::FeatureFlag::ENABLED);
                            } else if (val == "disabled") {
                                partitioningSettings->set_partitioning_by_size(Ydb::FeatureFlag::DISABLED);
                            } else {
                                auto errText = TStringBuilder() << "Unknown feature flag '"
                                    << val
                                    << "' for auto partitioning by size";
                                ctx.AddError(TIssue(ctx.GetPosition(setting.Value().Cast<TCoAtom>().Pos()),
                                    errText));
                                return SyncError();
                            }
                        } else if (name == "partitionSizeMb") {
                            ui64 value = FromString<ui64>(
                                setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                                );
                            if (value) {
                                auto partitioningSettings = alterTableRequest.mutable_alter_partitioning_settings();
                                partitioningSettings->set_partition_size_mb(value);
                            } else {
                                ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                                    "Can't set preferred partition size to 0. "
                                    "To disable auto partitioning by size use 'SET AUTO_PARTITIONING_BY_SIZE DISABLED'"));
                                return SyncError();
                            }
                        } else if (name == "autoPartitioningByLoad") {
                            auto partitioningSettings = alterTableRequest.mutable_alter_partitioning_settings();
                            TString val = to_lower(TString(setting.Value().Cast<TCoAtom>().Value()));
                            if (val == "enabled") {
                                partitioningSettings->set_partitioning_by_load(Ydb::FeatureFlag::ENABLED);
                            } else if (val == "disabled") {
                                partitioningSettings->set_partitioning_by_load(Ydb::FeatureFlag::DISABLED);
                            } else {
                                auto errText = TStringBuilder() << "Unknown feature flag '"
                                    << val
                                    << "' for auto partitioning by load";
                                ctx.AddError(TIssue(ctx.GetPosition(setting.Value().Cast<TCoAtom>().Pos()),
                                    errText));
                                return SyncError();
                            }
                        } else if (name == "minPartitions") {
                            ui64 value = FromString<ui64>(
                                setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                                );
                            if (value) {
                                auto partitioningSettings = alterTableRequest.mutable_alter_partitioning_settings();
                                partitioningSettings->set_min_partitions_count(value);
                            } else {
                                ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                                    "Can't set min partition count to 0"));
                                return SyncError();
                            }
                        } else if (name == "maxPartitions") {
                            ui64 value = FromString<ui64>(
                                setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                                );
                            if (value) {
                                auto partitioningSettings = alterTableRequest.mutable_alter_partitioning_settings();
                                partitioningSettings->set_max_partitions_count(value);
                            } else {
                                ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                                    "Can't set max partition count to 0"));
                                return SyncError();
                            }
                        } else if (name == "keyBloomFilter") {
                            auto val = to_lower(TString(setting.Value().Cast<TCoAtom>().Value()));
                            if (val == "enabled") {
                                alterTableRequest.set_set_key_bloom_filter(Ydb::FeatureFlag::ENABLED);
                            } else if (val == "disabled") {
                                alterTableRequest.set_set_key_bloom_filter(Ydb::FeatureFlag::DISABLED);
                            } else {
                                auto errText = TStringBuilder() << "Unknown feature flag '"
                                    << val
                                    << "' for key bloom filter";
                                ctx.AddError(TIssue(ctx.GetPosition(setting.Value().Cast<TCoAtom>().Pos()),
                                    errText));
                                return SyncError();
                            }

                        } else if (name == "readReplicasSettings") {
                            const auto replicasSettings = TString(
                                setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                            );
                            Ydb::StatusIds::StatusCode code;
                            TString errText;
                            if (!ConvertReadReplicasSettingsToProto(replicasSettings,
                                 *alterTableRequest.mutable_set_read_replicas_settings(), code, errText)) {

                                ctx.AddError(YqlIssue(ctx.GetPosition(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Pos()),
                                                      NYql::YqlStatusFromYdbStatus(code),
                                                      errText));
                                return SyncError();
                            }
                        } else if (name == "setTtlSettings") {
                            TTtlSettings ttlSettings;
                            TString error;

                            YQL_ENSURE(setting.Value().Maybe<TCoNameValueTupleList>());
                            if (!TTtlSettings::TryParse(setting.Value().Cast<TCoNameValueTupleList>(), ttlSettings, error)) {
                                ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                                    TStringBuilder() << "Invalid TTL settings: " << error));
                                return SyncError();
                            }

                            ConvertTtlSettingsToProto(ttlSettings, *alterTableRequest.mutable_set_ttl_settings());
                        } else if (name == "resetTtlSettings") {
                            alterTableRequest.mutable_drop_ttl_settings();
                        } else {
                            ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                                TStringBuilder() << "Unknown table profile setting: " << name));
                            return SyncError();
                        }
                    }
                } else if (name == "addIndex") {
                    auto listNode = action.Value().Cast<TExprList>();
                    auto add_index = alterTableRequest.add_add_indexes();
                    for (size_t i = 0; i < listNode.Size(); ++i) {
                        auto item = listNode.Item(i);
                        auto columnTuple = item.Cast<TExprList>();
                        auto nameNode = columnTuple.Item(0).Cast<TCoAtom>();
                        auto name = TString(nameNode.Value());
                        if (name == "indexName") {
                            add_index->set_name(TString(columnTuple.Item(1).Cast<TCoAtom>().Value()));
                        } else if (name == "indexType") {
                            const auto type = TString(columnTuple.Item(1).Cast<TCoAtom>().Value());
                            if (type == "syncGlobal") {
                                add_index->mutable_global_index();
                            } else if (type == "asyncGlobal") {
                                add_index->mutable_global_async_index();
                            } else {
                                ctx.AddError(TIssue(ctx.GetPosition(columnTuple.Item(1).Cast<TCoAtom>().Pos()),
                                    TStringBuilder() << "Unknown index type: " << type));
                                return SyncError();
                            }
                        } else if (name == "indexColumns") {
                            auto columnList = columnTuple.Item(1).Cast<TCoAtomList>();
                            for (auto column : columnList) {
                                TString columnName(column.Value());
                                add_index->add_index_columns(columnName);
                            }
                        } else if (name == "dataColumns") {
                            auto columnList = columnTuple.Item(1).Cast<TCoAtomList>();
                            for (auto column : columnList) {
                                TString columnName(column.Value());
                                add_index->add_data_columns(columnName);
                            }
                        } else {
                            ctx.AddError(TIssue(ctx.GetPosition(nameNode.Pos()),
                                TStringBuilder() << "Unknown add index setting: " << name));
                            return SyncError();
                        }
                    }
                    switch (add_index->type_case()) {
                        case Ydb::Table::TableIndex::kGlobalIndex:
                            *add_index->mutable_global_index() = Ydb::Table::GlobalIndex();
                            break;
                        case Ydb::Table::TableIndex::kGlobalAsyncIndex:
                            *add_index->mutable_global_async_index() = Ydb::Table::GlobalAsyncIndex();
                            break;
                        default:
                            YQL_ENSURE(false, "Unknown index type: " << (ui32)add_index->type_case());
                    }
                } else if (name == "dropIndex") {
                    auto nameNode = action.Value().Cast<TCoAtom>();
                    alterTableRequest.add_drop_indexes(TString(nameNode.Value()));
                } else if (name == "addChangefeed") {
                    auto listNode = action.Value().Cast<TExprList>();
                    auto add_changefeed = alterTableRequest.add_add_changefeeds();
                    for (size_t i = 0; i < listNode.Size(); ++i) {
                        auto item = listNode.Item(i);
                        auto columnTuple = item.Cast<TExprList>();
                        auto nameNode = columnTuple.Item(0).Cast<TCoAtom>();
                        auto name = TString(nameNode.Value());
                        if (name == "name") {
                            add_changefeed->set_name(TString(columnTuple.Item(1).Cast<TCoAtom>().Value()));
                        } else if (name == "settings") {
                            for (const auto& setting : columnTuple.Item(1).Cast<TCoNameValueTupleList>()) {
                                auto name = setting.Name().Value();
                                if (name == "mode") {
                                    auto mode = TString(
                                        setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                                    );

                                    if (to_lower(mode) == "keys_only") {
                                        add_changefeed->set_mode(Ydb::Table::ChangefeedMode::MODE_KEYS_ONLY);
                                    } else if (to_lower(mode) == "updates") {
                                        add_changefeed->set_mode(Ydb::Table::ChangefeedMode::MODE_UPDATES);
                                    } else if (to_lower(mode) == "new_image") {
                                        add_changefeed->set_mode(Ydb::Table::ChangefeedMode::MODE_NEW_IMAGE);
                                    } else if (to_lower(mode) == "old_image") {
                                        add_changefeed->set_mode(Ydb::Table::ChangefeedMode::MODE_OLD_IMAGE);
                                    } else if (to_lower(mode) == "new_and_old_images") {
                                        add_changefeed->set_mode(Ydb::Table::ChangefeedMode::MODE_NEW_AND_OLD_IMAGES);
                                    } else {
                                        ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                                            TStringBuilder() << "Unknown changefeed mode: " << mode));
                                        return SyncError();
                                    }
                                } else if (name == "format") {
                                    auto format = TString(
                                        setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                                    );

                                    if (to_lower(format) == "json") {
                                        add_changefeed->set_format(Ydb::Table::ChangefeedFormat::FORMAT_JSON);
                                    } else {
                                        ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                                            TStringBuilder() << "Unknown changefeed format: " << format));
                                        return SyncError();
                                    }
                                } else if (name == "local") {
                                    // nop
                                } else {
                                    ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                                        TStringBuilder() << "Unknown changefeed setting: " << name));
                                    return SyncError();
                                }
                            }
                        } else if (name == "state") {
                            YQL_ENSURE(!columnTuple.Item(1).Maybe<TCoAtom>());
                        } else {
                            ctx.AddError(TIssue(ctx.GetPosition(nameNode.Pos()),
                                TStringBuilder() << "Unknown add changefeed setting: " << name));
                            return SyncError();
                        }
                    }
                } else if (name == "dropChangefeed") {
                    auto nameNode = action.Value().Cast<TCoAtom>();
                    alterTableRequest.add_drop_changefeeds(TString(nameNode.Value()));
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(action.Name().Pos()),
                        TStringBuilder() << "Unknown alter table action: " << name));
                    return SyncError();
                }
            }

            auto future = Gateway->AlterTable(std::move(alterTableRequest), cluster);

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                    Y_UNUSED(res);
                    auto resultNode = ctx.NewWorld(input->Pos());
                    return resultNode;
                });

        }

        if (auto maybeCreateUser = TMaybeNode<TKiCreateUser>(input)) {
            if (!EnsureNotPrepare("CREATE USER", input->Pos(), SessionCtx->Query(), ctx)) {
                return SyncError();
            }

            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            auto cluster = TString(maybeCreateUser.Cast().DataSink().Cluster());
            TCreateUserSettings createUserSettings = ParseCreateUserSettings(maybeCreateUser.Cast());

            auto future = Gateway->CreateUser(cluster, createUserSettings);

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                Y_UNUSED(res);
                auto resultNode = ctx.NewWorld(input->Pos());
                return resultNode;
            }, "Executing CREATE USER");
        }

        if (auto maybeAlterUser = TMaybeNode<TKiAlterUser>(input)) {
            if (!EnsureNotPrepare("ALTER USER", input->Pos(), SessionCtx->Query(), ctx)) {
                return SyncError();
            }

            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            auto cluster = TString(maybeAlterUser.Cast().DataSink().Cluster());
            TAlterUserSettings alterUserSettings = ParseAlterUserSettings(maybeAlterUser.Cast());

            auto future = Gateway->AlterUser(cluster, alterUserSettings);

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                Y_UNUSED(res);
                auto resultNode = ctx.NewWorld(input->Pos());
                return resultNode;
            }, "Executing ALTER USER");
        }

        if (auto maybeDropUser = TMaybeNode<TKiDropUser>(input)) {
            if (!EnsureNotPrepare("DROP USER", input->Pos(), SessionCtx->Query(), ctx)) {
                return SyncError();
            }

            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            auto cluster = TString(maybeDropUser.Cast().DataSink().Cluster());
            TDropUserSettings dropUserSettings = ParseDropUserSettings(maybeDropUser.Cast());

            auto future = Gateway->DropUser(cluster, dropUserSettings);

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                Y_UNUSED(res);
                auto resultNode = ctx.NewWorld(input->Pos());
                return resultNode;
            }, "Executing DROP USER");
        }

        if (auto maybeCreateGroup = TMaybeNode<TKiCreateGroup>(input)) {
            if (!EnsureNotPrepare("CREATE GROUP", input->Pos(), SessionCtx->Query(), ctx)) {
                return SyncError();
            }

            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            auto cluster = TString(maybeCreateGroup.Cast().DataSink().Cluster());
            TCreateGroupSettings createGroupSettings = ParseCreateGroupSettings(maybeCreateGroup.Cast());

            auto future = Gateway->CreateGroup(cluster, createGroupSettings);

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                Y_UNUSED(res);
                auto resultNode = ctx.NewWorld(input->Pos());
                return resultNode;
            }, "Executing CREATE GROUP");
        }

        if (auto maybeAlterGroup = TMaybeNode<TKiAlterGroup>(input)) {
            if (!EnsureNotPrepare("ALTER GROUP", input->Pos(), SessionCtx->Query(), ctx)) {
                return SyncError();
            }

            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            auto cluster = TString(maybeAlterGroup.Cast().DataSink().Cluster());
            TAlterGroupSettings alterGroupSettings = ParseAlterGroupSettings(maybeAlterGroup.Cast());

            auto future = Gateway->AlterGroup(cluster, alterGroupSettings);

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                Y_UNUSED(res);
                auto resultNode = ctx.NewWorld(input->Pos());
                return resultNode;
            }, "Executing ALTER GROUP");
        }

        if (auto maybeDropGroup = TMaybeNode<TKiDropGroup>(input)) {
            if (!EnsureNotPrepare("DROP GROUP", input->Pos(), SessionCtx->Query(), ctx)) {
                return SyncError();
            }

            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            auto cluster = TString(maybeDropGroup.Cast().DataSink().Cluster());
            TDropGroupSettings dropGroupSettings = ParseDropGroupSettings(maybeDropGroup.Cast());

            auto future = Gateway->DropGroup(cluster, dropGroupSettings);

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                Y_UNUSED(res);
                auto resultNode = ctx.NewWorld(input->Pos());
                return resultNode;
            }, "Executing DROP GROUP");
        }

        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
            << "(Kikimr DataSink) Failed to execute node: " << input->Content()));
        return SyncError();
    }

private:
    using TExecuteRunFunc = std::function<TIntrusivePtr<IKikimrQueryExecutor::TAsyncQueryResult>(
        const IKikimrQueryExecutor::TExecuteSettings& settings)>;
    using TExecuteFinalizeFunc = std::function<void(TExprBase node, const IKikimrQueryExecutor::TQueryResult& result,
        TExprContext& ctx)>;

    std::pair<IGraphTransformer::TStatus, TAsyncTransformCallbackFuture> PerformExecution(TExprBase node,
        TExprContext& ctx, const TString& cluster, TMaybe<TString> mode, TMaybe<bool> useNewEngine,
        const TExecuteRunFunc& runFunc, const TExecuteFinalizeFunc& finalizeFunc)
    {
        if (node.Ref().GetState() == TExprNode::EState::ExecutionComplete) {
            return SyncOk();
        }

        auto resultId = node.Ref().UniqueId();

        TIntrusivePtr<IKikimrQueryExecutor::TAsyncQueryResult> asyncResult;
        if (auto resultPtr = SessionCtx->Query().InProgress.FindPtr(resultId)) {
            asyncResult = *resultPtr;
        } else {
            auto config = SessionCtx->Config().Snapshot();

            auto isolationLevel = config->IsolationLevel.Get(cluster);

            IKikimrQueryExecutor::TExecuteSettings settings;
            settings.CommitTx = true;
            if (mode) {
                if (*mode == KikimrCommitModeFlush()) {
                    settings.CommitTx = false;
                }

                if (*mode == KikimrCommitModeRollback()) {
                    settings.CommitTx = false;
                    settings.RollbackTx = true;
                }
            }

            settings.IsolationLevel = isolationLevel;
            settings.UseNewEngine = useNewEngine;
            settings.StrictDml = config->StrictDml.Get(cluster).GetRef();

            const auto& scanQuery = config->ScanQuery.Get(cluster);
            if (scanQuery) {
                settings.UseScanQuery = scanQuery.GetRef();
            }

            settings.StatsMode = SessionCtx->Query().StatsMode;
            auto profile = config->Profile.Get(cluster);
            if (profile && *profile) {
                // Do not disable profiling if it was enabled at request level
                settings.StatsMode = EKikimrStatsMode::Profile;
            }

            asyncResult = runFunc(settings);

            auto insertResult = SessionCtx->Query().InProgress.insert(std::make_pair(resultId, asyncResult));
            YQL_ENSURE(insertResult.second);
        }

        YQL_ENSURE(asyncResult);

        if (asyncResult->HasResult()) {
            SessionCtx->Query().InProgress.erase(resultId);

            auto result = asyncResult->GetResult();
            if (!result.Success()) {
                node.Ptr()->SetState(TExprNode::EState::Error);

                return std::make_pair(IGraphTransformer::TStatus::Error, TAsyncTransformCallbackFuture());
            }

            auto insertResult = SessionCtx->Query().Results.emplace(resultId, std::move(result));
            YQL_ENSURE(insertResult.second);

            SessionCtx->Query().ExecutionOrder.push_back(resultId);

            node.Ptr()->SetState(TExprNode::EState::ExecutionComplete);
            node.Ptr()->SetResult(ctx.NewAtom(node.Pos(), ToString(resultId)));

            if (finalizeFunc) {
                finalizeFunc(node, result, ctx);
            }

            return std::make_pair(IGraphTransformer::TStatus::Ok, TAsyncTransformCallbackFuture());
        }

        return std::make_pair(IGraphTransformer::TStatus::Async, asyncResult->Continue().Apply(
            [](const TFuture<bool>& future) {
                Y_UNUSED(future);

                return TAsyncTransformCallback(
                    [](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                        Y_UNUSED(ctx);
                        output = input;

                        input->SetState(TExprNode::EState::ExecutionRequired);
                        return IGraphTransformer::TStatus::Repeat;
                    });
            }));
    }

    std::pair<IGraphTransformer::TStatus, TAsyncTransformCallbackFuture> RunKiExecDataQuery(TExprContext& ctx,
        const TKiExecDataQuery& execQuery)
    {
        auto cluster = TString(execQuery.DataSink().Cluster());
        auto settings = TKiExecDataQuerySettings::Parse(execQuery);

        auto runFunc = [this, cluster, execQuery, &ctx](const IKikimrQueryExecutor::TExecuteSettings& settings)
            -> TIntrusivePtr<IKikimrQueryExecutor::TAsyncQueryResult>
        {
            return QueryExecutor->ExecuteDataQuery(cluster, execQuery.Query().Ptr(), ctx, settings);
        };

        auto finalizeFunc = [] (TExprBase node, const IKikimrQueryExecutor::TQueryResult& result, TExprContext& ctx) {
            FillExecDataQueryAst(node.Cast<TKiExecDataQuery>(), result.QueryAst, ctx);
        };

        return PerformExecution(execQuery, ctx, cluster, settings.Mode, settings.UseNewEngine, runFunc, finalizeFunc);
    }

    bool ApplyTableOperations(const TString& cluster, const TVector<NKqpProto::TKqpTableOp>& tableOps,
        NKikimrKqp::EIsolationLevel isolationLevel, TExprContext& ctx)
    {
        bool strictDml = SessionCtx->Config().StrictDml.Get(cluster).GetRef();
        auto queryType = SessionCtx->Query().Type;
        TVector<NKqpProto::TKqpTableInfo> tableInfo;

        for (const auto& op : tableOps) {
            auto table = op.GetTable();
            const auto& desc = SessionCtx->Tables().GetTable(cluster, table);
            YQL_ENSURE(desc.Metadata);
            tableInfo.push_back(NKqpProto::TKqpTableInfo());
            TableDescriptionToTableInfo(desc, &tableInfo.back());
        }

        if (!SessionCtx->HasTx()) {
            TKikimrTransactionContextBase emptyCtx;
            return emptyCtx.ApplyTableOperations(tableOps, tableInfo, isolationLevel, strictDml, queryType, ctx);
        }

        return SessionCtx->Tx().ApplyTableOperations(tableOps, tableInfo, isolationLevel, strictDml, queryType, ctx);
    }

    bool ApplyDdlOperation(const TString& cluster, TPositionHandle pos, const TString& table,
        TYdbOperation op, TExprContext& ctx)
    {
        YQL_ENSURE(op & KikimrSchemeOps());

        auto position = ctx.GetPosition(pos);

        NKqpProto::TKqpTableOp protoOp;
        protoOp.MutablePosition()->SetRow(position.Row);
        protoOp.MutablePosition()->SetColumn(position.Column);
        protoOp.SetTable(table);
        protoOp.SetOperation((ui32)op);

        return ApplyTableOperations(cluster, {protoOp}, NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE, ctx);
    }

private:
    TIntrusivePtr<IKikimrGateway> Gateway;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TIntrusivePtr<IKikimrQueryExecutor> QueryExecutor;
};

} // namespace

NKikimrKqp::EIsolationLevel GetIsolationLevel(const TMaybe<TString>& isolationLevel) {
    YQL_ENSURE(isolationLevel);
    auto levelStr = *isolationLevel;

    if (levelStr == "ReadStale") {
        return NKikimrKqp::ISOLATION_LEVEL_READ_STALE;
    } else if (levelStr == "ReadUncommitted") {
        return NKikimrKqp::ISOLATION_LEVEL_READ_UNCOMMITTED;
    } else if (levelStr == "ReadCommitted") {
        return NKikimrKqp::ISOLATION_LEVEL_READ_COMMITTED;
    } else if (levelStr == "Serializable") {
        return NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE;
    }

    YQL_ENSURE(false, "Unsupported isolation level: " << levelStr);
}

TMaybe<TString> GetIsolationLevel(const NKikimrKqp::EIsolationLevel& isolationLevel) {
    switch (isolationLevel) {
        case NKikimrKqp::ISOLATION_LEVEL_READ_STALE: return "ReadStale";
        case NKikimrKqp::ISOLATION_LEVEL_READ_UNCOMMITTED: return "ReadUncommitted";
        case NKikimrKqp::ISOLATION_LEVEL_READ_COMMITTED: return "ReadCommitted";
        case NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE: return "Serializable";
        default:
            return TMaybe<TString>();
    }
}

TAutoPtr<IGraphTransformer> CreateKiSourceCallableExecutionTransformer(
    TIntrusivePtr<IKikimrGateway> gateway,
    TIntrusivePtr<TKikimrSessionContext> sessionCtx)
{
    return new TKiSourceCallableExecutionTransformer(gateway, sessionCtx);
}

TAutoPtr<IGraphTransformer> CreateKiSinkCallableExecutionTransformer(
    TIntrusivePtr<IKikimrGateway> gateway,
    TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    TIntrusivePtr<IKikimrQueryExecutor> queryExecutor)
{
    return new TKiSinkCallableExecutionTransformer(gateway, sessionCtx, queryExecutor);
}

TAutoPtr<IGraphTransformer> CreateKiSinkPlanInfoTransformer(TIntrusivePtr<IKikimrQueryExecutor> queryExecutor) {
    return new TKiSinkPlanInfoTransformer(queryExecutor);
}

} // namespace NYql
