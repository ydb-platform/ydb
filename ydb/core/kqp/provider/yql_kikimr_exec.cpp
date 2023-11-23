#include "yql_kikimr_provider_impl.h"

#include <ydb/core/docapi/traits.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/yql_execution.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>

#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <ydb/library/yql/dq/tasks/dq_task_program.h>

#include <ydb/library/yql/minikql/mkql_program_builder.h>

#include <ydb/core/kqp/provider/yql_kikimr_results.h>

namespace NYql {
namespace {

using namespace NNodes;
using namespace NCommon;
using namespace NThreading;

namespace {
    NThreading::TFuture<IKikimrGateway::TGenericResult> CreateDummySuccess() {
        IKikimrGateway::TGenericResult result;
        result.SetSuccess();
        return NThreading::MakeFuture(result);
    }

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

    TModifyPermissionsSettings ParsePermissionsSettings(TKiModifyPermissions modifyPermissions) {
        TModifyPermissionsSettings permissionsSettings;

        TString action = modifyPermissions.Action().StringValue();
        if (action == "grant") {
            permissionsSettings.Action = TModifyPermissionsSettings::EAction::Grant;
        } else if (action == "revoke") {
            permissionsSettings.Action = TModifyPermissionsSettings::EAction::Revoke;
        }

        for (auto atom : modifyPermissions.Permissions()) {
            const TString permission = atom.Cast<TCoAtom>().StringValue();
            if (permission == "all_privileges") {
                if (permissionsSettings.Action == TModifyPermissionsSettings::EAction::Grant) {
                    permissionsSettings.Permissions.insert("ydb.generic.full");
                } else if (permissionsSettings.Action == TModifyPermissionsSettings::EAction::Revoke) {
                    permissionsSettings.Permissions.clear();
                    permissionsSettings.IsPermissionsClear = true;
                    break;
                }
                continue;
            }
            permissionsSettings.Permissions.insert(NKikimr::ConvertShortYdbPermissionNameToFullYdbPermissionName(permission));
        }

        THashSet<TString> pathesMap;
        for (auto atom : modifyPermissions.Pathes()) {
            permissionsSettings.Pathes.insert(atom.Cast<TCoAtom>().StringValue());
        }

        for (auto atom : modifyPermissions.Roles()) {
            permissionsSettings.Roles.insert(atom.Cast<TCoAtom>().StringValue());
        }
        return permissionsSettings;
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

    TCreateTableStoreSettings ParseCreateTableStoreSettings(TKiCreateTable create, const TTableSettings& settings) {
        TCreateTableStoreSettings out;
        out.TableStore = TString(create.Table());
        out.ShardsCount = settings.MinPartitions ? *settings.MinPartitions : 0;

        for (auto atom : create.PrimaryKey()) {
            out.KeyColumnNames.emplace_back(atom.Value());
        }

        for (auto item : create.Columns()) {
            auto columnTuple = item.Cast<TExprList>();
            auto nameNode = columnTuple.Item(0).Cast<TCoAtom>();
            auto typeNode = columnTuple.Item(1);

            auto columnName = TString(nameNode.Value());
            auto columnType = typeNode.Ref().GetTypeAnn();
            YQL_ENSURE(columnType && columnType->GetKind() == ETypeAnnotationKind::Type);

            auto type = columnType->Cast<TTypeExprType>()->GetType();
            auto notNull = type->GetKind() != ETypeAnnotationKind::Optional;
            auto actualType = notNull ? type : type->Cast<TOptionalExprType>()->GetItemType();
            auto dataType = actualType->Cast<TDataExprType>();

            TKikimrColumnMetadata columnMeta;
            columnMeta.Name = columnName;
            columnMeta.Type = dataType->GetName();
            columnMeta.NotNull = notNull;

            out.ColumnOrder.push_back(columnName);
            out.Columns.insert(std::make_pair(columnName, columnMeta));
        }
#if 0 // TODO
        for (const auto& index : create.Indexes()) {
            TIndexDescription indexDesc;
            out.Indexes.push_back(indexDesc);
        }
#endif
        return out;
    }

    TCreateExternalTableSettings ParseCreateExternalTableSettings(TKiCreateTable create, const TTableSettings& settings) {
        TCreateExternalTableSettings out;
        out.ExternalTable = TString(create.Table());

        for (auto item : create.Columns()) {
            auto columnTuple = item.Cast<TExprList>();
            auto nameNode = columnTuple.Item(0).Cast<TCoAtom>();
            auto typeNode = columnTuple.Item(1);

            auto columnName = TString(nameNode.Value());
            auto columnType = typeNode.Ref().GetTypeAnn();
            YQL_ENSURE(columnType && columnType->GetKind() == ETypeAnnotationKind::Type);

            auto type = columnType->Cast<TTypeExprType>()->GetType();
            auto notNull = type->GetKind() != ETypeAnnotationKind::Optional;
            auto actualType = RemoveAllOptionals(type);

            TKikimrColumnMetadata columnMeta;
            columnMeta.Name = columnName;
            columnMeta.Type = FormatType(actualType);
            columnMeta.NotNull = notNull;

            out.ColumnOrder.push_back(columnName);
            out.Columns.insert(std::make_pair(columnName, columnMeta));
        }
        if (settings.DataSourcePath) {
            out.DataSourcePath = *settings.DataSourcePath;
        }
        if (settings.Location) {
            out.Location = *settings.Location;
        }
        out.SourceTypeParameters = settings.ExternalSourceParameters;
        return out;
    }

    TDropExternalTableSettings ParseDropExternalTableSettings(TKiDropTable drop) {
        return TDropExternalTableSettings{
            .ExternalTable = TString(drop.Table())
        };
    }

    TAlterTableStoreSettings ParseAlterTableStoreSettings(TKiAlterTable alter) {
        return TAlterTableStoreSettings{
            .TableStore = TString(alter.Table())
        };
    }

    TDropTableStoreSettings ParseDropTableStoreSettings(TKiDropTable drop) {
        return TDropTableStoreSettings{
            .TableStore = TString(drop.Table())
        };
    }

    TAlterColumnTableSettings ParseAlterColumnTableSettings(TKiAlterTable alter) {
        return TAlterColumnTableSettings{
            .Table = TString(alter.Table())
        };
    }

    [[nodiscard]] TString AddConsumerToTopicRequest(
            Ydb::Topic::Consumer* protoConsumer, const TCoTopicConsumer& consumer
    ) {
        protoConsumer->set_name(consumer.Name().StringValue());
        auto settings = consumer.Settings().Cast<TCoNameValueTupleList>();
        for (const auto& setting : settings) {
            auto name = setting.Name().Value();
            if (name == "important") {
                protoConsumer->set_important(FromString<bool>(
                        setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                ));
            } else if (name == "setReadFromTs") {
                ui64 tsValue = 0;
                if(setting.Value().Maybe<TCoDatetime>()) {
                    tsValue = FromString<ui64>(setting.Value().Cast<TCoDatetime>().Literal().Value());
                } else if (setting.Value().Maybe<TCoTimestamp>()) {
                    tsValue = static_cast<ui64>(
                            FromString<ui64>(setting.Value().Cast<TCoTimestamp>().Literal().Value()) / 1'000'000
                    );
                } else {
                    try {
                        tsValue = FromString<ui64>(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value());
                    } catch (const yexception& e) {
                        return TStringBuilder() <<  "Failed to parse read_from setting value for consumer"
                                                << consumer.Name().StringValue()
                                                << ". Datetime(), Timestamp or integer value is supported";
                    }
                }
                protoConsumer->mutable_read_from()->set_seconds(tsValue);

            } else if (name == "setSupportedCodecs") {
                auto codecs = GetTopicCodecsFromString(
                        TString(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value())
                );

                auto* protoCodecs = protoConsumer->mutable_supported_codecs();
                for (auto codec : codecs) {
                    protoCodecs->add_codecs(codec);
                }
            }
        }
        return {};
    }

    [[nodiscard]] TString AddAlterConsumerToTopicRequest(
            Ydb::Topic::AlterConsumer* protoConsumer, const TCoTopicConsumer& consumer
    ) {
        protoConsumer->set_name(consumer.Name().StringValue());
        auto settings = consumer.Settings().Cast<TCoNameValueTupleList>();
        for (const auto& setting : settings) {
            //ToDo[RESET]: Add reset when supported
            auto name = setting.Name().Value();
            if (name == "important") {
                protoConsumer->set_set_important(FromString<bool>(
                        setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                ));
            } else if (name == "setReadFromTs") {
                ui64 tsValue = 0;
                if(setting.Value().Maybe<TCoDatetime>()) {
                    tsValue = FromString<ui64>(setting.Value().Cast<TCoDatetime>().Literal().Value());
                } else if (setting.Value().Maybe<TCoTimestamp>()) {
                    tsValue = static_cast<ui64>(
                            FromString<ui64>(setting.Value().Cast<TCoTimestamp>().Literal().Value()) / 1'000'000
                    );
                } else {
                    try {
                        tsValue = FromString<ui64>(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value());
                    } catch (const yexception& e) {
                        return TStringBuilder() <<  "Failed to parse read_from setting value for consumer"
                                                << consumer.Name().StringValue()
                                                << ". Datetime(), Timestamp or integer value is supported";
                    }
                }
                protoConsumer->mutable_set_read_from()->set_seconds(tsValue);
            } else if (name == "setSupportedCodecs") {
                auto codecs = GetTopicCodecsFromString(
                        TString(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value())
                );
                auto* protoCodecs = protoConsumer->mutable_set_supported_codecs();
                for (auto codec : codecs) {
                    protoCodecs->add_codecs(codec);
                }
            }
        }
        return {};
    }

    void AddTopicSettingsToRequest(Ydb::Topic::CreateTopicRequest* request, const TCoNameValueTupleList& topicSettings) {
        for (const auto& setting : topicSettings) {
            auto name = setting.Name().Value();
            if (name == "setMinPartitions") {
                request->mutable_partitioning_settings()->set_min_active_partitions(
                        FromString<ui32>(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value())
                );
            } else if (name == "setPartitionsLimit") {
                request->mutable_partitioning_settings()->set_partition_count_limit(
                        FromString<ui32>(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value())
                );
            } else if (name == "setRetentionPeriod") {
                auto microValue = FromString<ui64>(setting.Value().Cast<TCoInterval>().Literal().Value());
                request->mutable_retention_period()->set_seconds(
                        static_cast<ui64>(microValue / 1'000'000)
                );
            } else if (name == "setPartitionWriteSpeed") {
                request->set_partition_write_speed_bytes_per_second(
                        FromString<ui64>(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value())
                );
            } else if (name == "setPartitionWriteBurstSpeed") {
                request->set_partition_write_burst_bytes(
                        FromString<ui64>(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value())
                );
            }  else if (name == "setMeteringMode") {
                Ydb::Topic::MeteringMode meteringMode;
                auto result = GetTopicMeteringModeFromString(
                        TString(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()),
                        meteringMode
                );
                YQL_ENSURE(result);
                request->set_metering_mode(meteringMode);
            } else if (name == "setSupportedCodecs") {
                auto codecs = GetTopicCodecsFromString(
                        TString(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value())
                );
                auto* protoCodecs = request->mutable_supported_codecs();
                for (auto codec : codecs) {
                    protoCodecs->add_codecs(codec);
                }
            }
        }
    }

    void AddAlterTopicSettingsToRequest(Ydb::Topic::AlterTopicRequest* request, const TCoNameValueTupleList& topicSettings) {
    //ToDo [RESET]: Add reset options once supported.
        for (const auto& setting : topicSettings) {
            auto name = setting.Name().Value();
            if (name == "setMinPartitions") {
                request->mutable_alter_partitioning_settings()->set_set_min_active_partitions(
                        FromString<ui32>(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value())
                );
            } else if (name == "setPartitionsLimit") {
                request->mutable_alter_partitioning_settings()->set_set_partition_count_limit(
                        FromString<ui32>(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value())
                );
            } else if (name == "setRetentionPeriod") {
                auto microValue = FromString<ui64>(setting.Value().Cast<TCoInterval>().Literal().Value());
                request->mutable_set_retention_period()->set_seconds(
                        static_cast<ui64>(microValue / 1'000'000)
                );
            } else if (name == "setPartitionWriteSpeed") {
                request->set_set_partition_write_speed_bytes_per_second(
                        FromString<ui64>(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value())
                );
            } else if (name == "setPartitionWriteBurstSpeed") {
                request->set_set_partition_write_burst_bytes(
                        FromString<ui64>(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value())
                );
            }  else if (name == "setMeteringMode") {
                Ydb::Topic::MeteringMode meteringMode;
                auto result = GetTopicMeteringModeFromString(
                        TString(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()),
                        meteringMode
                );
                YQL_ENSURE(result);
                request->set_set_metering_mode(meteringMode);
            } else if (name == "setSupportedCodecs") {
                auto codecs = GetTopicCodecsFromString(
                        TString(setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value())
                );
                auto* protoCodecs = request->mutable_set_supported_codecs();
                for (auto codec : codecs) {
                    protoCodecs->add_codecs(codec);
                }
            }
        }
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

    void Rewind() final {
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
private:
    IGraphTransformer::TStatus PeepHole(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) const {
        TPeepholeSettings peepholeSettings;
        bool hasNonDeterministicFunctions;
        auto status = PeepHoleOptimizeNode(input, output, ctx, TypesCtx, nullptr, hasNonDeterministicFunctions, peepholeSettings);
        if (status.Level != TStatus::Ok) {
            ctx.AddError(TIssue(ctx.GetPosition(output->Pos()), TString("Peephole optimization failed for Dq stage")));
            return status;
        }
        return status;
    }

    IGraphTransformer::TStatus GetLambdaBody(TExprNode::TPtr resInput, NKikimrMiniKQL::TType& resultType, TExprContext& ctx, TString& lambda) {
        auto pos = resInput->Pos();
        auto typeAnn = resInput->GetTypeAnn();

        const auto kind = resInput->GetTypeAnn()->GetKind();
        const bool data = kind != ETypeAnnotationKind::Flow && kind != ETypeAnnotationKind::Stream && kind != ETypeAnnotationKind::Optional;
        auto node = ctx.WrapByCallableIf(kind != ETypeAnnotationKind::Stream, "ToStream",
                        ctx.WrapByCallableIf(data, "Just", std::move(resInput)));

        auto peepHoleStatus = PeepHole(node, node, ctx);
        if (peepHoleStatus.Level != IGraphTransformer::TStatus::Ok) {
            return peepHoleStatus;
        }

        auto guard = Guard(SessionCtx->Query().QueryData->GetAllocState()->Alloc);

        auto input = Build<TDqPhyStage>(ctx, pos)
            .Inputs()
                .Build()
            .Program<TCoLambda>()
                .Args({})
                .Body(node)
            .Build()
            .Settings().Build()
        .Done().Ptr();

        NCommon::TMkqlCommonCallableCompiler compiler;

        auto programLambda = TDqPhyStage(input).Program();

        TVector<TExprBase> fakeReads;
        auto paramsType = NDq::CollectParameters(programLambda, ctx);
        lambda = NDq::BuildProgram(
            programLambda, *paramsType, compiler, SessionCtx->Query().QueryData->GetAllocState()->TypeEnv,
                *SessionCtx->Query().QueryData->GetAllocState()->HolderFactory.GetFunctionRegistry(),
                ctx, fakeReads);

        NKikimr::NMiniKQL::TProgramBuilder programBuilder(SessionCtx->Query().QueryData->GetAllocState()->TypeEnv,
            *SessionCtx->Query().QueryData->GetAllocState()->HolderFactory.GetFunctionRegistry());

        TStringStream errorStream;
        auto type = NYql::NCommon::BuildType(*typeAnn, programBuilder, errorStream);
        ExportTypeToProto(type, resultType);

        return IGraphTransformer::TStatus::Ok;
    }

    TString EncodeResultToYson(const NKikimrMiniKQL::TResult& result, bool& truncated) {
        TStringStream ysonStream;
        NYson::TYsonWriter writer(&ysonStream, NYson::EYsonFormat::Binary);
        NYql::IDataProvider::TFillSettings fillSettings;
        KikimrResultToYson(ysonStream, writer, result, {}, fillSettings, truncated);

        TStringStream out;
        NYson::TYsonWriter writer2((IOutputStream*)&out);
        writer2.OnBeginMap();
        writer2.OnKeyedItem("Data");
        writer2.OnRaw(ysonStream.Str());
        writer2.OnEndMap();

        return out.Str();
    }

public:
    TKiSourceCallableExecutionTransformer(TIntrusivePtr<IKikimrGateway> gateway,
        TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        TTypeAnnotationContext& types)
        : Gateway(gateway)
        , SessionCtx(sessionCtx)
        , TypesCtx(types) {}

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
        }

        if (input->Content() == "Result") {
            auto result = TMaybeNode<TResult>(input).Cast();
            NKikimrMiniKQL::TType resultType;
            TString program;
            TStatus status = GetLambdaBody(result.Input().Ptr(), resultType, ctx, program);
            if (status.Level != TStatus::Ok) {
                return SyncStatus(status);
            }
            auto asyncResult = Gateway->ExecuteLiteral(program, resultType, SessionCtx->Query().QueryData->GetAllocState());

            return std::make_pair(IGraphTransformer::TStatus::Async, asyncResult.Apply(
                [this](const NThreading::TFuture<IKikimrGateway::TExecuteLiteralResult>& future) {
                    return TAsyncTransformCallback(
                        [future, this](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {

                            const auto& literalResult = future.GetValueSync();

                            if (!literalResult.Success()) {
                                for (const auto& issue : literalResult.Issues()) {
                                    ctx.AddError(issue);
                                }
                                input->SetState(TExprNode::EState::Error);
                                return IGraphTransformer::TStatus::Error;
                            }

                            bool truncated = false;
                            auto yson = this->EncodeResultToYson(literalResult.Result, truncated);
                            if (truncated) {
                                input->SetState(TExprNode::EState::Error);
                                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "EvaluteExpr result is too big and was truncated"));
                                return IGraphTransformer::TStatus::Error;
                            }

                            output = input;
                            input->SetState(TExprNode::EState::ExecutionComplete);
                            input->SetResult(ctx.NewAtom(input->Pos(), yson));
                            return IGraphTransformer::TStatus::Ok;
                        });
                }));
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
        const NKikimrMiniKQL::TResult& resultValue, TExprContext& ctx)
    {
        TVector<TString> columnHints(NCommon::GetResOrPullColumnHints(node));

        auto protoValue = &resultValue;
        YQL_ENSURE(resultValue.GetArena());
        if (IsRawKikimrResult(resultValue)) {
            protoValue = KikimrResultToProto(resultValue, columnHints, fillSettings, resultValue.GetArena());
        }

        YQL_ENSURE(fillSettings.Format == IDataProvider::EResultFormat::Custom);
        YQL_ENSURE(fillSettings.FormatDetails == KikimrMkqlProtoFormat);

        TVector<char> buffer(protoValue->ByteSize());
        Y_PROTOBUF_SUPPRESS_NODISCARD protoValue->SerializeToArray(buffer.data(), buffer.size());
        return ctx.NewAtom(node.Pos(), TStringBuf(buffer.data(), buffer.size()));
    }

    std::pair<IGraphTransformer::TStatus, TAsyncTransformCallbackFuture> RunResOrPullForExec(TResOrPullBase res,
        TExprBase exec, TExprContext& ctx, ui32 resultIndex)
    {
        auto requireStatus = RequireChild(res.Ref(), 0);
        if (requireStatus.Level != TStatus::Ok) {
            return SyncStatus(requireStatus);
        }

        if (NCommon::HasResOrPullOption(res.Ref(), "ref")) {
            ctx.AddError(TIssue(ctx.GetPosition(res.Pos()), TStringBuilder()
                << "refselect isn't supported for Kikimr provider."));
            return SyncError();
        }

        IDataProvider::TFillSettings fillSettings = NCommon::GetFillSettings(res.Ref());

        if (IsIn({EKikimrQueryType::Query, EKikimrQueryType::Script}, SessionCtx->Query().Type)) {
            if (fillSettings.Discard) {
                ctx.AddError(YqlIssue(ctx.GetPosition(res.Pos()), TIssuesIds::KIKIMR_BAD_OPERATION, TStringBuilder()
                    << "DISCARD not supported in YDB queries"));
                return SyncError();
            }
        }

        auto* runResult = SessionCtx->Query().Results.FindPtr(exec.Ref().UniqueId());
        if (!runResult) {
            ctx.AddError(TIssue(ctx.GetPosition(exec.Pos()), TStringBuilder() << "KiExecute run result not found."));
            return SyncError();
        }

        if (SessionCtx->Query().PrepareOnly) {
            res.Ptr()->SetResult(ctx.NewWorld(res.Pos()));
            return SyncOk();
        }

        YQL_ENSURE(resultIndex < runResult->Results.size());
        auto resultValue = runResult->Results[resultIndex];
        YQL_ENSURE(resultValue);

        auto resResultNode = GetResOrPullResult(res.Ref(), fillSettings, *resultValue, ctx);

        res.Ptr()->SetResult(std::move(resResultNode));
        return SyncOk();
    }

private:
    TIntrusivePtr<IKikimrGateway> Gateway;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TTypeAnnotationContext& TypesCtx;
};

template <class TKiObject, class TSettings>
class TObjectModifierTransformer {
private:
    TIntrusivePtr<IKikimrGateway> Gateway;
    TString ActionInfo;
protected:
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    virtual TFuture<IKikimrGateway::TGenericResult> DoExecute(const TString& cluster, const TSettings& settings) = 0;
    TIntrusivePtr<IKikimrGateway> GetGateway() const {
        return Gateway;
    }
public:
    TObjectModifierTransformer(const TString& actionInfo, TIntrusivePtr<IKikimrGateway> gateway, TIntrusivePtr<TKikimrSessionContext> sessionCtx)
        : Gateway(gateway)
        , ActionInfo(actionInfo)
        , SessionCtx(sessionCtx)
    {

    }

    std::pair<IGraphTransformer::TStatus, TAsyncTransformCallbackFuture> Execute(const TKiObject& kiObject, const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureNotPrepare(ActionInfo + " " + kiObject.TypeId(), input->Pos(), SessionCtx->Query(), ctx)) {
            return SyncError();
        }

        auto requireStatus = RequireChild(*input, 0);
        if (requireStatus.Level != IGraphTransformer::TStatus::Ok) {
            return SyncStatus(requireStatus);
        }

        auto cluster = TString(kiObject.DataSink().Cluster());
        TSettings settings;
        if (!settings.DeserializeFromKi(kiObject)) {
            return SyncError();
        }
        bool prepareOnly = SessionCtx->Query().PrepareOnly;
        auto future = prepareOnly ? CreateDummySuccess() : DoExecute(cluster, settings);

        return WrapFuture(future,
            [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                Y_UNUSED(res);
                auto resultNode = ctx.NewWorld(input->Pos());
                return resultNode;
            }, "Executing " + ActionInfo + " " + kiObject.TypeId());
    }
};

class TUpsertObjectTransformer: public TObjectModifierTransformer<TKiUpsertObject, TUpsertObjectSettings> {
private:
    using TBase = TObjectModifierTransformer<TKiUpsertObject, TUpsertObjectSettings>;
protected:
    virtual TFuture<IKikimrGateway::TGenericResult> DoExecute(const TString& cluster, const TUpsertObjectSettings& settings) override {
        return GetGateway()->UpsertObject(cluster, settings);
    }
public:
    using TBase::TBase;
};

class TCreateObjectTransformer: public TObjectModifierTransformer<TKiCreateObject, TCreateObjectSettings> {
private:
    using TBase = TObjectModifierTransformer<TKiCreateObject, TCreateObjectSettings>;
protected:
    virtual TFuture<IKikimrGateway::TGenericResult> DoExecute(const TString& cluster, const TCreateObjectSettings& settings) override {
        return GetGateway()->CreateObject(cluster, settings);
    }
public:
    using TBase::TBase;
};

class TAlterObjectTransformer: public TObjectModifierTransformer<TKiAlterObject, TAlterObjectSettings> {
private:
    using TBase = TObjectModifierTransformer<TKiAlterObject, TAlterObjectSettings>;
protected:
    virtual TFuture<IKikimrGateway::TGenericResult> DoExecute(const TString& cluster, const TAlterObjectSettings& settings) override {
        return GetGateway()->AlterObject(cluster, settings);
    }
public:
    using TBase::TBase;
};

class TDropObjectTransformer: public TObjectModifierTransformer<TKiDropObject, TDropObjectSettings> {
private:
    using TBase = TObjectModifierTransformer<TKiDropObject, TDropObjectSettings>;
protected:
    virtual TFuture<IKikimrGateway::TGenericResult> DoExecute(const TString& cluster, const TDropObjectSettings& settings) override {
        return GetGateway()->DropObject(cluster, settings);
    }
public:
    using TBase::TBase;
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
            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            auto cluster = TString(maybeCreate.Cast().DataSink().Cluster());
            auto& table = SessionCtx->Tables().GetTable(cluster, TString(maybeCreate.Cast().Table()));

            auto tableTypeItem = table.Metadata->TableType;
            if (tableTypeItem == ETableType::ExternalTable && !SessionCtx->Config().FeatureFlags.GetEnableExternalDataSources()) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()),
                    TStringBuilder() << "External table are disabled. Please contact your system administrator to enable it"));
                return SyncError();
            }

            if ((tableTypeItem == ETableType::Unknown || tableTypeItem == ETableType::Table) && !ApplyDdlOperation(cluster, input->Pos(), table.Metadata->Name, TYdbOperation::CreateTable, ctx)) {
                return SyncError();
            }

            NThreading::TFuture<IKikimrGateway::TGenericResult> future;
            bool isColumn = (table.Metadata->StoreType == EStoreType::Column);
            bool existingOk = (maybeCreate.ExistingOk().Cast().Value() == "1");
            switch (tableTypeItem) {
                case ETableType::ExternalTable: {
                    future = Gateway->CreateExternalTable(cluster,
                        ParseCreateExternalTableSettings(maybeCreate.Cast(), table.Metadata->TableSettings), false);
                    break;
                }
                case ETableType::TableStore: {
                    if (!isColumn) {
                        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()),
                            TStringBuilder() << "TABLESTORE with not COLUMN store"));
                        return SyncError();
                    }
                    future = Gateway->CreateTableStore(cluster,
                        ParseCreateTableStoreSettings(maybeCreate.Cast(), table.Metadata->TableSettings));
                    break;
                }
                case ETableType::Table:
                case ETableType::Unknown: {
                    future = isColumn ? Gateway->CreateColumnTable(table.Metadata, true) : Gateway->CreateTable(table.Metadata, true, existingOk);
                    break;
                }
            }

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                    Y_UNUSED(res);
                    auto resultNode = ctx.NewWorld(input->Pos());
                    return resultNode;
                }, GetCreateTableDebugString(tableTypeItem));
        }

        if (auto maybeDrop = TMaybeNode<TKiDropTable>(input)) {
            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }
            auto cluster = TString(maybeDrop.Cast().DataSink().Cluster());
            TString tableName = TString(maybeDrop.Cast().Table());
            auto& table = SessionCtx->Tables().GetTable(cluster, tableName);
            auto tableTypeString = TString(maybeDrop.Cast().TableType());
            auto tableTypeItem = GetTableTypeFromString(tableTypeString);
            switch (tableTypeItem) {
                case ETableType::Table:
                    if (!ApplyDdlOperation(cluster, input->Pos(), tableName, TYdbOperation::DropTable, ctx)) {
                        return SyncError();
                    }
                    break;
                case ETableType::TableStore:
                case ETableType::ExternalTable:
                    break;
                case ETableType::Unknown:
                    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Unsupported table type " << tableTypeString));
                    return SyncError();
            }

            if (tableTypeItem == ETableType::ExternalTable && !SessionCtx->Config().FeatureFlags.GetEnableExternalDataSources()) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()),
                    TStringBuilder() << "External table are disabled. Please contact your system administrator to enable it"));
                return SyncError();
            }

            bool missingOk = (maybeDrop.MissingOk().Cast().Value() == "1");

            NThreading::TFuture<IKikimrGateway::TGenericResult> future;
            switch (tableTypeItem) {
                case ETableType::Table:
                    future = Gateway->DropTable(table.Metadata->Cluster, TDropTableSettings{.Table = table.Metadata->Name, .SuccessOnNotExist = missingOk});
                    if (missingOk) {
                        future = future.Apply([](const NThreading::TFuture<IKikimrGateway::TGenericResult>& res) {
                            auto operationResult = res.GetValue();
                            bool pathNotExist = false;
                            for (const auto& issue : operationResult.Issues()) {
                                WalkThroughIssues(issue, false, [&pathNotExist](const NYql::TIssue& issue, int level) {
                                    Y_UNUSED(level);
                                    pathNotExist |= (issue.GetCode() == NKikimrIssues::TIssuesIds::PATH_NOT_EXIST);
                                });
                            }
                            return pathNotExist ? CreateDummySuccess() : res;
                        });
                    }
                    break;
                case ETableType::TableStore:
                    future = Gateway->DropTableStore(cluster, ParseDropTableStoreSettings(maybeDrop.Cast()));
                    break;
                case ETableType::ExternalTable:
                    future = Gateway->DropExternalTable(cluster, ParseDropExternalTableSettings(maybeDrop.Cast()));
                    break;
                case ETableType::Unknown:
                    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Unsupported table type " << tableTypeString));
                    return SyncError();
            }

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                    Y_UNUSED(res);
                    auto resultNode = ctx.NewWorld(input->Pos());
                    return resultNode;
                }, GetDropTableDebugString(tableTypeItem));

            input->SetState(TExprNode::EState::ExecutionComplete);
            input->SetResult(ctx.NewWorld(input->Pos()));
            return SyncOk();
        }

        if (auto maybeAlter = TMaybeNode<TKiAlterTable>(input)) {
            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            auto cluster = TString(maybeAlter.Cast().DataSink().Cluster());
            auto& table = SessionCtx->Tables().GetTable(cluster, TString(maybeAlter.Cast().Table()));

            if (!ApplyDdlOperation(cluster, input->Pos(), table.Metadata->Name, TYdbOperation::AlterTable, ctx)) {
                return SyncError();
            }

            ui64 alterTableFlags = 0; //additional flags to pass options without public api modification
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

                        auto columnConstraints = columnTuple.Item(2).Cast<TCoNameValueTuple>();
                        for(const auto& constraint: columnConstraints.Value().Cast<TCoNameValueTupleList>()) {
                            if (constraint.Name().Value() == "serial") {
                                ctx.AddError(TIssue(ctx.GetPosition(constraint.Pos()), 
                                    "Column addition with serial data type is unsupported"));
                                return SyncError();
                            }
                        }

                        auto families = columnTuple.Item(3).Cast<TCoAtomList>();
                        if (families.Size() > 1) {
                            ctx.AddError(TIssue(ctx.GetPosition(families.Pos()),
                                "Unsupported number of families"));
                            return SyncError();
                        }

                        for (auto family : families) {
                            add_column->set_family(TString(family.Value()));
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
                        } else if (name == "setTiering") {
                            const auto tieringName = TString(
                                setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                            );
                            alterTableRequest.set_set_tiering(tieringName);
                        } else if (name == "resetTiering") {
                            alterTableRequest.mutable_drop_tiering();
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
                        } else if (name == "flags") {
                            auto flagList = columnTuple.Item(1).Cast<TCoAtomList>();
                            for (auto flag : flagList) {
                                TString flagName(flag.Value());
                                if (flagName == "pg") {
                                    alterTableFlags |= NKqpProto::TKqpSchemeOperation::FLAG_PG_MODE;
                                } else if (flagName == "ifNotExists") {
                                    alterTableFlags |= NKqpProto::TKqpSchemeOperation::FLAG_IF_NOT_EXISTS;
                                } else {
                                    ctx.AddError(TIssue(ctx.GetPosition(nameNode.Pos()),
                                        TStringBuilder() << "Unknown add index flag: " << flagName));
                                    return SyncError();
                                }
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
                                    } else if (to_lower(format) == "dynamodb_streams_json") {
                                        add_changefeed->set_format(Ydb::Table::ChangefeedFormat::FORMAT_DYNAMODB_STREAMS_JSON);
                                    } else if (to_lower(format) == "debezium_json") {
                                        add_changefeed->set_format(Ydb::Table::ChangefeedFormat::FORMAT_DEBEZIUM_JSON);
                                    } else {
                                        ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                                            TStringBuilder() << "Unknown changefeed format: " << format));
                                        return SyncError();
                                    }
                                } else if (name == "initial_scan") {
                                    auto value = TString(
                                        setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                                    );

                                    add_changefeed->set_initial_scan(FromString<bool>(to_lower(value)));
                                } else if (name == "virtual_timestamps") {
                                    auto value = TString(
                                        setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                                    );

                                    add_changefeed->set_virtual_timestamps(FromString<bool>(to_lower(value)));
                                } else if (name == "resolved_timestamps") {
                                    YQL_ENSURE(setting.Value().Maybe<TCoInterval>());
                                    const auto value = FromString<i64>(
                                        setting.Value().Cast<TCoInterval>().Literal().Value()
                                    );

                                    if (value <= 0) {
                                        ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                                            TStringBuilder() << name << " must be positive"));
                                        return SyncError();
                                    }

                                    const auto interval = TDuration::FromValue(value);
                                    auto& resolvedTimestamps = *add_changefeed->mutable_resolved_timestamps_interval();
                                    resolvedTimestamps.set_seconds(interval.Seconds());
                                } else if (name == "retention_period") {
                                    YQL_ENSURE(setting.Value().Maybe<TCoInterval>());
                                    const auto value = FromString<i64>(
                                        setting.Value().Cast<TCoInterval>().Literal().Value()
                                    );

                                    if (value <= 0) {
                                        ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                                            TStringBuilder() << name << " must be positive"));
                                        return SyncError();
                                    }

                                    const auto duration = TDuration::FromValue(value);
                                    auto& retention = *add_changefeed->mutable_retention_period();
                                    retention.set_seconds(duration.Seconds());
                                } else if (name == "topic_min_active_partitions") {
                                    auto value = TString(
                                        setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                                    );

                                    i64 minActivePartitions;
                                    if (!TryFromString(value, minActivePartitions) || minActivePartitions <= 0) {
                                        ctx.AddError(TIssue(ctx.GetPosition(setting.Name().Pos()),
                                            TStringBuilder() << name << " must be greater than 0"));
                                        return SyncError();
                                    }

                                    add_changefeed->mutable_topic_partitioning_settings()->set_min_active_partitions(minActivePartitions);
                                } else if (name == "aws_region") {
                                    auto value = TString(
                                        setting.Value().Cast<TCoDataCtor>().Literal().Cast<TCoAtom>().Value()
                                    );

                                    add_changefeed->set_aws_region(value);
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
                } else if (name == "renameIndexTo") {
                    auto listNode = action.Value().Cast<TExprList>();
                    auto renameIndexes = alterTableRequest.add_rename_indexes();
                    for (size_t i = 0; i < listNode.Size(); ++i) {
                        auto item = listNode.Item(i);
                        auto columnTuple = item.Cast<TExprList>();
                        auto nameNode = columnTuple.Item(0).Cast<TCoAtom>();
                        auto name = TString(nameNode.Value());
                        if (name == "src") {
                            renameIndexes->set_source_name(columnTuple.Item(1).Cast<TCoAtom>().StringValue());
                        } else if (name == "dst") {
                            renameIndexes->set_destination_name(columnTuple.Item(1).Cast<TCoAtom>().StringValue());
                        } else {
                            ctx.AddError(TIssue(ctx.GetPosition(action.Name().Pos()),
                                TStringBuilder() << "Unknown renameIndexTo param: " << name));
                            return SyncError();
                        }
                    }
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(action.Name().Pos()),
                        TStringBuilder() << "Unknown alter table action: " << name));
                    return SyncError();
                }
            }

            NThreading::TFuture<IKikimrGateway::TGenericResult> future;
            bool isTableStore = (table.Metadata->TableType == ETableType::TableStore);
            bool isColumn = (table.Metadata->StoreType == EStoreType::Column);

            if (isTableStore) {
                if (!isColumn) {
                    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()),
                        TStringBuilder() << "TABLESTORE with not COLUMN store"));
                    return SyncError();
                }
                future = Gateway->AlterTableStore(cluster, ParseAlterTableStoreSettings(maybeAlter.Cast()));
            } else if (isColumn) {
                future = Gateway->AlterColumnTable(cluster, ParseAlterColumnTableSettings(maybeAlter.Cast()));
            } else {
                TMaybe<TString> requestType;
                if (!SessionCtx->Query().DocumentApiRestricted) {
                    requestType = NKikimr::NDocApi::RequestType;
                }
                future = Gateway->AlterTable(cluster, std::move(alterTableRequest), requestType, alterTableFlags);
            }

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                    Y_UNUSED(res);
                    auto resultNode = ctx.NewWorld(input->Pos());
                    return resultNode;
                });

        }

        if (auto maybeCreate = TMaybeNode<TKiCreateTopic>(input)) {
            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }
            auto cluster = TString(maybeCreate.Cast().DataSink().Cluster());
            TString topicName = TString(maybeCreate.Cast().Topic());
            Ydb::Topic::CreateTopicRequest createReq;
            createReq.set_path(topicName);
            for (const auto& consumer : maybeCreate.Cast().Consumers()) {
                auto error = AddConsumerToTopicRequest(createReq.add_consumers(), consumer);
                if (!error.empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << error << input->Content()));
                    return SyncError();
                }
            }
            AddTopicSettingsToRequest(&createReq,maybeCreate.Cast().TopicSettings());
            bool prepareOnly = SessionCtx->Query().PrepareOnly;
            // DEBUG
            // Cerr << "Create topic request proto: " << createReq.DebugString() << Endl;
            auto future = prepareOnly ? CreateDummySuccess() : (
                    Gateway->CreateTopic(cluster, std::move(createReq))
            );

            return WrapFuture(future,
                              [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                                  Y_UNUSED(res);
                                  auto resultNode = ctx.NewWorld(input->Pos());
                                  return resultNode;
                              }, "Executing CREATE TOPIC");
        }
        if (auto maybeAlter = TMaybeNode<TKiAlterTopic>(input)) {
            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }
            auto cluster = TString(maybeAlter.Cast().DataSink().Cluster());
            TString topicName = TString(maybeAlter.Cast().Topic());
            Ydb::Topic::AlterTopicRequest alterReq;
            alterReq.set_path(topicName);
            for (const auto& consumer : maybeAlter.Cast().AddConsumers()) {
                auto error = AddConsumerToTopicRequest(alterReq.add_add_consumers(), consumer);
                if (!error.empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << error << input->Content()));
                    return SyncError();
                }
            }
            for (const auto& consumer : maybeAlter.Cast().AlterConsumers()) {
                auto error = AddAlterConsumerToTopicRequest(alterReq.add_alter_consumers(), consumer);
                if (!error.empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << error << input->Content()));
                    return SyncError();
                }
            }
            for (const auto& consumer : maybeAlter.Cast().DropConsumers()) {
                auto name = consumer.Cast<TCoAtom>().StringValue();
                alterReq.add_drop_consumers(name);
            }
            AddAlterTopicSettingsToRequest(&alterReq, maybeAlter.Cast().TopicSettings());
            bool prepareOnly = SessionCtx->Query().PrepareOnly;
	    // DEBUG
            // Cerr << "Alter topic request proto:\n" << alterReq.DebugString() << Endl;
            auto future = prepareOnly ? CreateDummySuccess() : (
                    Gateway->AlterTopic(cluster, std::move(alterReq))
            );

            return WrapFuture(future,
                              [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                                  Y_UNUSED(res);
                                  auto resultNode = ctx.NewWorld(input->Pos());
                                  return resultNode;
                              }, "Executing ALTER TOPIC");
        }

        if (auto maybeDrop = TMaybeNode<TKiDropTopic>(input)) {
            if (!EnsureNotPrepare("DROP TOPIC", input->Pos(), SessionCtx->Query(), ctx)) {
                return SyncError();
            }

            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }
            auto cluster = TString(maybeDrop.Cast().DataSink().Cluster());
            TString topicName = TString(maybeDrop.Cast().Topic());

            bool prepareOnly = SessionCtx->Query().PrepareOnly;
            auto future = prepareOnly ? CreateDummySuccess() : (
                    Gateway->DropTopic(cluster, topicName)
            );

            return WrapFuture(future,
                              [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                                  Y_UNUSED(res);
                                  auto resultNode = ctx.NewWorld(input->Pos());
                                  return resultNode;
                              }, "Executing DROP TOPIC");
        }

        if (auto maybeGrantPermissions = TMaybeNode<TKiModifyPermissions>(input)) {
            if (!EnsureNotPrepare("MODIFY PERMISSIONS", input->Pos(), SessionCtx->Query(), ctx)) {
                return SyncError();
            }

            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }

            auto cluster = TString(maybeGrantPermissions.Cast().DataSink().Cluster());
            TModifyPermissionsSettings settings = ParsePermissionsSettings(maybeGrantPermissions.Cast());

            bool prepareOnly = SessionCtx->Query().PrepareOnly;
            auto future = prepareOnly ? CreateDummySuccess() : Gateway->ModifyPermissions(cluster, settings);

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                Y_UNUSED(res);
                auto resultNode = ctx.NewWorld(input->Pos());
                return resultNode;
            }, "Executing MODIFY PERMISSIONS");
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

            bool prepareOnly = SessionCtx->Query().PrepareOnly;
            auto future = prepareOnly ? CreateDummySuccess() : Gateway->CreateUser(cluster, createUserSettings);

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

            bool prepareOnly = SessionCtx->Query().PrepareOnly;
            auto future = prepareOnly ? CreateDummySuccess() : Gateway->AlterUser(cluster, alterUserSettings);

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

            bool prepareOnly = SessionCtx->Query().PrepareOnly;
            auto future = prepareOnly ? CreateDummySuccess() : Gateway->DropUser(cluster, dropUserSettings);

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                Y_UNUSED(res);
                auto resultNode = ctx.NewWorld(input->Pos());
                return resultNode;
            }, "Executing DROP USER");
        }

        if (auto kiObject = TMaybeNode<TKiUpsertObject>(input)) {
            return TUpsertObjectTransformer("UPSERT OBJECT", Gateway, SessionCtx).Execute(kiObject.Cast(), input, ctx);
        }

        if (auto kiObject = TMaybeNode<TKiCreateObject>(input)) {
            return TCreateObjectTransformer("CREATE OBJECT", Gateway, SessionCtx).Execute(kiObject.Cast(), input, ctx);
        }

        if (auto kiObject = TMaybeNode<TKiAlterObject>(input)) {
            return TAlterObjectTransformer("ALTER OBJECT", Gateway, SessionCtx).Execute(kiObject.Cast(), input, ctx);
        }

        if (auto kiObject = TMaybeNode<TKiDropObject>(input)) {
            return TDropObjectTransformer("DROP OBJECT", Gateway, SessionCtx).Execute(kiObject.Cast(), input, ctx);
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

            bool prepareOnly = SessionCtx->Query().PrepareOnly;
            auto future = prepareOnly ? CreateDummySuccess() : Gateway->CreateGroup(cluster, createGroupSettings);

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

            bool prepareOnly = SessionCtx->Query().PrepareOnly;
            auto future = prepareOnly ? CreateDummySuccess() : Gateway->AlterGroup(cluster, alterGroupSettings);

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

            bool prepareOnly = SessionCtx->Query().PrepareOnly;
            auto future = prepareOnly ? CreateDummySuccess() : Gateway->DropGroup(cluster, dropGroupSettings);

            return WrapFuture(future,
                [](const IKikimrGateway::TGenericResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                Y_UNUSED(res);
                auto resultNode = ctx.NewWorld(input->Pos());
                return resultNode;
            }, "Executing DROP GROUP");
        }

        if (auto maybePgDropObject = TMaybeNode<TPgDropObject>(input)) {
            auto requireStatus = RequireChild(*input, 0);
            if (requireStatus.Level != TStatus::Ok) {
                return SyncStatus(requireStatus);
            }
            auto pgDrop = maybePgDropObject.Cast();

            auto cluster = TString(pgDrop.DataSink().Cluster());
            const auto type = TString(pgDrop.TypeId().Value());
            const auto objectName = TString(pgDrop.ObjectId().Value());

            if (type == "pgIndex") {
                // TODO: KIKIMR-19695
                ctx.AddError(TIssue(ctx.GetPosition(pgDrop.Pos()),
                                    TStringBuilder() << "DROP INDEX for Postgres indexes is not implemented yet"));
                return SyncError();
            } else {
                ctx.AddError(TIssue(ctx.GetPosition(pgDrop.TypeId().Pos()),
                                    TStringBuilder() << "Unknown PgDrop operation: " << type));
                return SyncError();
            }
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

    static TString GetCreateTableDebugString(ETableType tableType) {
        switch (tableType) {
            case ETableType::ExternalTable:
                return "Executing CREATE EXTERNAL TABLE";
            case ETableType::TableStore:
                return "Executing CREATE TABLESTORE";
            case ETableType::Table:
            case ETableType::Unknown:
                return "Executing CREATE TABLE";
        }
    }

    static TString GetDropTableDebugString(ETableType tableType) {
        switch (tableType) {
            case ETableType::ExternalTable:
                return "Executing DROP EXTERNAL TABLE";
            case ETableType::TableStore:
                return "Executing DROP TABLESTORE";
            case ETableType::Table:
            case ETableType::Unknown:
                return "Executing DROP TABLE";
        }
    }

    std::pair<IGraphTransformer::TStatus, TAsyncTransformCallbackFuture> PerformExecution(TExprBase node,
        TExprContext& ctx, const TString& cluster, TMaybe<TString> mode,
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

            const auto& scanQuery = config->ScanQuery.Get(cluster);
            if (scanQuery) {
                settings.UseScanQuery = scanQuery.GetRef();
            }

            settings.StatsMode = SessionCtx->Query().StatsMode;

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
                finalizeFunc(node, insertResult.first->second, ctx);
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
            return QueryExecutor->ExecuteDataQuery(cluster, execQuery.QueryBlocks().Ptr(), ctx, settings);
        };

        auto finalizeFunc = [] (TExprBase node, const IKikimrQueryExecutor::TQueryResult& result, TExprContext& ctx) {
            FillExecDataQueryAst(node.Cast<TKiExecDataQuery>(), result.QueryAst, ctx);
        };

        return PerformExecution(execQuery, ctx, cluster, settings.Mode, runFunc, finalizeFunc);
    }

    std::pair<bool, TIssues> ApplyTableOperations(const TString& cluster, const TVector<NKqpProto::TKqpTableOp>& tableOps)
    {
        auto queryType = SessionCtx->Query().Type;
        TVector<NKqpProto::TKqpTableInfo> tableInfo;

        for (const auto& op : tableOps) {
            auto table = op.GetTable();
            auto operation = static_cast<TYdbOperation>(op.GetOperation());
            const auto& desc = SessionCtx->Tables().GetTable(cluster, table);
            YQL_ENSURE(desc.Metadata);
            TableDescriptionToTableInfo(desc, operation, tableInfo);
        }

        if (!SessionCtx->HasTx()) {
            TKikimrTransactionContextBase emptyCtx(SessionCtx->Config().EnableKqpImmediateEffects);
            emptyCtx.SetTempTables(SessionCtx->GetTempTablesState());
            return emptyCtx.ApplyTableOperations(tableOps, tableInfo, queryType);
        }

        return SessionCtx->Tx().ApplyTableOperations(tableOps, tableInfo, queryType);
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

        auto [success, issues] = ApplyTableOperations(cluster, {protoOp});
        for (auto& i : issues) {
            ctx.AddError(std::move(i));
        }
        return success;
    }

private:
    TIntrusivePtr<IKikimrGateway> Gateway;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TIntrusivePtr<IKikimrQueryExecutor> QueryExecutor;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateKiSourceCallableExecutionTransformer(
    TIntrusivePtr<IKikimrGateway> gateway,
    TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    TTypeAnnotationContext& types)
{
    return new TKiSourceCallableExecutionTransformer(gateway, sessionCtx, types);
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
