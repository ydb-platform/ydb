#include "read_attributes_utils.h"
#include "rewrite_io_utils.h"
#include "yql_kikimr_provider_impl.h"

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/host/kqp_translate.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/config/yql_configuration_transformer.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <ydb/core/external_sources/external_source_factory.h>
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>

#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <util/generic/is_in.h>

namespace NYql {

TExprNode::TPtr BuildExternalTableSettings(TPositionHandle pos, TExprContext& ctx, const TMap<TString, NYql::TKikimrColumnMetadata>& columns, const NKikimr::NExternalSource::IExternalSource::TPtr& source, const TString& content) {
    TExprNode::TListType items;
    items.emplace_back(BuildSchemaFromMetadata(pos, ctx, columns));

    for (const auto& [key, values]: source->GetParameters(content)) {
        TExprNode::TListType children = {ctx.NewAtom(pos, NormalizeName(key))};
        children.reserve(values.size() + 1);
        for (const TString& value : values) {
            children.emplace_back(ctx.NewAtom(pos, value));
        }

        items.emplace_back(ctx.NewList(pos, std::move(children)));
    }

    return ctx.NewList(pos, std::move(items));
}

TString FillAuthProperties(THashMap<TString, TString>& properties, const TExternalSource& externalSource) {
    switch (externalSource.DataSourceAuth.identity_case()) {
        case NKikimrSchemeOp::TAuth::kServiceAccount:
            properties["authMethod"] = "SERVICE_ACCOUNT";
            properties["serviceAccountId"] = externalSource.DataSourceAuth.GetServiceAccount().GetId();
            properties["serviceAccountIdSignature"] = externalSource.ServiceAccountIdSignature;
            properties["serviceAccountIdSignatureReference"] = externalSource.DataSourceAuth.GetServiceAccount().GetSecretName();
            return {};

        case NKikimrSchemeOp::TAuth::kNone:
            properties["authMethod"] = "NONE";
            return {};

        case NKikimrSchemeOp::TAuth::kBasic:
            properties["authMethod"] = "BASIC";
            properties["login"] = externalSource.DataSourceAuth.GetBasic().GetLogin();
            properties["password"] = externalSource.Password;
            properties["passwordReference"] = externalSource.DataSourceAuth.GetBasic().GetPasswordSecretName();
            return {};

        case NKikimrSchemeOp::TAuth::kMdbBasic:
            properties["authMethod"] = "MDB_BASIC";
            properties["serviceAccountId"] = externalSource.DataSourceAuth.GetMdbBasic().GetServiceAccountId();
            properties["serviceAccountIdSignature"] = externalSource.ServiceAccountIdSignature;
            properties["serviceAccountIdSignatureReference"] = externalSource.DataSourceAuth.GetMdbBasic().GetServiceAccountSecretName();

            properties["login"] = externalSource.DataSourceAuth.GetMdbBasic().GetLogin();
            properties["password"] = externalSource.Password;
            properties["passwordReference"] = externalSource.DataSourceAuth.GetMdbBasic().GetPasswordSecretName();
            return {};

        case NKikimrSchemeOp::TAuth::kAws:
            properties["authMethod"] = "AWS";
            properties["awsAccessKeyId"] = externalSource.AwsAccessKeyId;
            properties["awsAccessKeyIdReference"] = externalSource.DataSourceAuth.GetAws().GetAwsAccessKeyIdSecretName();
            properties["awsSecretAccessKey"] = externalSource.AwsSecretAccessKey;
            properties["awsSecretAccessKeyReference"] = externalSource.DataSourceAuth.GetAws().GetAwsSecretAccessKeySecretName();
            properties["awsRegion"] = externalSource.DataSourceAuth.GetAws().GetAwsRegion();
            return {};

        case NKikimrSchemeOp::TAuth::kToken:
            properties["authMethod"] = "TOKEN";
            properties["token"] = externalSource.Token;
            properties["tokenReference"] = externalSource.DataSourceAuth.GetToken().GetTokenSecretName();
            return {};

        case NKikimrSchemeOp::TAuth::IDENTITY_NOT_SET:
            return {"Identity case is not specified"};
    }
}

namespace {

using namespace NKikimr;
using namespace NNodes;

class TKiSourceIntentDeterminationTransformer: public TKiSourceVisitorTransformer {
public:
    TKiSourceIntentDeterminationTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx)
        : SessionCtx(sessionCtx) {}

private:
    TStatus HandleKiRead(TKiReadBase node, TExprContext& ctx) override {
        auto cluster = node.DataSource().Cluster();
        TKikimrKey key(ctx);
        if (!key.Extract(node.TableKey().Ref())) {
            return TStatus::Error;
        }

        return HandleKey(cluster, key);
    }

    TStatus HandleRead(TExprBase node, TExprContext& ctx) override {
        auto cluster = node.Ref().Child(1)->Child(1)->Content();
        TKikimrKey key(ctx);
        if (!key.Extract(*node.Ref().Child(2))) {
            return TStatus::Error;
        }

        return HandleKey(cluster, key);
    }

    TStatus HandleLength(TExprBase node, TExprContext& ctx) override {
        Y_UNUSED(node);
        Y_UNUSED(ctx);
        return TStatus::Ok;
    }

    TStatus HandleConfigure(TExprBase node, TExprContext& ctx) override {
        Y_UNUSED(node);
        Y_UNUSED(ctx);
        return TStatus::Ok;
    }

private:
    TStatus HandleKey(const TStringBuf& cluster, const TKikimrKey& key) {
        switch (key.GetKeyType()) {
            case TKikimrKey::Type::Table:
            case TKikimrKey::Type::TableScheme: {
                auto& table = SessionCtx->Tables().GetOrAddTable(TString(cluster), SessionCtx->GetDatabase(),
                    key.GetTablePath());

                if (key.GetKeyType() == TKikimrKey::Type::TableScheme) {
                    table.RequireStats();
                }

                return TStatus::Ok;
            }

            case TKikimrKey::Type::TableList:
                return TStatus::Ok;

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

        return TStatus::Error;
    }

private:
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
};

class TKiSourceLoadTableMetadataTransformer : public TGraphTransformerBase {
public:
    TKiSourceLoadTableMetadataTransformer(
        TIntrusivePtr<IKikimrGateway> gateway,
        TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        TTypeAnnotationContext& types,
        const NExternalSource::IExternalSourceFactory::TPtr& externalSourceFactory,
        bool isInternalCall)
        : Gateway(gateway)
        , SessionCtx(sessionCtx)
        , Types(types)
        , ExternalSourceFactory(externalSourceFactory)
        , IsInternalCall(isInternalCall)
        {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        if (ctx.Step.IsDone(TExprStep::LoadTablesMetadata)) {
            return TStatus::Ok;
        }

        size_t tablesCount = SessionCtx->Tables().GetTables().size();
        TVector<NThreading::TFuture<void>> futures;
        futures.reserve(tablesCount);
        std::optional<THashMap<std::pair<TString, TString>, THashMap<TString, TString>>> readAttributes;

        for (auto& it : SessionCtx->Tables().GetTables()) {
            const TString& clusterName = it.first.first;
            const TString& tableName = it.first.second;
            TKikimrTableDescription& table = SessionCtx->Tables().GetTable(clusterName, tableName);

            if (table.Metadata || table.GetTableType() != ETableType::Table) {
                continue;
            }

            const THashMap<TString, TString>* readAttrs = nullptr;
            if (!table.Metadata && clusterName != NKqp::DefaultKikimrPublicClusterName) {
                if (!readAttributes) {
                    readAttributes = GatherReadAttributes(*input, ctx);
                }
                readAttrs = readAttributes->FindPtr(std::make_pair(clusterName, tableName));
            }

            auto emplaceResult = LoadResults.emplace(std::make_pair(clusterName, tableName),
                std::make_shared<IKikimrGateway::TTableMetadataResult>());

            YQL_ENSURE(emplaceResult.second);
            auto queryType = SessionCtx->Query().Type;
            auto& result = emplaceResult.first->second;

            auto future = Gateway->LoadTableMetadata(clusterName, tableName,
                IKikimrGateway::TLoadTableMetadataSettings()
                            .WithTableStats(table.GetNeedsStats())
                            .WithPrivateTables(IsInternalCall)
                            .WithExternalDatasources(SessionCtx->Config().FeatureFlags.GetEnableExternalDataSources())
                            .WithAuthInfo(table.GetNeedAuthInfo())
                            .WithExternalSourceFactory(ExternalSourceFactory)
                            .WithReadAttributes(readAttrs ? std::move(*readAttrs) : THashMap<TString, TString>{})
            );

            futures.push_back(future.Apply([result, queryType]
                (const NThreading::TFuture<IKikimrGateway::TTableMetadataResult>& future) {
                    YQL_ENSURE(!future.HasException());
                    const auto& value = future.GetValue();
                    switch (queryType) {
                        case EKikimrQueryType::Unspecified: {
                            if (value.Metadata) {
                                if (!value.Metadata->Indexes.empty()) {
                                    result->AddIssue(TIssue({}, TStringBuilder()
                                        << "Using index tables unsupported for legacy or unspecified request type"));
                                    result->SetStatus(TIssuesIds::KIKIMR_INDEX_METADATA_LOAD_FAILED);
                                    return;
                                }
                            }
                        }
                        break;
                    default:
                        break;
                    }
                    *result = value;
                }));
        }

        if (futures.empty()) {
            return TStatus::Ok;
        }

        AsyncFuture = NThreading::WaitExceptionOrAll(futures);
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        Y_UNUSED(input);
        return AsyncFuture;
    }

    bool AddCluster(const std::pair<TString, TString>& table, IKikimrGateway::TTableMetadataResult& res, TExprNode::TPtr input, TExprContext& ctx) {
        const auto& metadata = *res.Metadata;
        if (metadata.Kind != EKikimrTableKind::External) {
            return true;
        }
        auto source = ExternalSourceFactory->GetOrCreate(metadata.ExternalSource.Type);
        auto it = Types.DataSourceMap.find(source->GetName());
        if (it == Types.DataSourceMap.end()) {
            TIssueScopeGuard issueScope(ctx.IssueManager, [input, &table, &ctx]() {
                return MakeIntrusive<TIssue>(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                    << "Failed to load metadata for table (data source doesn't exist): "
                    << NCommon::FullTableName(table.first, table.second)));
            });

            res.ReportIssues(ctx.IssueManager);
            LoadResults.clear();
            return false;
        }

        THashMap<TString, TString> properties = {{
            {"location", metadata.ExternalSource.DataSourceLocation },
            {"installation", metadata.ExternalSource.DataSourceInstallation },
            {"source_type", metadata.ExternalSource.Type}
        }};

        properties.insert(metadata.ExternalSource.Properties.GetProperties().begin(), metadata.ExternalSource.Properties.GetProperties().end());

        const auto error = FillAuthProperties(properties, metadata.ExternalSource);
        if (error) {
            res.AddIssue(TIssue(error));
            return false;
        }

        it->second->AddCluster(metadata.ExternalSource.DataSourcePath, properties);

        return true;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        YQL_ENSURE(AsyncFuture.HasValue());

        auto gatheredAttributes = GatherReadAttributes(*input, ctx);
        for (auto& it : LoadResults) {
            const auto& table = it.first;
            IKikimrGateway::TTableMetadataResult& res = *it.second;

            if (res.Success()) {
                res.ReportIssues(ctx.IssueManager);
                TString cluster = it.first.first;
                TString tablePath;
                if (res.Metadata->Temporary) {
                    tablePath = *res.Metadata->QueryName;
                } else {
                    tablePath = it.first.second;
                }
                TKikimrTableDescription* tableDesc = &SessionCtx->Tables().GetTable(cluster, tablePath);

                YQL_ENSURE(res.Metadata);
                tableDesc->Metadata = res.Metadata;

                bool sysColumnsEnabled = SessionCtx->Config().SystemColumnsEnabled();
                YQL_ENSURE(res.Metadata->Indexes.size() == res.Metadata->SecondaryGlobalIndexMetadata.size());
                for (const auto& indexMeta : res.Metadata->SecondaryGlobalIndexMetadata) {
                    YQL_ENSURE(indexMeta);
                    auto& desc = SessionCtx->Tables().GetOrAddTable(indexMeta->Cluster, SessionCtx->GetDatabase(), indexMeta->Name);
                    desc.Metadata = indexMeta;
                    desc.Load(ctx, sysColumnsEnabled);
                }

                if (!tableDesc->Load(ctx, sysColumnsEnabled)) {
                    LoadResults.clear();
                    return TStatus::Error;
                }

                if (tableDesc->Metadata->Kind == EKikimrTableKind::External) {
                    auto currentAttributes = gatheredAttributes.FindPtr(std::make_pair(cluster, tablePath));
                    if (currentAttributes && !currentAttributes->empty()) {
                        ReplaceReadAttributes(*input, *currentAttributes, cluster, tablePath, tableDesc->Metadata, ctx);
                    }
                }

                if (!AddCluster(table, res, input, ctx)) {
                    return TStatus::Error;
                }

                if (const auto& preparingQuery = SessionCtx->Query().PreparingQuery;
                        preparingQuery
                        && res.Metadata->Kind == EKikimrTableKind::View
                ) {
                    const auto& viewMetadata = *res.Metadata;
                    auto* viewInfo = preparingQuery->MutablePhysicalQuery()->MutableViewInfos()->Add();
                    auto* pathId = viewInfo->MutableTableId();
                    pathId->SetOwnerId(viewMetadata.PathId.OwnerId());
                    pathId->SetTableId(viewMetadata.PathId.TableId());
                    viewInfo->SetSchemaVersion(viewMetadata.SchemaVersion);
                }
            } else {
                TIssueScopeGuard issueScope(ctx.IssueManager, [input, &table, &ctx]() {
                    return MakeIntrusive<TIssue>(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                        << "Failed to load metadata for table: "
                        << NCommon::FullTableName(table.first, table.second)));
                });

                res.ReportIssues(ctx.IssueManager);
                LoadResults.clear();
                return TStatus::Error;
            }
        }
        output = input;

        LoadResults.clear();
        return TStatus::Ok;
    }

    void Rewind() final {
        LoadResults.clear();
        AsyncFuture = {};
    }

private:
    TIntrusivePtr<IKikimrGateway> Gateway;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TTypeAnnotationContext& Types;
    NExternalSource::IExternalSourceFactory::TPtr ExternalSourceFactory;
    const bool IsInternalCall;

    THashMap<std::pair<TString, TString>, std::shared_ptr<IKikimrGateway::TTableMetadataResult>> LoadResults;
    NThreading::TFuture<void> AsyncFuture;
};

class TKikimrConfigurationTransformer : public NCommon::TProviderConfigurationTransformer {
public:
    TKikimrConfigurationTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        const TTypeAnnotationContext& types)
        : TProviderConfigurationTransformer(sessionCtx->ConfigPtr(), types, TString(KikimrProviderName))
        , SessionCtx(sessionCtx) {}

protected:
    const THashSet<TStringBuf> AllowedScriptingPragmas = {
        "scanquery"
    };

    bool HandleAttr(TPositionHandle pos, const TString& cluster, const TString& name, const TMaybe<TString>& value,
        TExprContext& ctx) final
    {
        YQL_ENSURE(SessionCtx->Query().Type != EKikimrQueryType::Unspecified);

        if (!Dispatcher->Dispatch(cluster, name, value, NCommon::TSettingDispatcher::EStage::STATIC, NCommon::TSettingDispatcher::GetErrorCallback(pos, ctx))) {
            return false;
        }

        if (Dispatcher->IsRuntime(name)) {
            bool pragmaAllowed = false;

            switch (SessionCtx->Query().Type) {
                case EKikimrQueryType::YqlInternal:
                    pragmaAllowed = true;
                    break;

                case EKikimrQueryType::YqlScript:
                case EKikimrQueryType::YqlScriptStreaming:
                    pragmaAllowed = AllowedScriptingPragmas.contains(name);
                    break;

                default:
                    break;
            }

            if (!pragmaAllowed) {
                ctx.AddError(YqlIssue(ctx.GetPosition(pos), TIssuesIds::KIKIMR_PRAGMA_NOT_SUPPORTED, TStringBuilder()
                    << "Pragma can't be set for YDB query in current execution mode: " << name));
                return false;
            }
        }

        return true;
    }

    bool HandleAuth(TPositionHandle pos, const TString& cluster, const TString& alias, TExprContext& ctx) final {
        YQL_ENSURE(SessionCtx->Query().Type != EKikimrQueryType::Unspecified);

        if (SessionCtx->Query().Type != EKikimrQueryType::YqlInternal) {
            ctx.AddError(YqlIssue(ctx.GetPosition(pos), TIssuesIds::KIKIMR_PRAGMA_NOT_SUPPORTED, TStringBuilder()
                << "Pragma auth not supported inside Kikimr query."));
            return false;
        }

        return TProviderConfigurationTransformer::HandleAuth(pos, cluster, alias, ctx);
    }

private:
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
};

class TKikimrDataSource : public TDataProviderBase {
public:
    TKikimrDataSource(
        const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
        TTypeAnnotationContext& types,
        TIntrusivePtr<IKikimrGateway> gateway,
        TIntrusivePtr<TKikimrSessionContext> sessionCtx,
        const NExternalSource::IExternalSourceFactory::TPtr& externalSourceFactory,
        bool isInternalCall,
        TGUCSettings::TPtr gucSettings)
        : FunctionRegistry(functionRegistry)
        , Types(types)
        , Gateway(gateway)
        , SessionCtx(sessionCtx)
        , ExternalSourceFactory(externalSourceFactory)
        , GUCSettings(gucSettings)
        , ConfigurationTransformer(new TKikimrConfigurationTransformer(sessionCtx, types))
        , IntentDeterminationTransformer(new TKiSourceIntentDeterminationTransformer(sessionCtx))
        , LoadTableMetadataTransformer(CreateKiSourceLoadTableMetadataTransformer(gateway, sessionCtx, types, externalSourceFactory, isInternalCall))
        , TypeAnnotationTransformer(CreateKiSourceTypeAnnotationTransformer(sessionCtx, types))
        , CallableExecutionTransformer(CreateKiSourceCallableExecutionTransformer(gateway, sessionCtx, types))

    {
        Y_UNUSED(FunctionRegistry);
        Y_UNUSED(Types);

        YQL_ENSURE(gateway);
        YQL_ENSURE(sessionCtx);
    }

    ~TKikimrDataSource() {}

    TStringBuf GetName() const override {
        return KikimrProviderName;
    }

    bool Initialize(TExprContext& ctx) override {
        TString defaultToken;
        if (auto credential = Types.Credentials->FindCredential(TString("default_") + KikimrProviderName)) {
            if (credential->Category != KikimrProviderName) {
                ctx.AddError(TIssue({}, TStringBuilder()
                    << "Mismatch default credential category, expected: " << KikimrProviderName
                    << ", but found: " << credential->Category));
                return false;
            }

            defaultToken = credential->Content;
        }

        if (defaultToken.empty()) {
            if (!Types.Credentials->GetUserCredentials().OauthToken.empty()) {
                defaultToken = Types.Credentials->GetUserCredentials().OauthToken;
            }
        }

        for (auto& cluster : Gateway->GetClusters()) {
            auto token = defaultToken;

            if (auto credential = Types.Credentials->FindCredential(TString("default_") + cluster)) {
                if (credential->Category != KikimrProviderName) {
                    ctx.AddError(TIssue({}, TStringBuilder()
                        << "Mismatch credential category, for cluster " << cluster
                        << " expected: " << KikimrProviderName
                        << ", but found: " << credential->Category));
                    return false;
                }

                token = credential->Content;
            }

            TIntrusiveConstPtr<NACLib::TUserToken> tokenPtr = new NACLib::TUserToken(token);
            if (!token.empty()) {
                Gateway->SetToken(cluster, tokenPtr);
            }
        }

        return true;
    }

    IGraphTransformer& GetConfigurationTransformer() override {
        return *ConfigurationTransformer;
    }

    IGraphTransformer& GetIntentDeterminationTransformer() override {
        return *IntentDeterminationTransformer;
    }

    IGraphTransformer& GetLoadTableMetadataTransformer() override {
        return *LoadTableMetadataTransformer;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotationTransformer;
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *CallableExecutionTransformer;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSource::CallableName())) {
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

        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Kikimr DataSource parameters."));
        return false;
    }

    bool CanParse(const TExprNode& node) override {
        if (node.IsCallable(ReadName)) {
            return node.Child(1)->Child(0)->Content() == KikimrProviderName;
        }

        if (node.IsCallable(TKiReadTable::CallableName()) ||
            node.IsCallable(TKiReadTableScheme::CallableName()) ||
            node.IsCallable(TKiReadTableList::CallableName()))
        {
            return TKiDataSource(node.ChildPtr(1)).Category() == KikimrProviderName;
        }

        if (
            IsIn({EKikimrQueryType::Query, EKikimrQueryType::Script}, SessionCtx->Query().Type)
            &&
            (
                node.IsCallable(TDqSourceWrap::CallableName()) ||
                node.IsCallable(TDqSourceWideWrap::CallableName()) ||
                node.IsCallable(TDqSourceWideBlockWrap::CallableName()) ||
                node.IsCallable(TDqReadWrap::CallableName()) ||
                node.IsCallable(TDqReadWideWrap::CallableName()) ||
                node.IsCallable(TDqSource::CallableName())
            )
        )
        {
            return true;
        }

        YQL_ENSURE(!KikimrDataSourceFunctions().contains(node.Content()));
        return false;
    }

    bool IsPersistent(const TExprNode& node) override {
        if (node.IsCallable(ReadName)) {
            return node.Child(1)->Child(0)->Content() == KikimrProviderName;
        }

        if (node.IsCallable(TKiReadTable::CallableName())) {
            return TKiDataSource(node.ChildPtr(1)).Category() == KikimrProviderName;
        }

        return false;
    }

    bool CanPullResult(const TExprNode& node, TSyncMap& syncList, bool& canRef) override {
        Y_UNUSED(syncList);
        canRef = false;

        if (node.IsCallable(TCoRight::CallableName())) {
            const auto input = node.Child(0);
            if (input->IsCallable(TKiReadTableList::CallableName())) {
                return true;
            }

            if (input->IsCallable(TKiReadTableScheme::CallableName())) {
                return true;
            }
        }

        if (auto maybeRight = TMaybeNode<TCoNth>(&node).Tuple().Maybe<TCoRight>()) {
            if (maybeRight.Input().Maybe<TKiExecDataQuery>()) {
                return true;
            }
        }

        return false;
    }

    bool CanExecute(const TExprNode& node) override {
        if (node.IsCallable(TKiReadTableScheme::CallableName()) || node.IsCallable(TKiReadTableList::CallableName())) {
            return true;
        }

        if (auto configure = TMaybeNode<TCoConfigure>(&node)) {
            if (configure.DataSource().Maybe<TKiDataSource>()) {
                return true;
            }
        }

        return false;
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        auto read = node->Child(0);
        if (!read->IsCallable(ReadName)) {
            ythrow yexception() << "Expected Read!";
        }

        TKiDataSource source(read->ChildPtr(1));
        TKikimrKey key(ctx);
        if (!key.Extract(*read->Child(2))) {
            return nullptr;
        }

        TString newName;
        switch (key.GetKeyType()) {
            case TKikimrKey::Type::Table:
                newName = TKiReadTable::CallableName();
                break;
            case TKikimrKey::Type::TableScheme:
                newName = TKiReadTableScheme::CallableName();
                break;
            case TKikimrKey::Type::TableList:
                newName = TKiReadTableList::CallableName();
                break;
            default:
                YQL_ENSURE(false, "Unsupported Kikimr KeyType.");
        }

        const TString cluster = source.Cluster().StringValue();
        const TString tablePath = key.GetTablePath();
        auto& tableDesc = SessionCtx->Tables().GetTable(cluster, tablePath);
        if (key.GetKeyType() == TKikimrKey::Type::Table) {
            if (tableDesc.Metadata->Kind == EKikimrTableKind::External) {
                if (tableDesc.Metadata->ExternalSource.SourceType == ESourceType::ExternalDataSource && tableDesc.Metadata->TableType == NYql::ETableType::Unknown) {
                    ctx.AddError(TIssue(node->Pos(ctx),
                                        TStringBuilder() << "Attempt to read from external data source \"" << tablePath << "\" without table. Please specify table to read from"));
                    return nullptr;
                }
                if (tableDesc.Metadata->ExternalSource.SourceType == ESourceType::ExternalDataSource) {
                    const auto& source = ExternalSourceFactory->GetOrCreate(tableDesc.Metadata->ExternalSource.Type);
                    ctx.Step.Repeat(TExprStep::DiscoveryIO)
                            .Repeat(TExprStep::Epochs)
                            .Repeat(TExprStep::Intents)
                            .Repeat(TExprStep::LoadTablesMetadata)
                            .Repeat(TExprStep::RewriteIO);
                    auto readArgs = read->ChildrenList();
                    readArgs[1] = Build<TCoDataSource>(ctx, node->Pos())
                                    .Category(ctx.NewAtom(node->Pos(), source->GetName()))
                                    .FreeArgs()
                                        .Add(readArgs[1]->ChildrenList()[1])
                                    .Build()
                                    .Done().Ptr();
                    readArgs[2] = ctx.NewCallable(node->Pos(), "MrTableConcat", { readArgs[2] });
                    auto newRead = ctx.ChangeChildren(*read, std::move(readArgs));
                    auto retChildren = node->ChildrenList();
                    retChildren[0] = newRead;
                    return ctx.ChangeChildren(*node, std::move(retChildren));
                } else if (tableDesc.Metadata->ExternalSource.SourceType == ESourceType::ExternalTable) {
                    const auto& source = ExternalSourceFactory->GetOrCreate(tableDesc.Metadata->ExternalSource.Type);
                    ctx.Step.Repeat(TExprStep::DiscoveryIO)
                            .Repeat(TExprStep::Epochs)
                            .Repeat(TExprStep::Intents)
                            .Repeat(TExprStep::LoadTablesMetadata)
                            .Repeat(TExprStep::RewriteIO);
                    TExprNode::TPtr path = ctx.NewCallable(node->Pos(), "String", { ctx.NewAtom(node->Pos(), tableDesc.Metadata->ExternalSource.TableLocation) });
                    auto table = ctx.NewList(node->Pos(), {ctx.NewAtom(node->Pos(), "table"), path});
                    auto newKey = ctx.NewCallable(node->Pos(), "Key", {table});
                    auto newRead = Build<TCoRead>(ctx, node->Pos())
                                            .World(read->Child(0))
                                            .DataSource(
                                                Build<TCoDataSource>(ctx, node->Pos())
                                                    .Category(ctx.NewAtom(node->Pos(), source->GetName()))
                                                    .FreeArgs()
                                                        .Add(ctx.NewAtom(node->Pos(), tableDesc.Metadata->ExternalSource.DataSourcePath))
                                                        .Add(ctx.NewAtom(node->Pos(), tableDesc.Metadata->Name))
                                                    .Build()
                                                .Done().Ptr()
                                            )
                                            .FreeArgs()
                                                .Add(ctx.NewCallable(node->Pos(), "MrTableConcat", {newKey}))
                                                .Add(ctx.NewCallable(node->Pos(), "Void", {}))
                                                .Add(BuildExternalTableSettings(node->Pos(), ctx, tableDesc.Metadata->Columns, source, tableDesc.Metadata->ExternalSource.TableContent))
                                            .Build()
                                            .Done().Ptr();
                    auto retChildren = node->ChildrenList();
                    retChildren[0] = newRead;
                    return ctx.ChangeChildren(*node, std::move(retChildren));
                }
            } else if (tableDesc.Metadata->Kind == EKikimrTableKind::View) {
                if (!SessionCtx->Config().FeatureFlags.GetEnableViews()) {
                    ctx.AddError(TIssue(node->Pos(ctx),
                                        "Views are disabled. Please contact your system administrator to enable the feature"));
                    return nullptr;
                }

                ctx.Step
                    .Repeat(TExprStep::ExpandApplyForLambdas)
                    .Repeat(TExprStep::ExprEval)
                    .Repeat(TExprStep::DiscoveryIO)
                    .Repeat(TExprStep::Epochs)
                    .Repeat(TExprStep::Intents)
                    .Repeat(TExprStep::LoadTablesMetadata)
                    .Repeat(TExprStep::RewriteIO);

                const auto& query = tableDesc.Metadata->ViewPersistedData.QueryText;
                NKqp::TKqpTranslationSettingsBuilder settingsBuilder(
                    SessionCtx->Query().Type,
                    SessionCtx->Config()._KqpYqlSyntaxVersion.Get().GetRef(),
                    cluster,
                    query,
                    SessionCtx->Config().BindingsMode,
                    GUCSettings
                );
                return RewriteReadFromView(node, ctx, query, settingsBuilder, Types.Modules);
            }
        }

        auto newRead = ctx.RenameNode(*read, newName);

        if (auto maybeRead = TMaybeNode<TKiReadTable>(newRead)) {
            auto read = maybeRead.Cast();
        }

        auto retChildren = node->ChildrenList();
        retChildren[0] = newRead;
        auto ret = ctx.ChangeChildren(*node, std::move(retChildren));
        return ret;
    }

    TExprNode::TPtr OptimizePull(const TExprNode::TPtr& source, const TFillSettings& fillSettings, TExprContext& ctx,
        IOptimizationContext& optCtx) override
    {
        auto queryType = SessionCtx->Query().Type;
        if (queryType == EKikimrQueryType::Scan || queryType == EKikimrQueryType::Query) {
            return source;
        }

        if (auto execQuery = TMaybeNode<TCoNth>(source).Tuple().Maybe<TCoRight>().Input().Maybe<TKiExecDataQuery>()) {
            auto nth = TCoNth(source);
            ui32 index = ::FromString<ui32>(nth.Index());

            if (nth.Ref().GetTypeAnn()->GetKind() != ETypeAnnotationKind::List) {
                return source;
            }

            auto exec = execQuery.Cast();
            auto queryBlocks = exec.QueryBlocks();

            ui32 blockId = 0;
            ui32 startBlockIndex = 0;
            while (blockId < queryBlocks.ArgCount() && startBlockIndex + queryBlocks.Arg(blockId).Results().Size() <= index) {
                startBlockIndex += queryBlocks.Arg(blockId).Results().Size();
                ++blockId;
            }
            auto results = queryBlocks.Arg(blockId).Results();

            auto result = results.Item(index - startBlockIndex);
            ui64 rowsLimit = ::FromString<ui64>(result.RowsLimit());
            if (!rowsLimit) {
                if (!fillSettings.RowsLimitPerWrite) {
                    return source;
                }

                // NOTE: RowsLimitPerWrite in OptimizePull already incremented by one, see result provider
                // implementation for details
                rowsLimit = *fillSettings.RowsLimitPerWrite - 1;
            }

            auto newResult = Build<TKiResult>(ctx, result.Pos())
                .Value<TCoTake>()
                    .Input(result.Value())
                    .Count<TCoUint64>()
                        .Literal().Build(ToString(rowsLimit + 1))
                        .Build()
                    .Build()
                .Columns(result.Columns())
                .RowsLimit().Build(ToString(rowsLimit))
                .Done();

            auto newResults = ctx.ChangeChild(results.Ref(), index - startBlockIndex, newResult.Ptr());
            auto newQueryBlock = ctx.ChangeChild(queryBlocks.Arg(blockId).Ref(), 0, std::move(newResults));
            auto newQueryBlocks = ctx.ChangeChild(queryBlocks.Ref(), blockId, std::move(newQueryBlock));

            auto newExec = Build<TKiExecDataQuery>(ctx, exec.Pos())
                .World(exec.World())
                .DataSink(exec.DataSink())
                .QueryBlocks(newQueryBlocks)
                .Settings(exec.Settings())
                .Ast(exec.Ast())
                .Done();

            auto ret = Build<TCoNth>(ctx, nth.Pos())
                .Tuple<TCoRight>()
                    .Input(newExec)
                    .Build()
                .Index(nth.Index())
                .Done();

            optCtx.RemapNode(exec.Ref(), newExec.Ptr());
        }

        return source;
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(compact);
        if (CanExecute(node)) {
            children.push_back(node.ChildPtr(0));
            return true;
        }

        return false;
    }

    TString GetProviderPath(const TExprNode& node) override {
        Y_UNUSED(node);

        return TString(KikimrProviderName);
    }

private:
    const NKikimr::NMiniKQL::IFunctionRegistry& FunctionRegistry;
    TTypeAnnotationContext& Types;
    TIntrusivePtr<IKikimrGateway> Gateway;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    NExternalSource::IExternalSourceFactory::TPtr ExternalSourceFactory;
    TGUCSettings::TPtr GUCSettings;

    TAutoPtr<IGraphTransformer> ConfigurationTransformer;
    TAutoPtr<IGraphTransformer> IntentDeterminationTransformer;
    TAutoPtr<IGraphTransformer> LoadTableMetadataTransformer;
    TAutoPtr<IGraphTransformer> TypeAnnotationTransformer;
    TAutoPtr<IGraphTransformer> CallableExecutionTransformer;
};

} // namespace

IGraphTransformer::TStatus TKiSourceVisitorTransformer::DoTransform(TExprNode::TPtr input,
    TExprNode::TPtr& output, TExprContext& ctx)
{
    YQL_ENSURE(input->Type() == TExprNode::Callable);
    output = input;

    if (auto node = TMaybeNode<TKiReadBase>(input)) {
        return HandleKiRead(node.Cast(), ctx);
    }

    if (input->IsCallable(ReadName)) {
        return HandleRead(TExprBase(input), ctx);
    }

    if (input->IsCallable(ConfigureName)) {
        return HandleConfigure(TExprBase(input), ctx);
    }

    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "(Kikimr DataSource) Unsupported function: "
        << input->Content()));
    return TStatus::Error;
}

TIntrusivePtr<IDataProvider> CreateKikimrDataSource(
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    TTypeAnnotationContext& types,
    TIntrusivePtr<IKikimrGateway> gateway,
    TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    const NExternalSource::IExternalSourceFactory::TPtr& externalSourceFactory,
    bool isInternalCall,
    TGUCSettings::TPtr gucSettings)
{
    return new TKikimrDataSource(functionRegistry, types, gateway, sessionCtx, externalSourceFactory, isInternalCall, gucSettings);
}

TAutoPtr<IGraphTransformer> CreateKiSourceLoadTableMetadataTransformer(TIntrusivePtr<IKikimrGateway> gateway,
    TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    TTypeAnnotationContext& types,
    const NExternalSource::IExternalSourceFactory::TPtr& externalSourceFactory,
    bool isInternalCall)
{
    return new TKiSourceLoadTableMetadataTransformer(gateway, sessionCtx, types, externalSourceFactory, isInternalCall);
}

} // namespace NYql
