#include "dq_manager.h"
#include "plugin.h"

#include "error_helpers.h"
#include "progress_merger.h"

#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/comp_nodes/dq/dq_yt_factory.h>
#include <ydb/library/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <ydb/library/yql/providers/yt/lib/log/yt_logger.h>
#include <ydb/library/yql/providers/yt/lib/res_pull/res_or_pull.h>
#include <ydb/library/yql/providers/yt/lib/row_spec/yql_row_spec.h>
#include <ydb/library/yql/providers/yt/lib/schema/schema.h>
#include <ydb/library/yql/providers/yt/lib/skiff/yql_skiff_schema.h>
#include <ydb/library/yql/providers/yt/lib/yt_download/yt_download.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>

#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.h>

#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_provider.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_state.h>
#include <ydb/library/yql/providers/dq/provider/exec/yql_dq_exectransformer.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>
#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/core/url_preprocessing/url_preprocessing.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/utils/log/log.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/writer.h>

#include <library/cpp/digest/md5/md5.h>

#include <library/cpp/resource/resource.h>

#include <util/folder/path.h>

#include <util/stream/file.h>

#include <util/string/builder.h>

#include <util/system/fs.h>

namespace NYT::NYqlPlugin {
namespace NNative {

using namespace NYson;
using namespace NKikimr::NMiniKQL;

static const TString YqlAgent = "yql_agent";

////////////////////////////////////////////////////////////////////////////////

std::optional<TString> MaybeToOptional(const TMaybe<TString>& maybeStr)
{
    if (!maybeStr) {
        return std::nullopt;
    }
    return *maybeStr;
};

////////////////////////////////////////////////////////////////////////////////

struct TQueryPlan
{
    std::optional<TString> Plan;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, PlanSpinLock);
};

struct TActiveQuery
{
    NYql::TProgramPtr Program;
    bool Compiled = false;

    TProgressMerger ProgressMerger;
    std::optional<TString> Plan;
};

////////////////////////////////////////////////////////////////////////////////

class TQueryPipelineConfigurator
    : public NYql::IPipelineConfigurator
{
public:
    TQueryPipelineConfigurator(NYql::TProgramPtr program, TQueryPlan& plan)
        : Program_(std::move(program))
        , Plan_(plan)
    { }

    void AfterCreate(NYql::TTransformationPipeline* /*pipeline*/) const override
    { }

    void AfterTypeAnnotation(NYql::TTransformationPipeline* /*pipeline*/) const override
    { }

    void AfterOptimize(NYql::TTransformationPipeline* pipeline) const override
    {
        auto transformer = [this](NYql::TExprNode::TPtr input, NYql::TExprNode::TPtr& output, NYql::TExprContext& /*ctx*/) {
            output = input;

            auto guard = WriterGuard(Plan_.PlanSpinLock);
            Plan_.Plan = MaybeToOptional(Program_->GetQueryPlan());

            return NYql::IGraphTransformer::TStatus::Ok;
        };

        pipeline->Add(NYql::CreateFunctorTransformer(transformer), "PlanOutput");
    }

private:
    NYql::TProgramPtr Program_;
    TQueryPlan& Plan_;
};

////////////////////////////////////////////////////////////////////////////////

class TSkiffConverter
    : public ISkiffConverter
{
public:
    TString ConvertNodeToSkiff(const TDqStatePtr state, const IDataProvider::TFillSettings& fillSettings, const NYT::TNode& rowSpec, const NYT::TNode& item) override
    {
        TMemoryUsageInfo memInfo("DqResOrPull");
        TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), state->FunctionRegistry->SupportsSizedAllocators());
        THolderFactory holderFactory(alloc.Ref(), memInfo, state->FunctionRegistry);
        TTypeEnvironment env(alloc);
        NYql::NCommon::TCodecContext codecCtx(env, *state->FunctionRegistry, &holderFactory);

        auto skiffBuilder = MakeHolder<TSkiffExecuteResOrPull>(fillSettings.RowsLimitPerWrite, fillSettings.AllResultsBytesLimit, codecCtx, holderFactory, rowSpec, state->TypeCtx->OptLLVM.GetOrElse("OFF"));
        if (item.IsList()) {
            skiffBuilder->SetListResult();
            for (auto& node : item.AsList()) {
                skiffBuilder->WriteNext(node);
            }
        } else {
            skiffBuilder->WriteNext(item);
        }

        return skiffBuilder->Finish();
    }

    TYtType ParseYTType(const TExprNode& node, TExprContext& ctx, const TMaybe<NYql::TColumnOrder>& columns) override
    {
        const auto sequenceItemType = GetSequenceItemType(node.Pos(), node.GetTypeAnn(), false, ctx);

        auto rowSpecInfo = MakeIntrusive<TYqlRowSpecInfo>();
        rowSpecInfo->SetType(sequenceItemType->Cast<TStructExprType>(), NTCF_ALL);
        rowSpecInfo->SetColumnOrder(columns);

        NYT::TNode tableSpec = NYT::TNode::CreateMap();
        rowSpecInfo->FillCodecNode(tableSpec[YqlRowSpecAttribute]);

        auto resultYTType = NodeToYsonString(RowSpecToYTSchema(tableSpec[YqlRowSpecAttribute], NTCF_ALL).ToNode());
        auto resultRowSpec = NYT::TNode::CreateMap()(TString{YqlIOSpecTables}, NYT::TNode::CreateList().Add(tableSpec));
        auto resultSkiffType = NodeToYsonString(TablesSpecToOutputSkiff(resultRowSpec));

        return {
            .Type = resultYTType,
            .SkiffType = resultSkiffType,
            .RowSpec = resultRowSpec
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYqlPlugin
    : public IYqlPlugin
{
public:
    TYqlPlugin(TYqlPluginOptions options)
        : DqManagerConfig_(options.DqManagerConfig ? NYTree::ConvertTo<TDqManagerConfigPtr>(options.DqManagerConfig) : nullptr)
    {
        try {
            auto singletonsConfig = NYTree::ConvertTo<TSingletonsConfigPtr>(options.SingletonsConfig);
            ConfigureSingletons(singletonsConfig);

            NYql::NLog::InitLogger(std::move(options.LogBackend));

            auto& logger = NYql::NLog::YqlLogger();

            logger.SetDefaultPriority(ELogPriority::TLOG_DEBUG);
            for (int i = 0; i < NYql::NLog::EComponentHelpers::ToInt(NYql::NLog::EComponent::MaxValue); ++i) {
                logger.SetComponentLevel((NYql::NLog::EComponent)i, NYql::NLog::ELevel::DEBUG);
            }

            NYql::SetYtLoggerGlobalBackend(NYT::ILogger::ELevel::DEBUG);
            if (NYT::TConfig::Get()->Prefix.empty()) {
                NYT::TConfig::Get()->Prefix = "//";
            }

            NYson::TProtobufWriterOptions protobufWriterOptions;
            protobufWriterOptions.ConvertSnakeToCamelCase = true;

            auto* gatewayDqConfig = GatewaysConfig_.MutableDq();
            if (DqManagerConfig_) {
                gatewayDqConfig->ParseFromStringOrThrow(NYson::YsonStringToProto(
                    options.DqGatewayConfig,
                    NYson::ReflectProtobufMessageType<NYql::TDqGatewayConfig>(),
                    protobufWriterOptions));
            }

            auto* gatewayYtConfig = GatewaysConfig_.MutableYt();
            gatewayYtConfig->ParseFromStringOrThrow(NYson::YsonStringToProto(
                options.GatewayConfig,
                NYson::ReflectProtobufMessageType<NYql::TYtGatewayConfig>(),
                protobufWriterOptions));

            NYql::TFileStorageConfig fileStorageConfig;
            fileStorageConfig.ParseFromStringOrThrow(NYson::YsonStringToProto(
                options.FileStorageConfig,
                NYson::ReflectProtobufMessageType<NYql::TFileStorageConfig>(),
                protobufWriterOptions));

            gatewayYtConfig->SetMrJobBinMd5(MD5::File(gatewayYtConfig->GetMrJobBin()));

            for (const auto& mapping : gatewayYtConfig->GetClusterMapping()) {
                Clusters_.insert({mapping.name(), TString(NYql::YtProviderName)});
                if (mapping.GetDefault()) {
                    DefaultCluster_ = mapping.name();
                }
            }

            FileStorage_ = WithAsync(CreateFileStorage(fileStorageConfig, {MakeYtDownloader(fileStorageConfig)}));

            FuncRegistry_ = NKikimr::NMiniKQL::CreateFunctionRegistry(
                NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone();

            const NKikimr::NMiniKQL::TUdfModuleRemappings emptyRemappings;

            FuncRegistry_->SetBackTraceCallback(&NYql::NBacktrace::KikimrBackTrace);

            NKikimr::NMiniKQL::TUdfModulePathsMap systemModules;

            TVector<TString> udfPaths;
            NKikimr::NMiniKQL::FindUdfsInDir(gatewayYtConfig->GetMrJobUdfsDir(), &udfPaths);
            for (const auto& path : udfPaths) {
                // Skip YQL plugin shared library itself, it is not a UDF.
                if (path.EndsWith("libyqlplugin.so")) {
                    continue;
                }
                ui32 flags = 0;
                // System Python UDFs are not used locally so we only need types.
                if (path.Contains("systempython") && path.Contains(TString("udf") + MKQL_UDF_LIB_SUFFIX)) {
                    flags |= NUdf::IRegistrator::TFlags::TypesOnly;
                }
                FuncRegistry_->LoadUdfs(path, emptyRemappings, flags);
                if (DqManagerConfig_) {
                    DqManagerConfig_->UdfsWithMd5.emplace(path, MD5::File(path));
                }
            }

            if (DqManagerConfig_) {
                DqManagerConfig_->FileStorage = FileStorage_;
                DqManager_ = New<TDqManager>(DqManagerConfig_);
            }

            gatewayYtConfig->ClearMrJobUdfsDir();

            for (const auto& m : FuncRegistry_->GetAllModuleNames()) {
                TMaybe<TString> path = FuncRegistry_->FindUdfPath(m);
                if (!path) {
                    YQL_LOG(FATAL) << "Unable to detect UDF path for module " << m;
                    exit(1);
                }
                systemModules.emplace(m, *path);
            }

            FuncRegistry_->SetSystemModulePaths(systemModules);

            NYql::NUserData::TUserData::UserDataToLibraries({}, Modules_);
            auto userDataTable = GetYqlModuleResolver(ExprContext_, ModuleResolver_, {}, Clusters_, {});

            if (!userDataTable) {
                TStringStream err;
                ExprContext_.IssueManager
                    .GetIssues()
                    .PrintTo(err);
                YQL_LOG(FATAL) << "Failed to compile modules:\n"
                               << err.Str();
                exit(1);
            }

            OperationAttributes_ = options.OperationAttributes;

            TVector<NYql::TDataProviderInitializer> dataProvidersInit;

            NYql::TYtNativeServices ytServices;
            ytServices.FunctionRegistry = FuncRegistry_.Get();
            ytServices.FileStorage = FileStorage_;
            ytServices.Config = std::make_shared<NYql::TYtGatewayConfig>(*gatewayYtConfig);

            if (DqManagerConfig_) {
                auto dqGateway = NYql::CreateDqGateway("localhost", DqManagerConfig_->GrpcPort);
                auto dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
                    NYql::GetCommonDqFactory(),
                    NYql::GetDqYtFactory(),
                    NKikimr::NMiniKQL::GetYqlFactory(),
                });
                dataProvidersInit.push_back(GetDqDataProviderInitializer(NYql::CreateDqExecTransformerFactory(MakeIntrusive<TSkiffConverter>()), dqGateway, dqCompFactory, {}, FileStorage_));
            }

            auto ytNativeGateway = CreateYtNativeGateway(ytServices);
            dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytNativeGateway));

            ProgramFactory_ = std::make_unique<NYql::TProgramFactory>(
                false, FuncRegistry_.Get(), ExprContext_.NextUniqueId, dataProvidersInit, "embedded");

            if (options.YTTokenPath) {
                TFsPath path(options.YTTokenPath);
                YqlAgentToken_ = TIFStream(path).ReadAll();
            } else if (!NYT::TConfig::Get()->Token.empty()) {
                YqlAgentToken_ = NYT::TConfig::Get()->Token;
            }
            // do not use token from .yt/token or env in queries
            NYT::TConfig::Get()->Token = {};

            ProgramFactory_->AddUserDataTable(userDataTable);
            ProgramFactory_->SetCredentials(MakeIntrusive<NYql::TCredentials>());
            ProgramFactory_->SetModules(ModuleResolver_);
            ProgramFactory_->SetUdfResolver(NYql::NCommon::CreateSimpleUdfResolver(FuncRegistry_.Get(), FileStorage_));
            ProgramFactory_->SetGatewaysConfig(&GatewaysConfig_);
            ProgramFactory_->SetFileStorage(FileStorage_);
            ProgramFactory_->SetUrlPreprocessing(MakeIntrusive<NYql::TUrlPreprocessing>(GatewaysConfig_));
        } catch (const std::exception& ex) {
            YQL_LOG(FATAL) << "Unexpected exception while initializing YQL plugin: " << ex.what();
            exit(1);
        }
        YQL_LOG(INFO) << "YQL plugin initialized";
    }

    void Start() override
    {
        if (DqManager_) {
            DqManager_->Start();
        }
    }

    TClustersResult GuardedGetUsedClusters(
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files)
    {
        auto program = ProgramFactory_->Create("-memory-", queryText);

        program->AddCredentials({{"default_yt", NYql::TCredential("yt", "", YqlAgentToken_)}});
        program->SetOperationAttrsYson(PatchQueryAttributes(OperationAttributes_, settings));

        auto userDataTable = FilesToUserTable(files);
        program->AddUserDataTable(userDataTable);

        NSQLTranslation::TTranslationSettings sqlSettings;
        sqlSettings.ClusterMapping = Clusters_;
        sqlSettings.ModuleMapping = Modules_;
        if (DefaultCluster_) {
            sqlSettings.DefaultCluster = *DefaultCluster_;
        }
        sqlSettings.SyntaxVersion = 1;
        sqlSettings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;

        if (!program->ParseSql(sqlSettings)) {
            return TClustersResult{
                .YsonError = IssuesToYtErrorYson(program->Issues()),
            };
        }

        if (!program->Compile(YqlAgent)) {
            return TClustersResult{
                .YsonError = IssuesToYtErrorYson(program->Issues()),
            };
        }

        return TClustersResult{
            .YsonError = MessageToYtErrorYson("Can't get clusters from query"),
        };
    }

    TQueryResult GuardedRun(
        TQueryId queryId,
        TString user,
        TString token,
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files,
        int executeMode)
    {
        auto program = ProgramFactory_->Create("-memory-", queryText);
        {
            auto guard = WriterGuard(ProgressSpinLock);
            ActiveQueriesProgress_[queryId].Program = program;
        }

        program->AddCredentials({{"default_yt", NYql::TCredential("yt", "", token)}});
        program->SetOperationAttrsYson(PatchQueryAttributes(OperationAttributes_, settings));

        auto userDataTable = FilesToUserTable(files);
        program->AddUserDataTable(userDataTable);

        TQueryPlan queryPlan;
        auto pipelineConfigurator = TQueryPipelineConfigurator(program, queryPlan);

        program->SetProgressWriter([&] (const NYql::TOperationProgress& progress) {
            std::optional<TString> plan;
            {
                auto guard = ReaderGuard(queryPlan.PlanSpinLock);
                plan.swap(queryPlan.Plan);
            }

            auto guard = WriterGuard(ProgressSpinLock);
            ActiveQueriesProgress_[queryId].ProgressMerger.MergeWith(progress);
            if (plan) {
                ActiveQueriesProgress_[queryId].Plan.swap(plan);
            }
        });

        program->SetResultType(NYql::IDataProvider::EResultFormat::Skiff);

        NSQLTranslation::TTranslationSettings sqlSettings;
        sqlSettings.ClusterMapping = Clusters_;
        sqlSettings.ModuleMapping = Modules_;
        if (DefaultCluster_) {
            sqlSettings.DefaultCluster = *DefaultCluster_;
        }
        sqlSettings.SyntaxVersion = 1;
        sqlSettings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
        if (DqManager_) {
            sqlSettings.DqDefaultAuto = NSQLTranslation::ISqlFeaturePolicy::MakeAlwaysAllow();
        }

        if (!program->ParseSql(sqlSettings)) {
            return TQueryResult{
                .YsonError = IssuesToYtErrorYson(program->Issues()),
            };
        }

        if (!program->Compile(user)) {
            return TQueryResult{
                .YsonError = IssuesToYtErrorYson(program->Issues()),
            };
        }

        {
            auto guard = WriterGuard(ProgressSpinLock);
            ActiveQueriesProgress_[queryId].Compiled = true;
        }

        NYql::TProgram::TStatus status = NYql::TProgram::TStatus::Error;

        // NYT::NYqlClient::EExecuteMode (yt/yt/ytlib/yql_client/public.h)
        switch (executeMode) {
        case 0: // Validate.
            status = program->Validate(user, nullptr);
            break;
        case 1: // Optimize.
            status = program->OptimizeWithConfig(user, pipelineConfigurator);
            break;
        case 2: // Run.
            status = program->RunWithConfig(user, pipelineConfigurator);
            break;
        default: // Unknown.
            return TQueryResult{
                .YsonError = MessageToYtErrorYson(Format("Unknown execution mode: %v", executeMode)),
            };
        }

        if (status == NYql::TProgram::TStatus::Error) {
            return TQueryResult{
                .YsonError = IssuesToYtErrorYson(program->Issues()),
            };
        }

        TStringStream result;
        if (program->HasResults()) {
            ::NYson::TYsonWriter yson(&result, EYsonFormat::Binary);
            yson.OnBeginList();
            for (const auto& result : program->Results()) {
                yson.OnListItem();
                yson.OnRaw(result);
            }
            yson.OnEndList();
        }

        TString progress = ExtractQuery(queryId).value_or(TActiveQuery{}).ProgressMerger.ToYsonString();

        return {
            .YsonResult = result.Empty() ? std::nullopt : std::make_optional(result.Str()),
            .Plan = MaybeToOptional(program->GetQueryPlan()),
            .Statistics = MaybeToOptional(program->GetStatistics()),
            .Progress = progress,
            .TaskInfo = MaybeToOptional(program->GetTasksInfo()),
        };
    }

    TClustersResult GetUsedClusters(
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files) noexcept override
    {
        try {
            return GuardedGetUsedClusters(queryText, settings, files);
        } catch (const std::exception& ex) {
            return TClustersResult{
                .YsonError = MessageToYtErrorYson(ex.what()),
            };
        }
    }

    TQueryResult Run(
        TQueryId queryId,
        TString user,
        TString token,
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files,
        int executeMode) noexcept override
    {
        try {
            auto result = GuardedRun(queryId, user, token, queryText, settings, files, executeMode);
            if (result.YsonError) {
                ExtractQuery(queryId);
            }

            return result;
        } catch (const std::exception& ex) {
            ExtractQuery(queryId);

            return TQueryResult{
                .YsonError = MessageToYtErrorYson(ex.what()),
            };
        }
    }

    TQueryResult GetProgress(TQueryId queryId) noexcept override
    {
        auto guard = ReaderGuard(ProgressSpinLock);
        if (ActiveQueriesProgress_.contains(queryId)) {
            TQueryResult result;
            if (ActiveQueriesProgress_[queryId].ProgressMerger.HasChangesSinceLastFlush()) {
                result.Plan = ActiveQueriesProgress_[queryId].Plan;
                result.Progress = ActiveQueriesProgress_[queryId].ProgressMerger.ToYsonString();
            }
            return result;
        } else {
            return TQueryResult{
                .YsonError = MessageToYtErrorYson(Format("No progress for queryId: %v", queryId)),
            };
        }
    }

    TAbortResult Abort(TQueryId queryId) noexcept override
    {
        NYql::TProgramPtr program;
        {
            auto guard = WriterGuard(ProgressSpinLock);
            if (!ActiveQueriesProgress_.contains(queryId)) {
                return TAbortResult{
                    .YsonError = MessageToYtErrorYson(Format("Query %v is not found", queryId)),
                };
            }
            if (!ActiveQueriesProgress_[queryId].Compiled) {
                return TAbortResult{
                    .YsonError = MessageToYtErrorYson(Format("Query %v is not compiled", queryId)),
                };
            }

            program = ActiveQueriesProgress_[queryId].Program;
        }

        try {
            program->Abort().GetValueSync();
        } catch (...) {
            return TAbortResult{
                .YsonError = MessageToYtErrorYson(Format("Failed to abort query %v: %v", queryId, CurrentExceptionMessage())),
            };
        }

        return {};
    }

private:
    const TDqManagerConfigPtr DqManagerConfig_;
    TDqManagerPtr DqManager_;
    NYql::TFileStoragePtr FileStorage_;
    NYql::TExprContext ExprContext_;
    ::TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FuncRegistry_;
    NYql::IModuleResolver::TPtr ModuleResolver_;
    NYql::TGatewaysConfig GatewaysConfig_;
    std::unique_ptr<NYql::TProgramFactory> ProgramFactory_;
    THashMap<TString, TString> Clusters_;
    std::optional<TString> DefaultCluster_;
    THashMap<TString, TString> Modules_;
    TYsonString OperationAttributes_;
    TString YqlAgentToken_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ProgressSpinLock);
    THashMap<TQueryId, TActiveQuery> ActiveQueriesProgress_;
    TVector<NYql::TDataProviderInitializer> DataProvidersInit_;

    std::optional<TActiveQuery> ExtractQuery(TQueryId queryId) {
        // NB: TProgram destructor must be called without locking.
        std::optional<TActiveQuery> query;
        auto guard = WriterGuard(ProgressSpinLock);
        auto it = ActiveQueriesProgress_.find(queryId);
        if (it != ActiveQueriesProgress_.end()) {
            query = std::move(it->second);
            ActiveQueriesProgress_.erase(it);
        }
        return query;
    }

    static TString PatchQueryAttributes(TYsonString configAttributes, TYsonString querySettings)
    {
        auto querySettingsMap = NodeFromYsonString(querySettings.ToString());
        auto resultAttributesMap = NodeFromYsonString(configAttributes.ToString());

        for (const auto& item : querySettingsMap.AsMap()) {
            resultAttributesMap[item.first] = item.second;
        }

        return NodeToYsonString(resultAttributesMap);
    }

    static NYql::TUserDataTable FilesToUserTable(const std::vector<TQueryFile>& files)
    {
        NYql::TUserDataTable table;

        for (const auto& file : files) {
            NYql::TUserDataBlock& block = table[NYql::TUserDataKey::File(NYql::GetDefaultFilePrefix() + file.Name)];

            block.Data = file.Content;
            switch (file.Type) {
                case EQueryFileContentType::RawInlineData: {
                    block.Type = NYql::EUserDataType::RAW_INLINE_DATA;
                    break;
                }
                case EQueryFileContentType::Url: {
                    block.Type = NYql::EUserDataType::URL;
                    break;
                }
                default: {
                    ythrow yexception() << "Unexpected file content type";
                }
            }
        }

        return table;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYqlPlugin> CreateYqlPlugin(TYqlPluginOptions options) noexcept
{
    return std::make_unique<NNative::TYqlPlugin>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
