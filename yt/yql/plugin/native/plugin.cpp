#include "plugin.h"

#include "error_helpers.h"
#include "progress_merger.h"

#include <ydb/library/yql/providers/yt/lib/log/yt_logger.h>
#include <ydb/library/yql/providers/yt/lib/yt_download/yt_download.h>
#include <ydb/library/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>

#include <ydb/library/yql/core/url_preprocessing/url_preprocessing.h>

#include <ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include "ydb/library/yql/providers/common/proto/gateways_config.pb.h"
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>
#include "ydb/library/yql/core/file_storage/proto/file_storage.pb.h"
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/logging/logger.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/writer.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/digest/md5/md5.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/fs.h>
#include <util/system/user.h>

namespace NYT::NYqlPlugin {
namespace NNative {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TActiveQuery
{
    TProgressMerger ProgressMerger;
    std::optional<TString> Plan;
};

////////////////////////////////////////////////////////////////////////////////

class TYqlPlugin
    : public IYqlPlugin
{
public:
    TYqlPlugin(TYqlPluginOptions& options)
    {
        try {
            NYql::NLog::InitLogger(std::move(options.LogBackend));

            auto& logger = NYql::NLog::YqlLogger();

            logger.SetDefaultPriority(ELogPriority::TLOG_DEBUG);
            for (int i = 0; i < NYql::NLog::EComponentHelpers::ToInt(NYql::NLog::EComponent::MaxValue); ++i) {
                logger.SetComponentLevel((NYql::NLog::EComponent) i, NYql::NLog::ELevel::DEBUG);
            }

            NYql::SetYtLoggerGlobalBackend(NYT::ILogger::ELevel::DEBUG);
            if (NYT::TConfig::Get()->Prefix.empty()) {
                NYT::TConfig::Get()->Prefix = "//";
            }

            auto yqlCoreFlags = GatewaysConfig_.GetYqlCore()
                .GetFlags();

            auto ytConfig = GatewaysConfig_.MutableYt();
            if (!ytConfig->HasExecuteUdfLocallyIfPossible()) {
                ytConfig->SetExecuteUdfLocallyIfPossible(true);
            }

            ytConfig->SetYtLogLevel(NYql::EYtLogLevel::YL_DEBUG);
            ytConfig->SetMrJobBin(options.MRJobBinary);
            ytConfig->SetMrJobBinMd5(MD5::File(options.MRJobBinary));

            ytConfig->ClearMrJobUdfsDir();

            auto setting = ytConfig->AddDefaultSettings();
            setting->SetName("NativeYtTypeCompatibility");
            setting->SetValue("all");

            for (const auto& [cluster, address]: options.Clusters) {
                auto item = ytConfig->AddClusterMapping();
                item->SetName(cluster);
                item->SetCluster(address);
                if (cluster == options.DefaultCluster) {
                    item->SetDefault(true);
                }

                Clusters_.insert({item->GetName(), TString(NYql::YtProviderName)});
            }
            DefaultCluster_ = options.DefaultCluster;

            NYql::TFileStorageConfig fileStorageConfig;
            fileStorageConfig.SetMaxSizeMb(1 << 14);
            FileStorage_ = WithAsync(CreateFileStorage(fileStorageConfig, {MakeYtDownloader(fileStorageConfig)}));

            FuncRegistry_ = NKikimr::NMiniKQL::CreateFunctionRegistry(
                NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone();

            const NKikimr::NMiniKQL::TUdfModuleRemappings emptyRemappings;

            FuncRegistry_->SetBackTraceCallback(&NYql::NBacktrace::KikimrBackTrace);

            NKikimr::NMiniKQL::TUdfModulePathsMap systemModules;

            TVector<TString> udfPaths;
            NKikimr::NMiniKQL::FindUdfsInDir(options.UdfDirectory, &udfPaths);
            for (const auto& path: udfPaths) {
                // Skip YQL plugin shared library itself, it is not a UDF.
                if (path.EndsWith("libyqlplugin.so")) {
                    continue;
                }
                FuncRegistry_->LoadUdfs(path, emptyRemappings, 0);
            }

            for (auto& m: FuncRegistry_->GetAllModuleNames()) {
                TMaybe<TString> path = FuncRegistry_->FindUdfPath(m);
                if (!path) {
                    YQL_LOG(FATAL) << "Unable to detect UDF path for module " << m;
                    exit(1);
                }
                systemModules.emplace(m, *path);
            }

            FuncRegistry_->SetSystemModulePaths(systemModules);

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
            ytServices.Config = std::make_shared<NYql::TYtGatewayConfig>(*ytConfig);
            auto ytNativeGateway = CreateYtNativeGateway(ytServices);
            dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytNativeGateway));

            ProgramFactory_ = std::make_unique<NYql::TProgramFactory>(
                false, FuncRegistry_.Get(), ExprContext_.NextUniqueId, dataProvidersInit, "embedded");
            YTTokenPath_ = options.YTTokenPath;
            ProgramFactory_->AddUserDataTable(userDataTable);
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

    TQueryResult GuardedRun(TQueryId queryId, TString impersonationUser, TString queryText, TYsonString settings)
    {
        auto credentials = MakeIntrusive<NYql::TCredentials>();
        if (YTTokenPath_) {
            TFsPath path(YTTokenPath_);
            auto token = TIFStream(path).ReadAll();

            credentials->AddCredential("default_yt", NYql::TCredential("yt", "", token));
        }

        credentials->AddCredential("impersonation_user_yt", NYql::TCredential("yt", "", impersonationUser));
        ProgramFactory_->SetCredentials(credentials);

        auto program = ProgramFactory_->Create("-memory-", queryText);
        program->SetOperationAttrsYson(PatchQueryAttributes(OperationAttributes_, settings));

        auto maybeToOptional = [] (const TMaybe<TString>& maybeStr) -> std::optional<TString> {
            if (!maybeStr) {
                return std::nullopt;
            }
            return *maybeStr;
        };

        program->SetProgressWriter([&] (const NYql::TOperationProgress& progress) {
            auto guard = WriterGuard(ProgressSpinLock);
            ActiveQueriesProgress_[queryId].ProgressMerger.MergeWith(progress);
            ActiveQueriesProgress_[queryId].Plan = maybeToOptional(program->GetQueryPlan());
        });

        NSQLTranslation::TTranslationSettings sqlSettings;
        sqlSettings.ClusterMapping = Clusters_;
        sqlSettings.ModuleMapping = Modules_;
        if (DefaultCluster_) {
            sqlSettings.DefaultCluster = *DefaultCluster_;
        }
        sqlSettings.SyntaxVersion = 1;
        sqlSettings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;

        if (!program->ParseSql(sqlSettings)) {
            return TQueryResult{
                .YsonError = IssuesToYtErrorYson(program->Issues()),
            };
        }

        if (!program->Compile(GetUsername())) {
            return TQueryResult{
                .YsonError = IssuesToYtErrorYson(program->Issues()),
            };
        }

        NYql::TProgram::TStatus status = NYql::TProgram::TStatus::Error;
        status = program->Run(GetUsername(), nullptr, nullptr, nullptr);

        if (status == NYql::TProgram::TStatus::Error) {
            return TQueryResult{
                .YsonError = IssuesToYtErrorYson(program->Issues()),
            };
        }

        TStringStream result;
        if (program->HasResults()) {
            ::NYson::TYsonWriter yson(&result, EYsonFormat::Binary);
            yson.OnBeginList();
            for (const auto& result: program->Results()) {
                yson.OnListItem();
                yson.OnRaw(result);
            }
            yson.OnEndList();
        }

        TString progress;
        {
            auto guard = WriterGuard(ProgressSpinLock);
            progress = ActiveQueriesProgress_[queryId].ProgressMerger.ToYsonString();
            ActiveQueriesProgress_.erase(queryId);
        }

        return {
            .YsonResult = result.Empty() ? std::nullopt : std::make_optional(result.Str()),
            .Plan = maybeToOptional(program->GetQueryPlan()),
            .Statistics = maybeToOptional(program->GetStatistics()),
            .Progress = progress,
            .TaskInfo = maybeToOptional(program->GetTasksInfo()),
        };
    }

    TQueryResult Run(TQueryId queryId, TString impersonationUser, TString queryText, TYsonString settings) noexcept override
    {
        try {
            return GuardedRun(queryId, impersonationUser, queryText, settings);
        } catch (const std::exception& ex) {
            {
                auto guard = WriterGuard(ProgressSpinLock);
                ActiveQueriesProgress_.erase(queryId);
            }

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

private:
    NYql::TFileStoragePtr FileStorage_;
    NYql::TExprContext ExprContext_;
    ::TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FuncRegistry_;
    NYql::IModuleResolver::TPtr ModuleResolver_;
    NYql::TGatewaysConfig GatewaysConfig_;
    std::unique_ptr<NYql::TProgramFactory> ProgramFactory_;
    TString YTTokenPath_;
    THashMap<TString, TString> Clusters_;
    std::optional<TString> DefaultCluster_;
    THashMap<TString, TString> Modules_;
    TYsonString OperationAttributes_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ProgressSpinLock);
    THashMap<TQueryId, TActiveQuery> ActiveQueriesProgress_;

    TString PatchQueryAttributes(TYsonString configAttributes, TYsonString querySettings)
    {
        auto querySettingsMap = NodeFromYsonString(querySettings.ToString());
        auto resultAttributesMap = NodeFromYsonString(configAttributes.ToString());

        for (const auto& item: querySettingsMap.AsMap()) {
            resultAttributesMap[item.first] = item.second;
        }

        return NodeToYsonString(resultAttributesMap);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYqlPlugin> CreateYqlPlugin(TYqlPluginOptions& options) noexcept
{
    return std::make_unique<NNative::TYqlPlugin>(options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
