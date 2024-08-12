#include "yql_embedded.h"

#include <ydb/library/yql/providers/yt/lib/log/yt_logger.h>
#include <ydb/library/yql/providers/yt/lib/yt_download/yt_download.h>
#include <ydb/library/yql/providers/yt/lib/yt_url_lister/yt_url_lister.h>
#include <ydb/library/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>

#include <ydb/library/yql/core/url_preprocessing/url_preprocessing.h>

#include <ydb/library/yql/providers/common/udf_resolve/yql_outproc_udf_resolver.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>
#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>
#include <ydb/library/yql/core/file_storage/http_download/http_download.h>
#include <ydb/library/yql/core/url_lister/url_lister_manager.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <library/cpp/yson/parser.h>
#include <library/cpp/json/json_writer.h>

#include <library/cpp/resource/resource.h>
#include <google/protobuf/text_format.h>
#include <library/cpp/digest/md5/md5.h>

#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/subst.h>
#include <util/system/fs.h>
#include <util/system/user.h>
#include <util/system/env.h>

namespace NYql {
    namespace NEmbedded {
        namespace {
            void ThrowNotSupported() {
                ythrow yexception() << "Yson element is not supported";
            }

            class TJsonConsumer : public NYson::TYsonConsumerBase {
            public:
                TJsonConsumer(NJson::TJsonWriter& writer)
                    : Writer(writer)
                {}

                void OnStringScalar(TStringBuf value) override {
                    Writer.Write(value);
                }

                void OnInt64Scalar(i64 value) override {
                    Y_UNUSED(value);
                    ThrowNotSupported();
                }

                void OnUint64Scalar(ui64 value) override {
                    Y_UNUSED(value);
                    ThrowNotSupported();
                }

                void OnDoubleScalar(double value) override {
                    Y_UNUSED(value);
                    ThrowNotSupported();
                }

                void OnBooleanScalar(bool value) override {
                    Writer.Write(value);
                }

                void OnEntity() override {
                    Writer.WriteNull();
                }

                void OnBeginList() override {
                    Writer.OpenArray();
                }

                void OnListItem() override {
                }

                void OnEndList() override {
                    Writer.CloseArray();
                }

                void OnBeginMap() override {
                    Writer.OpenMap();
                }

                void OnKeyedItem(TStringBuf key) override {
                    Writer.WriteKey(key);
                }

                void OnEndMap() override {
                    Writer.CloseMap();
                }

                void OnBeginAttributes() override {
                    ThrowNotSupported();
                }

                void OnEndAttributes() override {
                    ThrowNotSupported();
                }


            private:
                NJson::TJsonWriter& Writer;
            };

            TString Yson2Json(const TString& yson) {
                TString jsonString;

                TStringOutput jsonStream{ jsonString };
                NJson::TJsonWriter jsonWriter{ &jsonStream, false };
                TJsonConsumer jsonConsumer{ jsonWriter };
                NYson::ParseYsonStringBuffer(yson, &jsonConsumer);
                jsonWriter.Flush();
                return jsonString;
            }
        }

        class TOperation: public IOperation {
        public:
            TOperation(const TString& result, const TString& plan, const TString& statistics, const TString& taskInfo)
                : Result_(result)
                , Plan_(plan)
                , Statistics_(statistics)
                , TaskInfo_(taskInfo)
            {
            }

            const TString& YsonResult() const override {
                return Result_;
            }

            const TString& Plan() const override {
                return Plan_;
            }

            const TString& Statistics() const override {
                return Statistics_;
            }

            const TString& TaskInfo() const override {
                return TaskInfo_;
            }

        private:
            const TString Result_;
            const TString Plan_;
            const TString Statistics_;
            const TString TaskInfo_;
        };

        class TOperationFactory: public IOperationFactory {
        public:
            TOperationFactory(const TOperationFactoryOptions& options,
                const TString& configData,
                std::function<NFS::IDownloaderPtr(const TFileStorageConfig&)> arcDownloaderFactory)
                : Logger(&Cerr)
                , Options_(options)
            {
                auto& logger = NLog::YqlLogger();
                logger.SetDefaultPriority(Options_.LogLevel_);
                for (int i = 0; i < NLog::EComponentHelpers::ToInt(NLog::EComponent::MaxValue); ++i) {
                    logger.SetComponentLevel((NLog::EComponent)i, (NLog::ELevel)Options_.LogLevel_);
                }

                NYql::SetYtLoggerGlobalBackend(Options_.YtLogLevel_);
                if (NYT::TConfig::Get()->Prefix.empty()) {
                    NYT::TConfig::Get()->Prefix = "//";
                }

                if (GetEnv("YT_FORCE_IPV6").empty()) {
                    NYT::TConfig::Get()->ForceIpV6 = true;
                }

                const bool useStaticLinking = Options_.MrJobBinary_.empty();
                if (Options_.MrJobBinary_) {
                    EnsureBinary(Options_.MrJobBinary_, "MrJobBinary");
                }

                if (!::google::protobuf::TextFormat::ParseFromString(configData, &GatewaysConfig_)) {
                    ythrow yexception() << "Bad format of gateways configuration";
                }
                auto yqlCoreFlags = GatewaysConfig_.GetYqlCore().GetFlags();
                GatewaysConfig_.MutableYqlCore()->ClearFlags();
                for (auto flag : yqlCoreFlags) {
                    if (flag.GetName() != "GeobaseDownloadUrl") {
                        *GatewaysConfig_.MutableYqlCore()->AddFlags() = flag;
                    }
                }

                auto ytConfig = GatewaysConfig_.MutableYt();
                if (!ytConfig->HasExecuteUdfLocallyIfPossible()) {
                    ytConfig->SetExecuteUdfLocallyIfPossible(true);
                }
                ytConfig->SetYtLogLevel(static_cast<NYql::EYtLogLevel>(Options_.YtLogLevel_));
                if (useStaticLinking) {
                    ytConfig->ClearMrJobBin();
                } else {
                    ytConfig->SetMrJobBin(Options_.MrJobBinary_);
                    ytConfig->SetMrJobBinMd5(MD5::File(Options_.MrJobBinary_));
                }

                ytConfig->ClearMrJobUdfsDir();
                if (Options_.LocalChainTest_) {
                    ytConfig->SetLocalChainTest(true);
                    ytConfig->SetLocalChainFile(Options_.LocalChainFile_);
                }

                for (const auto& cluster : Options_.YtClusters_) {
                    auto clusterMapping = ytConfig->AddClusterMapping();
                    clusterMapping->SetName(cluster.Name_);
                    clusterMapping->SetCluster(cluster.Cluster_);
                }
                for (size_t index = 0; index < ytConfig->ClusterMappingSize(); ++index) {
                    auto cluster = ytConfig->MutableClusterMapping(index);
                    auto settings = cluster->MutableSettings();
                    bool hasOwners = false;
                    for (int settingsIndex = 0; settingsIndex < settings->size(); ++settingsIndex) {
                        auto attr = settings->Mutable(settingsIndex);
                        if (attr->GetName() == "Owners") {
                            hasOwners = true;
                            if (!Options_.YtOwners_.empty()) {
                                attr->SetValue(Options_.YtOwners_);
                            }
                        }
                    }

                    if (!hasOwners && !Options_.YtOwners_.empty()) {
                        auto newSetting = settings->Add();
                        newSetting->SetName("Owners");
                        newSetting->SetValue(Options_.YtOwners_);
                    }

                    Clusters_.insert({cluster->GetName(), TString(YtProviderName)});
                }

                TFileStorageConfig fileStorageConfig;
                fileStorageConfig.SetMaxSizeMb(1 << 14);

                std::vector<NFS::IDownloaderPtr> downloaders;
                downloaders.push_back(MakeYtDownloader(fileStorageConfig));
                auto arcDownloader = arcDownloaderFactory(fileStorageConfig);
                if (arcDownloader) {
                    downloaders.push_back(arcDownloader);
                }

                FileStorage_ = WithAsync(CreateFileStorage(fileStorageConfig, downloaders));

                NResource::TResources libs;
                const TStringBuf prefix = "resfs/file/yql_libs/";
                NResource::FindMatch(prefix, &libs);
                for (auto x : libs) {
                    auto libName = x.Key;
                    libName.SkipPrefix(prefix);
                    NUserData::TUserData d{ NUserData::EType::LIBRARY,
                        NUserData::EDisposition::RESOURCE_FILE,
                        TString("yql_libs/") + libName,
                        TString(x.Key) };
                    Options_.UserData_.push_back(d);
                }

                NUserData::TUserData::UserDataToLibraries(Options_.UserData_, Modules_);

                FuncRegistry_ = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone();

                const NKikimr::NMiniKQL::TUdfModuleRemappings emptyRemappings;
                if (!useStaticLinking && !Options_.UdfsDir_.empty()) {
                    FuncRegistry_->SetBackTraceCallback(&NYql::NBacktrace::KikimrBackTrace);

                    NKikimr::NMiniKQL::TUdfModulePathsMap systemModules;
                    if (!Options_.UdfResolverBinary_.empty()) {
                        NCommon::LoadSystemModulePaths(
                            Options_.UdfResolverBinary_,
                            Options_.UdfsDir_,
                            &systemModules);

                        if (Options_.PreloadUdfs_) {
                            for (const auto& p : systemModules) {
                                FuncRegistry_->LoadUdfs(p.second, emptyRemappings, 0);
                            }
                        }
                    } else {
                        TVector<TString> udfPaths;
                        NKikimr::NMiniKQL::FindUdfsInDir(Options_.UdfsDir_, &udfPaths);
                        for (const auto& path : udfPaths) {
                            Cerr << path << "\n";
                            FuncRegistry_->LoadUdfs(path, emptyRemappings, 0);
                        }

                        for (auto& m : FuncRegistry_->GetAllModuleNames()) {
                            TMaybe<TString> path = FuncRegistry_->FindUdfPath(m);
                            if (!path) {
                                // should not happen
                                ythrow yexception() << "Unable to detect UDF path for module " << m;
                            }
                            systemModules.emplace(m, *path);
                        }
                    }

                    FuncRegistry_->SetSystemModulePaths(systemModules);
                }

                if (useStaticLinking) {
                    NKikimr::NMiniKQL::FillStaticModules(*FuncRegistry_);
                }

                TUserDataTable userDataTable = GetYqlModuleResolver(ExprContext_, ModuleResolver_, Options_.UserData_, Clusters_, {});

                if (!userDataTable) {
                    TStringStream err;
                    ExprContext_.IssueManager.GetIssues().PrintTo(err);
                    ythrow yexception() << "Failed to compile modules:\n"
                                        << err.Str();
                }

                TVector<TDataProviderInitializer> dataProvidersInit;

                TYtNativeServices ytServices;
                ytServices.FunctionRegistry = FuncRegistry_.Get();
                ytServices.FileStorage = FileStorage_;
                ytServices.Config = std::make_shared<TYtGatewayConfig>(*ytConfig);
                auto ytNativeGateway = CreateYtNativeGateway(ytServices);
                dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytNativeGateway));

                ProgramFactory_ = MakeHolder<TProgramFactory>(
                    false, FuncRegistry_.Get(), ExprContext_.NextUniqueId, dataProvidersInit, "embedded");
                auto credentials = MakeIntrusive<TCredentials>();
                if (!Options_.YtToken_.empty()) {
                    credentials->AddCredential("default_yt", TCredential("yt", "", Options_.YtToken_));
                }
                if (!Options_.StatToken_.empty()) {
                    credentials->AddCredential("default_statface", TCredential("statface", "", Options_.StatToken_));
                }
                for (const auto& [name, value] : Options_.CustomTokens_) {
                    credentials->AddCredential(name, TCredential("custom", "", value));
                }

                ProgramFactory_->AddUserDataTable(userDataTable);
                ProgramFactory_->SetCredentials(credentials);
                ProgramFactory_->SetModules(ModuleResolver_);
                ProgramFactory_->SetUdfResolver((useStaticLinking || Options_.UdfResolverBinary_.empty()) ? NCommon::CreateSimpleUdfResolver(FuncRegistry_.Get(), FileStorage_) :
                    NCommon::CreateOutProcUdfResolver(FuncRegistry_.Get(), FileStorage_, Options_.UdfResolverBinary_, {}, {}, false, {}));
                ProgramFactory_->SetGatewaysConfig(&GatewaysConfig_);
                ProgramFactory_->SetFileStorage(FileStorage_);
                ProgramFactory_->SetUrlPreprocessing(MakeIntrusive<TUrlPreprocessing>(GatewaysConfig_));
                ProgramFactory_->SetUrlListerManager(
                    MakeUrlListerManager(
                        {MakeYtUrlLister()}
                    )
                );
            }

            THolder<IOperation> Run(const TString& queryText, const TOperationOptions& options) const override {
                TProgramPtr program = ProgramFactory_->Create("-memory-", queryText);
                if (options.Title) {
                    program->SetOperationTitle(*options.Title);
                }

                if (options.Attributes) {
                    program->SetOperationAttrsYson(*options.Attributes);
                }

                if (options.Parameters) {
                    program->SetParametersYson(*options.Parameters);
                }

                NSQLTranslation::TTranslationSettings sqlSettings;
                sqlSettings.ClusterMapping = Clusters_;
                sqlSettings.ModuleMapping = Modules_;
                sqlSettings.SyntaxVersion = options.SyntaxVersion;
                sqlSettings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
                for (const auto& item : Options_.UserData_) {
                    if (item.Type_ == NUserData::EType::LIBRARY) {
                        sqlSettings.Libraries.emplace(item.Disposition_ == NUserData::EDisposition::RESOURCE_FILE ?
                            item.Name_ : item.Content_);
                    }
                }

                if (!program->ParseSql(sqlSettings)) {
                    TStringStream err;
                    program->PrintErrorsTo(err);
                    ythrow yexception() << "Failed to parse SQL: " << err.Str();
                }

                if (!program->Compile(GetUsername())) {
                    TStringStream err;
                    program->PrintErrorsTo(err);
                    ythrow yexception() << "Failed to compile: " << err.Str();
                }

                TProgram::TStatus status = TProgram::TStatus::Error;
                switch (options.Mode) {
                case EExecuteMode::Run:
                    status = program->Run(GetUsername(), nullptr, nullptr, nullptr);
                    break;
                case EExecuteMode::Optimize:
                    status = program->Optimize(GetUsername(), nullptr, nullptr, nullptr);
                    break;
                case EExecuteMode::Validate:
                    status = program->Validate(GetUsername(), nullptr);
                    break;
                case EExecuteMode::Lineage:
                    status = program->Lineage(GetUsername(), nullptr, nullptr);
                    break;
                }

                if (status == TProgram::TStatus::Error) {
                    TStringStream err;
                    program->PrintErrorsTo(err);
                    ythrow yexception() << "Failed to run: " << err.Str();
                }

                TStringStream result;
                if (options.Mode == EExecuteMode::Lineage) {
                    if (auto data = program->GetLineage()) {
                        TStringInput in(*data);
                        NYson::ReformatYsonStream(&in, &result, Options_.ResultFormat_);
                    }
                } else if (program->HasResults()) {
                    NYson::TYsonWriter yson(&result, Options_.ResultFormat_);
                    yson.OnBeginList();
                    for (const auto& result : program->Results()) {
                        yson.OnListItem();
                        yson.OnRaw(result);
                    }
                    yson.OnEndList();
                }

                auto plan = program->GetQueryPlan(TPlanSettings().SetWithLimits(false)).GetOrElse("");
                auto taskInfo = program->GetTasksInfo().GetOrElse("");

                auto statistics = program->GetStatistics().GetOrElse("");
                if (statistics) {
                    TStringStream strInput(statistics);
                    TStringStream strFormatted;
                    NYson::ReformatYsonStream(&strInput, &strFormatted, NYson::EYsonFormat::Pretty);
                    statistics = strFormatted.Str();
                }

                if (taskInfo) {
                    TStringStream strInput(taskInfo);
                    TStringStream strFormatted;
                    NYson::ReformatYsonStream(&strInput, &strFormatted, NYson::EYsonFormat::Pretty);
                    taskInfo = strFormatted.Str();
                }

                return MakeHolder<TOperation>(result.Str(), plan, statistics, taskInfo);
            }

            void Save(const TString& queryText, const TOperationOptions& options, const TString& destinationFolder) const override {
                using namespace NUserData;
                TString finalQueryText;

                TStringBuilder cmdLine;
                cmdLine << "#!/usr/bin/env bash\nset -eux\nya yql -i main.sql";
                cmdLine << " --syntax-version=" << options.SyntaxVersion;
                if (options.Title) {
                    cmdLine << " --title=" << options.Title.Get()->Quote();
                }

                if (options.Parameters) {
                    TFileOutput paramFile(TFsPath(destinationFolder) / "params.json");
                    paramFile.Write(Yson2Json(*options.Parameters));
                    cmdLine << " --parameters-file=params.json";
                }

                ui32 fileIndex = 0;
                for (const auto& item : Options_.UserData_) {
                    switch (item.Disposition_) {
                        case EDisposition::INLINE: {
                            auto path = "files" + ToString(++fileIndex);
                            TFileOutput dataFile(TFsPath(destinationFolder) / path);
                            dataFile.Write(item.Content_);
                            cmdLine << " -F " << item.Name_.Quote() << "@" << path;
                            break;
                        }
                        case EDisposition::RESOURCE:
                        case EDisposition::RESOURCE_FILE: {
                            TString skipSlash(TStringBuf(item.Content_).After('/'));
                            if (item.Type_ == EType::LIBRARY) {
                                finalQueryText += "pragma library(" + skipSlash.Quote() + ");\n";
                            }

                            auto path = "files" + ToString(++fileIndex);
                            TFileOutput dataFile(TFsPath(destinationFolder) / path);
                            auto resContent = NResource::Find(item.Content_);
                            dataFile.Write(resContent);
                            cmdLine << " -F " << (item.Type_ == EType::LIBRARY ? skipSlash : item.Name_).Quote() << "@" << path;
                            break;
                        }
                        case EDisposition::FILESYSTEM: {
                            cmdLine << " -F " << item.Name_.Quote() << "@" << RealPath(item.Content_).Quote();
                            break;
                        }
                        case EDisposition::URL: {
                            cmdLine << " -U " << item.Name_.Quote() << "@" << item.Content_.Quote();
                            break;
                        }
                    }
                }


                auto patchedQueryText = queryText;
                SubstGlobal(patchedQueryText, "import .", "import ");

                finalQueryText += patchedQueryText;
                TFileOutput sqlFile(TFsPath(destinationFolder) / "main.sql");
                sqlFile.Write(finalQueryText);

                TFileOutput runFile(TFsPath(destinationFolder) / "run.sh");
                runFile.Write(cmdLine);
                runFile.Finish();
                Chmod((TFsPath(destinationFolder) / "run.sh").c_str(), MODE0755);
            }

        private:
            void EnsureBinary(const TString& path, const TString& name) {
                if (path.empty()) {
                    ythrow yexception() << "Parameter: " << name << " must not be empty";
                }

                if (!NFs::Exists(path)) {
                    ythrow yexception() << "Binary for parameter: " << name << " is not found at path: " << path;
                }
            }

        private:
            NLog::YqlLoggerScope Logger;
            TOperationFactoryOptions Options_;
            TFileStoragePtr FileStorage_;
            TExprContext ExprContext_;
            TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FuncRegistry_;
            IModuleResolver::TPtr ModuleResolver_;
            TGatewaysConfig GatewaysConfig_;
            THolder<TProgramFactory> ProgramFactory_;
            THashMap<TString, TString> Clusters_;
            THashMap<TString, TString> Modules_;
            THashSet<TString> Libraries_;
        };

        THolder<IOperationFactory> MakeOperationFactory(
            const TOperationFactoryOptions& options,
            const TString& configData,
            std::function<NFS::IDownloaderPtr(const TFileStorageConfig&)> arcDownloaderFactory) {
            return MakeHolder<TOperationFactory>(options, configData, arcDownloaderFactory);
        }
    }
}
