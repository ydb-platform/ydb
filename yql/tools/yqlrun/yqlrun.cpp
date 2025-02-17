#include <yql/tools/yqlrun/lib/yqlrun_lib.h>
#include <yql/tools/yqlrun/http/yql_server.h>

#include <yql/essentials/providers/common/udf_resolve/yql_outproc_udf_resolver.h>
#include <yql/essentials/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <yql/essentials/providers/common/udf_resolve/yql_udf_resolver_with_index.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <yql/essentials/utils/log/tls_backend.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/protos/pg_ext.pb.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/core/file_storage/proto/file_storage.pb.h>
#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/core/pg_ext/yql_pg_ext.h>
#include <yql/essentials/core/yql_udf_resolver.h>
#include <yql/essentials/core/yql_udf_index.h>
#include <yql/essentials/core/yql_library_compiler.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/sql.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/logger/stream.h>

#include <google/protobuf/text_format.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/datetime/base.h>

using namespace NYql;
using namespace NKikimr::NMiniKQL;
using namespace NYql::NHttp;

namespace NMiniKQL = NKikimr::NMiniKQL;

class TStoreMappingFunctor: public NLastGetopt::IOptHandler {
public:
    TStoreMappingFunctor(THashMap<TString, TString>* target, char delim = '@')
        : Target(target)
        , Delim(delim)
    {
    }

    void HandleOpt(const NLastGetopt::TOptsParser* parser) final {
        const TStringBuf val(parser->CurValOrDef());
        const auto service = TString(val.After(Delim));
        auto res = Target->emplace(TString(val.Before(Delim)), service);
        if (!res.second) {
            /// force replace already exist parametr
            res.first->second = service;
        }
    }

private:
    THashMap<TString, TString>* Target;
    char Delim;
};

void CommonInit(const NLastGetopt::TOptsParseResult& res, const TString& udfResolverPath, bool filterSysCalls,
    const TVector<TString>& udfsPaths, TFileStoragePtr fileStorage,
    IUdfResolver::TPtr& udfResolver, IFunctionRegistry::TPtr funcRegistry, TUdfIndex::TPtr& udfIndex) {

    if (fileStorage && res.Has("scan-udfs")) {
        if (!udfResolverPath) {
            ythrow yexception() << "udf-resolver path must be specified when use 'scan-udfs'";
        }

        udfResolver = NCommon::CreateOutProcUdfResolver(funcRegistry.Get(), fileStorage, udfResolverPath, {}, {}, filterSysCalls, {});

        Cerr << TInstant::Now().ToStringLocalUpToSeconds() << " Udf scanning started for " << udfsPaths.size() << " udfs ..." << Endl;
        udfIndex = new TUdfIndex();
        LoadRichMetadataToUdfIndex(*udfResolver, udfsPaths, false, TUdfIndex::EOverrideMode::RaiseError, *udfIndex);
        Cerr << TInstant::Now().ToStringLocalUpToSeconds() << " UdfIndex done." << Endl;

        udfResolver = NCommon::CreateUdfResolverWithIndex(udfIndex, udfResolver, fileStorage);
        Cerr << TInstant::Now().ToStringLocalUpToSeconds() << " Udfs scanned" << Endl;
        return;
    }

    udfResolver = fileStorage && udfResolverPath
        ? NCommon::CreateOutProcUdfResolver(funcRegistry.Get(), fileStorage, udfResolverPath, {}, {}, false, {})
        : NCommon::CreateSimpleUdfResolver(funcRegistry.Get(), fileStorage, true);
}

template <typename TMessage>
THolder<TMessage> ParseProtoConfig(const TString& cfgFile) {
    auto config = MakeHolder<TMessage>();
    TString configData = TFileInput(cfgFile).ReadAll();;

    using ::google::protobuf::TextFormat;
    if (!TextFormat::ParseFromString(configData, config.Get())) {
        Cerr << "Bad format of gateways configuration";
        return {};
    }

    return config;
}


int RunUI(int argc, const char* argv[])
{
    Cerr << "yqlrun ABI version: " << NKikimr::NUdf::CurrentAbiVersionStr() << Endl;

    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    TVector<TString> udfsPaths;
    TString udfsDir;
    TString mountConfig;
    TVector<TString> filesMappingList;
    TString udfResolverPath;
    bool udfResolverFilterSyscalls = false;
    TString gatewaysCfgFile;
    TString fsCfgFile;
    TString pgExtConfig;

    THashMap<TString, TString> clusterMapping;
    clusterMapping["plato"] = YtProviderName;
    THashSet<TString> sqlFlags;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption('u', "udf", "Load shared library with UDF by given path").AppendTo(&udfsPaths);
    opts.AddLongOption("udfs-dir", "Load all shared libraries with UDFs found in given directory").StoreResult(&udfsDir);
    opts.AddLongOption('m', "mounts", "Mount points config file.").StoreResult(&mountConfig);
    opts.AddLongOption('f', "file", "name@path").AppendTo(&filesMappingList);
    opts.AddLongOption("udf-resolver", "Path to udf-resolver").Optional().RequiredArgument("PATH").StoreResult(&udfResolverPath);
    opts.AddLongOption("udf-resolver-filter-syscalls", "Filter syscalls in udf resolver")
        .Optional()
        .NoArgument()
        .SetFlag(&udfResolverFilterSyscalls);
    opts.AddLongOption("scan-udfs", "Scan specified udfs with external udf resolver to use static function registry").NoArgument();
    opts.AddLongOption('C', "cluster", "set cluster to service mapping").RequiredArgument("name@service").Handler(new TStoreMappingFunctor(&clusterMapping));
    opts.AddLongOption("gateways-cfg", "gateways configuration file").Optional().RequiredArgument("FILE").StoreResult(&gatewaysCfgFile);
    opts.AddLongOption("fs-cfg", "fs configuration file").Optional().RequiredArgument("FILE").StoreResult(&fsCfgFile);
    opts.AddLongOption("pg-ext", "pg extensions config file").StoreResult(&pgExtConfig);
    opts.AddLongOption("sql-flags", "SQL translator pragma flags").SplitHandler(&sqlFlags, ',');

    TServerConfig config;
    config.SetAssetsPath("http/www");
    config.InitCliOptions(opts);
    NLastGetopt::TOptsParseResult res(&opts, argc, argv);
    config.ParseFromCli(res);

    TUserDataTable userData;
    for (auto& s : filesMappingList) {
        TStringBuf fileName, filePath;
        TStringBuf(s).Split('@', fileName, filePath);
        if (fileName.empty() || filePath.empty()) {
            Cerr << "Incorrect file mapping, expected form name@path, e.g. MyFile@file.txt" << Endl;
            return 1;
        }

        auto& file = userData[TUserDataKey::File(GetDefaultFilePrefix() + fileName)];
        file.Type = EUserDataType::PATH;
        file.Data = filePath;
    }

    NMiniKQL::FindUdfsInDir(udfsDir, &udfsPaths);
    NPg::SetSqlLanguageParser(NSQLTranslationPG::CreateSqlLanguageParser());
    NPg::LoadSystemFunctions(*NSQLTranslationPG::CreateSystemFunctionsParser());
    if (!pgExtConfig.empty()) {
        auto config = ParseProtoConfig<NProto::TPgExtensions>(pgExtConfig);
        Y_ABORT_UNLESS(config);
        TVector<NPg::TExtensionDesc> extensions;
        PgExtensionsFromProto(*config, extensions);
        NPg::RegisterExtensions(extensions, false,
            *NSQLTranslationPG::CreateExtensionSqlParser(),
            NKikimr::NMiniKQL::CreateExtensionLoader().get());
    }

    NPg::GetSqlLanguageParser()->Freeze();

    THolder<TGatewaysConfig> gatewaysConfig;
    if (!gatewaysCfgFile.empty()) {
        gatewaysConfig = ParseProtoConfig<TGatewaysConfig>(gatewaysCfgFile);
        if (!gatewaysConfig) {
            return -1;
        }

        if (gatewaysConfig->HasSqlCore()) {
            sqlFlags.insert(gatewaysConfig->GetSqlCore().GetTranslationFlags().begin(), gatewaysConfig->GetSqlCore().GetTranslationFlags().end());
        }
    }

    THolder<TFileStorageConfig> fsConfig;
    if (!fsCfgFile.empty()) {
        fsConfig = ParseProtoConfig<TFileStorageConfig>(fsCfgFile);
        if (!fsConfig) {
            return 1;
        }
    } else {
        fsConfig = MakeHolder<TFileStorageConfig>();
    }

    auto fileStorage = WithAsync(CreateFileStorage(*fsConfig));

    IUdfResolver::TPtr udfResolver;
    auto funcRegistry = CreateFunctionRegistry(&NYql::NBacktrace::KikimrBackTrace, CreateBuiltinRegistry(), false, udfsPaths);
    TUdfIndex::TPtr udfIndex;

    CommonInit(res, udfResolverPath, udfResolverFilterSyscalls, udfsPaths, fileStorage, udfResolver, funcRegistry, udfIndex);

    NSQLTranslation::TTranslators translators(
        nullptr,
        NSQLTranslationV1::MakeTranslator(),
        NSQLTranslationPG::MakeTranslator()
    );

    TExprContext ctx;
    ctx.NextUniqueId = NPg::GetSqlLanguageParser()->GetContext().NextUniqueId;
    IModuleResolver::TPtr moduleResolver;
    if (!mountConfig.empty()) {
        TModulesTable modules;
        auto mount = ParseProtoConfig<NYqlMountConfig::TMountConfig>(mountConfig);
        Y_ABORT_UNLESS(mount);
        FillUserDataTableFromFileSystem(*mount, userData);

        if (!CompileLibraries(translators, userData, ctx, modules)) {
            Cerr << "Errors on compile libraries:" << Endl;
            ctx.IssueManager.GetIssues().PrintTo(Cerr);
            return -1;
        }

        moduleResolver = std::make_shared<TModuleResolver>(translators, std::move(modules), ctx.NextUniqueId, clusterMapping, sqlFlags);
    } else {
        if (!GetYqlDefaultModuleResolver(ctx, moduleResolver, clusterMapping)) {
            Cerr << "Errors loading default YQL libraries:" << Endl;
            ctx.IssueManager.GetIssues().PrintTo(Cerr);
            return -1;
        }
    }

    TString fn = "pkg/a/b/c.sql";
    TString content0 = "$sqr = ($x) -> { return 2 * $x * $x; }; export $sqr;";
    TString content1 = "$sqr = ($x) -> { return 3 * $x * $x; }; export $sqr;";
    moduleResolver->RegisterPackage("a.b");
    if (!moduleResolver->AddFromMemory(fn, content0, ctx, 1, 0) || !moduleResolver->AddFromMemory(fn, content1, ctx, 1, 1)) {
        Cerr << "Unable to compile SQL library" << Endl;
        ctx.IssueManager.GetIssues().PrintTo(Cerr);
        return -1;
    }

    TExprContext::TFreezeGuard freezeGuard(ctx);

    NLog::YqlLoggerScope logger(new NLog::TTlsLogBackend(new TStreamLogBackend(&Cerr)));
    NLog::YqlLogger().SetComponentLevel(NLog::EComponent::Core, NLog::ELevel::DEBUG);
    NLog::YqlLogger().SetComponentLevel(NLog::EComponent::CoreEval, NLog::ELevel::DEBUG);
    NLog::YqlLogger().SetComponentLevel(NLog::EComponent::CorePeepHole, NLog::ELevel::DEBUG);

    auto server = CreateYqlServer(config,
                funcRegistry.Get(), udfIndex, ctx.NextUniqueId,
                userData,
                std::move(gatewaysConfig),
                sqlFlags,
                moduleResolver, udfResolver, fileStorage);
    server->Start();
    server->Wait();

    return 0;
}

int main(int argc, const char *argv[]) {
    try {
        if (argc > 1 && TString(argv[1]) == TStringBuf("ui")) {
            return RunUI(argc, argv);
        } else {
            return NYql::TYqlRunTool().Main(argc, argv);
        }
    }
    catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
