#include "gateway_spec.h"

#include <ydb/library/yql/tools/yqlrun/http/yql_server.h>

#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider_impl.h>
#include <ydb/library/yql/core/url_preprocessing/url_preprocessing.h>

#include <ydb/library/yql/sql/v1/format/sql_format.h>

#include <ydb/library/yql/providers/dq/provider/yql_dq_provider.h>
#include <ydb/library/yql/providers/pg/provider/yql_pg_provider.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_outproc_udf_resolver.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_udf_resolver_with_index.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_utils.h>
#include <ydb/library/yql/protos/yql_mount.pb.h>
#include <ydb/library/yql/core/yql_library_compiler.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>
#include <ydb/library/yql/core/file_storage/http_download/http_download.h>
#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/utils/log/tls_backend.h>
#include <ydb/library/yql/public/udf/udf_validate.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/comp_factory.h>

#include <ydb/core/util/pb.h>

#include <library/cpp/logger/stream.h>
#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/getopt/last_getopt.h>

#include <library/cpp/yson/public.h>
#include <library/cpp/yson/writer.h>

#include <google/protobuf/text_format.h>

#include <util/stream/file.h>
#include <util/system/user.h>
#include <util/folder/iterator.h>
#include <util/folder/dirut.h>
#include <util/string/join.h>
#include <util/string/builder.h>

#ifdef __unix__
#include <sys/resource.h>
#endif

const ui32 PRETTY_FLAGS = NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote |
        NYql::TAstPrintFlags::AdaptArbitraryContent;

class TMultiProgs {
public:
    TMultiProgs(NYql::TProgramFactory& factory, const TString& programFile, const TString& programText, size_t concurrentCount = 1) {
        Infos.reserve(concurrentCount);
        BaseProg = factory.Create(programFile, programText);
        if (concurrentCount) {
            factory.UnrepeatableRandom();
        }
        for (auto i = concurrentCount; i > 0; --i) {
            Infos.emplace_back(TProgInfo({factory.Create(programFile, programText), {}}));
        }
    }

    bool ParseYql() {
        bool result = BaseProg->ParseYql();
        for (auto& info: Infos) {
            info.Prog->ParseYql();
        }

        return result;
    }

    bool ParseSql(const NSQLTranslation::TTranslationSettings& settings) {
        bool result = BaseProg->ParseSql(settings);
        for (auto& info: Infos) {
            info.Prog->ParseSql(settings);
        }

        return result;
    }

    bool Compile(const TString& username) {
        bool result = BaseProg->Compile(username);
        for (auto& info: Infos) {
            info.Prog->Compile(username);
        }

        return result;
    }

    template<class T>
    bool CompareStreams(const TString& compareGoal, IOutputStream& out, const T& base, const T& concurrent) const {
        const auto baseSize = base.Size();
        const auto concurentSize = concurrent.Size();
        if (baseSize == concurentSize && memcmp(base.Data(), concurrent.Data(), baseSize) == 0) {
            return true;
        }
        out << "Difference " << compareGoal << " of cuncurrent mode is not the same as base run. Size base: " << baseSize <<
            ", size concurrent: " << concurentSize;
        if (concurentSize) {
            out << ", concurrent stream: " << Endl << concurrent.Data();
        } else {
            out << ", base stream: " << Endl << base.Data();
        }
        return false;
    }

    void PrintExprTo(IOutputStream& out) {
        TStringStream baseSS;
        auto baseAst = ConvertToAst(*BaseProg->ExprRoot(), BaseProg->ExprCtx(), NYql::TExprAnnotationFlags::None, true);
        baseAst.Root->PrettyPrintTo(baseSS, PRETTY_FLAGS);
        for (auto& info: Infos) {
            TStringStream ss;
            auto ast = ConvertToAst(*info.Prog->ExprRoot(), BaseProg->ExprCtx(), NYql::TExprAnnotationFlags::None, true);
            ast.Root->PrettyPrintTo(ss, PRETTY_FLAGS);
            if (!CompareStreams("expr representation", out, baseSS, ss)) {
                return;
            }
        }
        out << baseSS.Data();
    }

    void PrintErrorsTo(IOutputStream& out) const {
        TStringStream baseSS;
        BaseProg->PrintErrorsTo(baseSS);
        for (auto& info: Infos) {
            TStringStream ss;
            info.Prog->PrintErrorsTo(ss);
            if (!CompareStreams("error", out, baseSS, ss)) {
                return;
            }
        }
        out << baseSS.Data();
    }

    void PrintAstTo(IOutputStream& out) const {
        TStringStream baseSS;
        BaseProg->AstRoot()->PrettyPrintTo(out, PRETTY_FLAGS);
        for (auto& info: Infos) {
            TStringStream ss;
            info.Prog->AstRoot()->PrettyPrintTo(out, PRETTY_FLAGS);
            if (!CompareStreams("AST", out, baseSS, ss)) {
                return;
            }
        }
        out << baseSS.Data();
    }

    void SetProgressWriter(NYql::TOperationProgressWriter writer) {
        BaseProg->SetProgressWriter(writer);
        for (auto& info: Infos) {
            info.Prog->SetProgressWriter(writer);
        }
    }

    void SetValidateOptions(NKikimr::NUdf::EValidateMode validateMode) {
        BaseProg->SetValidateOptions(validateMode);
        for (auto& info: Infos) {
            info.Prog->SetValidateOptions(validateMode);
        }
    }

    void SetParametersYson(const TString& parameters) {
        BaseProg->SetParametersYson(parameters);
        for (auto& info : Infos) {
            info.Prog->SetParametersYson(parameters);
        }
    }

    void Print(IOutputStream* exprOut, IOutputStream* planOut) {
        bool cleanPlan = true;
        BaseProg->Print(exprOut, planOut, cleanPlan);
    }

    void ResultsOut(IOutputStream& out) {
        if (BaseProg->HasResults()) {
            BaseProg->ConfigureYsonResultFormat(NYson::EYsonFormat::Pretty);
            out << BaseProg->ResultsAsString();
        }
        // Multirun results are ignored
    }

    void DiscoveredDataOut(IOutputStream& out) {
        if (auto data = BaseProg->GetDiscoveredData()) {
            TStringInput in(*data);
            NYson::ReformatYsonStream(&in, &out, NYson::EYsonFormat::Pretty);
        }
    }

    void LineageOut(IOutputStream& out) {
        if (auto data = BaseProg->GetLineage()) {
            TStringInput in(*data);
            NYson::ReformatYsonStream(&in, &out, NYson::EYsonFormat::Pretty);
        }
    }

    NYql::TProgram::TStatus Run(const TString& username, IOutputStream* traceOut, IOutputStream* tracePlan, IOutputStream* exprOut, bool withTypes) {
        YQL_ENSURE(Infos.empty());
        return BaseProg->Run(username, traceOut, tracePlan, exprOut, withTypes);
    }

    NYql::TProgram::TStatus Optimize(const TString& username, IOutputStream* traceOut, IOutputStream* tracePlan, IOutputStream* exprOut, bool withTypes) {
        YQL_ENSURE(Infos.empty());
        return BaseProg->Optimize(username, traceOut, tracePlan, exprOut, withTypes);
    }

    NYql::TProgram::TStatus Validate(const TString& username, IOutputStream* exprOut, bool withTypes) {
        YQL_ENSURE(Infos.empty());
        return BaseProg->Validate(username, exprOut, withTypes);
    }

    NYql::TProgram::TStatus Discover(const TString& username) {
        YQL_ENSURE(Infos.empty());
        return BaseProg->Discover(username);
    }

    NYql::TProgram::TStatus Lineage(const TString& username, IOutputStream* traceOut, IOutputStream* exprOut, bool withTypes) {
        YQL_ENSURE(Infos.empty());
        return BaseProg->Lineage(username, traceOut, exprOut, withTypes);
    }

    NYql::TProgram::TStatus Peephole(const TString& username, IOutputStream* exprOut, bool withTypes) {
        YQL_ENSURE(Infos.empty());
        using namespace NYql;

        class TPeepHolePipelineConfigurator : public IPipelineConfigurator {
        public:
            TPeepHolePipelineConfigurator() = default;

            void AfterCreate(TTransformationPipeline* pipeline) const final {
                Y_UNUSED(pipeline);
            }

            void AfterTypeAnnotation(TTransformationPipeline* pipeline) const final {
                Y_UNUSED(pipeline);
            }

            void AfterOptimize(TTransformationPipeline* pipeline) const final {
                pipeline->Add(CreateYtWideFlowTransformer(nullptr), "WideFlow");
                pipeline->Add(MakePeepholeOptimization(pipeline->GetTypeAnnotationContext()), "PeepHole");
            }
        };

        TPeepHolePipelineConfigurator config;
        auto status = BaseProg->OptimizeWithConfig(username, config);
        if (exprOut && BaseProg->ExprRoot()) {
            auto ast = ConvertToAst(*BaseProg->ExprRoot(), BaseProg->ExprCtx(), withTypes ? TExprAnnotationFlags::Types : TExprAnnotationFlags::None, true);
            ui32 prettyFlags = TAstPrintFlags::ShortQuote;
            if (!withTypes) {
                prettyFlags |= TAstPrintFlags::PerLine;
            }
            ast.Root->PrettyPrintTo(*exprOut, prettyFlags);
        }
        return status;
    }

    NYql::TProgram::TStatus RunAsyncAndWait(const TString& username, IOutputStream* traceOut, IOutputStream* tracePlan, IOutputStream* exprOut,
            bool withTypes, bool& emulateOutputForMultirun) {
        NYql::TProgram::TStatus baseStatus = BaseProg->Run(username, traceOut, tracePlan, exprOut, withTypes);
        // switch this flag only after base run
        emulateOutputForMultirun = true;
        for (auto& info: Infos) {
            info.Future = info.Prog->RunAsync(username, nullptr, nullptr, nullptr, withTypes);
            YQL_ENSURE(info.Future.Initialized());
        }

        for (bool wasAsync = true; wasAsync;) {
            wasAsync = false;
            for (auto& info: Infos) {
                auto status = info.Future.GetValueSync();
                if (status == NYql::TProgram::TStatus::Async) {
                    wasAsync = true;
                    info.Future = info.Prog->ContinueAsync();
                } else if (status == NYql::TProgram::TStatus::Error) {
                    baseStatus = status;
                }
            }
        }
        return baseStatus;
    }

private:
    struct TProgInfo {
        NYql::TProgramPtr Prog;
        NYql::TProgram::TFutureStatus Future;
    };

    NYql::TProgramPtr BaseProg;
    TVector<TProgInfo> Infos;
};

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
    IUdfResolver::TPtr& udfResolver, NKikimr::NMiniKQL::IFunctionRegistry::TPtr funcRegistry, TUdfIndex::TPtr& udfIndex) {

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

int Main(int argc, const char *argv[])
{
    Y_UNUSED(NUdf::GetStaticSymbols());
    using namespace NLastGetopt;
    TOpts opts = TOpts::Default();
    TString programFile;
    TVector<TString> tablesMappingList;
    THashMap<TString, TString> tablesMapping;
    TVector<TString> filesMappingList;
    TUserDataTable filesMapping;
    TVector<TString> urlsMappingList;
    TString exprFile;
    TString resultFile;
    TString planFile;
    TString errFile;
    TString tmpDir;
    TVector<TString> udfsPaths;
    TString udfsDir;
    TString validateModeStr(NUdf::ValidateModeAsStr(NUdf::EValidateMode::Greedy));
    THashSet<TString> gatewayTypes;
    TString mountConfig;
    TString udfResolverPath;
    bool udfResolverFilterSyscalls = false;
    THashMap<TString, TString> clusterMapping;
    THashSet<TString> sqlFlags;
    clusterMapping["plato"] = YtProviderName;
    clusterMapping["pg_catalog"] = PgProviderName;
    clusterMapping["information_schema"] = PgProviderName;
    ui32 progsConcurrentCount = 0;
    TString paramsFile;
    ui16 syntaxVersion;
    ui64 memLimit;
    TString gatewaysCfgFile;
    TString fsCfgFile;

    opts.AddHelpOption();
    opts.AddLongOption('p', "program", "program file").StoreResult<TString>(&programFile);
    opts.AddLongOption('s', "sql", "program is SQL query").NoArgument();
    opts.AddLongOption("pg", "program has PG syntax").NoArgument();
    opts.AddLongOption('t', "table", "table@file").AppendTo(&tablesMappingList);
    opts.AddLongOption('C', "cluster", "set cluster to service mapping").RequiredArgument("name@service").Handler(new TStoreMappingFunctor(&clusterMapping));
    opts.AddLongOption("ndebug", "should be at first argument, do not show debug info in error output").NoArgument();
    opts.AddLongOption("parse-only", "exit after program has been parsed").NoArgument();
    opts.AddLongOption("print-ast", "print AST after loading").NoArgument();
    opts.AddLongOption("compile-only", "exit after program has been compiled").NoArgument();
    opts.AddLongOption("print-expr", "print rebuild AST before execution").NoArgument();
    opts.AddLongOption("with-types", "print types annotation").NoArgument();
    opts.AddLongOption("trace-opt", "print AST in the begin of each transformation").NoArgument();
    opts.AddLongOption("expr-file", "print AST to that file instead of stdout").StoreResult<TString>(&exprFile);
    opts.AddLongOption("print-result", "print program execution result to stdout").NoArgument();
    opts.AddLongOption("result-file", "print program execution result to file").StoreResult<TString>(&resultFile);
    opts.AddLongOption("plan-file", "print program plan to file").StoreResult<TString>(&planFile);
    opts.AddLongOption("err-file", "print validate/optimize/runtime errors to file").StoreResult<TString>(&errFile);
    opts.AddLongOption('P',"trace-plan", "print plan before execution").NoArgument();
    opts.AddLongOption('L', "show-log", "show logs").NoArgument();
    opts.AddLongOption('D', "discover", "discover tables in the program").NoArgument();
    opts.AddLongOption("validate", "exit after program has been validated").NoArgument();
    opts.AddLongOption("lineage", "exit after data lineage has been calculated").NoArgument();
    opts.AddLongOption('O',"optimize", "optimize expression").NoArgument();
    opts.AddLongOption('R',"run", "run expression using input/output tables").NoArgument();
    opts.AddLongOption("peephole", "perform peephole stage of expression using input/output tables").NoArgument();
    opts.AddLongOption('M',"multirun", "run expression in multi-evaluate (race) mode, as option set concurrent count").StoreResult(&progsConcurrentCount).DefaultValue(progsConcurrentCount);
    opts.AddLongOption('f', "file", "name@path").AppendTo(&filesMappingList);
    opts.AddLongOption('U', "url", "name@path").AppendTo(&urlsMappingList);
    opts.AddLongOption('m', "mounts", "Mount points config file.").StoreResult(&mountConfig);
    opts.AddLongOption("opt-collision", "provider optimize collision mode").NoArgument();
    opts.AddLongOption("keep-temp", "keep temporary tables").NoArgument();
    opts.AddLongOption("full-expr", "avoid buffering of expr/plan").NoArgument();
    opts.AddLongOption("show-progress", "report operation progress").NoArgument();
    opts.AddLongOption("tmp-dir", "directory for temporary tables").StoreResult<TString>(&tmpDir);
    opts.AddLongOption("reverse-mrkey", "reverse keys for Map/Reduce opeations").NoArgument();
    opts.AddLongOption('u', "udf", "Load shared library with UDF by given path").AppendTo(&udfsPaths);
    opts.AddLongOption("udfs-dir", "Load all shared libraries with UDFs found in given directory").StoreResult(&udfsDir);
    opts.AddLongOption("mem-info", "Print memory usage information").NoArgument();
    opts.AddLongOption("validate-mode", "validate udf mode, available values: " + NUdf::ValidateModeAvailables()).StoreResult<TString>(&validateModeStr).DefaultValue(validateModeStr);
    opts.AddLongOption('G', "gateways", "used gateways").SplitHandler(&gatewayTypes, ',').DefaultValue(YtProviderName);
    opts.AddLongOption("udf-resolver", "Path to udf-resolver").Optional().RequiredArgument("PATH").StoreResult(&udfResolverPath);
    opts.AddLongOption("udf-resolver-filter-syscalls", "Filter syscalls in udf resolver")
        .Optional()
        .NoArgument()
        .SetFlag(&udfResolverFilterSyscalls);
    opts.AddLongOption("scan-udfs", "Scan specified udfs with external udf-resolver to use static function registry").NoArgument();
    opts.AddLongOption("params-file", "Query parameters values in YSON format").StoreResult(&paramsFile);

    opts.AddLongOption("sql-flags", "SQL translator pragma flags").SplitHandler(&sqlFlags, ',');
    opts.AddLongOption("syntax-version", "SQL syntax version").StoreResult(&syntaxVersion).DefaultValue(1);
    opts.AddLongOption("ansi-lexer", "Use ansi lexer").NoArgument();
    opts.AddLongOption("assume-ydb-on-slash", "Assume YDB provider if cluster name starts with '/'").NoArgument();
    opts.AddLongOption("mem-limit", "Set memory limit in megabytes").StoreResult(&memLimit).DefaultValue(0);
    opts.AddLongOption("gateways-cfg", "gateways configuration file").Optional().RequiredArgument("FILE").StoreResult(&gatewaysCfgFile);
    opts.AddLongOption("fs-cfg", "fs configuration file").Optional().RequiredArgument("FILE").StoreResult(&fsCfgFile);
    opts.AddLongOption("test-format", "compare formatted query's AST with the original query's AST (only syntaxVersion=1 is supported)").NoArgument();
    opts.AddLongOption("show-kernels", "show all Arrow kernel families").NoArgument();

    opts.SetFreeArgsMax(0);
    TOptsParseResult res(&opts, argc, argv);
    auto builtins = CreateBuiltinRegistry();
    if (res.Has("show-kernels")) {
        auto families = builtins->GetAllKernelFamilies();
        Sort(families, [](const auto& x, const auto& y) { return x.first < y.first; });
        ui64 totalKernels = 0;
        for (const auto& f : families) {
            auto numKernels = f.second->GetAllKernels().size();
            Cout << f.first << ": " << numKernels << " kernels\n";
            totalKernels += numKernels;
        }

        Cout << "Total kernel families: " << families.size() << ", kernels: " << totalKernels << "\n";
        return 0;
    }

    const bool parseOnly = res.Has("parse-only");
    const bool compileOnly = res.Has("compile-only");
    const bool hasValidate = !parseOnly && !compileOnly;
    if (hasValidate && !gatewayTypes.contains(YtProviderName)) {
        Cerr << "At least one gateway from the list " << Join(",", YtProviderName).Quote() << " must be specified" << Endl;
        return 1;
    }

    for (auto& s: tablesMappingList) {
        TStringBuf tableName, filePath;
        TStringBuf(s).Split('@', tableName, filePath);
        if (tableName.empty() || filePath.empty()) {
            Cerr << "Incorrect table mapping, expected form table@file, e.g. yt.plato.Input@input.txt" << Endl;
            return 1;
        }
        tablesMapping[tableName] = filePath;
    }

    if (hasValidate) {
        for (auto& s : filesMappingList) {
            TStringBuf fileName, filePath;
            TStringBuf(s).Split('@', fileName, filePath);
            if (fileName.empty() || filePath.empty()) {
                Cerr << "Incorrect file mapping, expected form name@path, e.g. MyFile@file.txt" << Endl;
                return 1;
            }

            auto& file = filesMapping[TUserDataKey::File(GetDefaultFilePrefix() + fileName)];
            file.Type = EUserDataType::PATH;
            file.Data = filePath;
        }

        for (auto& s : urlsMappingList) {
            TStringBuf name, path;
            TStringBuf(s).Split('@', name, path);
            if (name.empty() || path.empty()) {
                Cerr << "Incorrect url mapping, expected form name@path, e.g. MyFile@sbr:123456" << Endl;
                return 1;
            }

            auto& block = filesMapping[TUserDataKey::File(GetDefaultFilePrefix() + name)];
            block.Type = EUserDataType::URL;
            block.Data = path;
        }
    }

    if (memLimit) {
#ifdef __unix__
        memLimit *= 1024 * 1024;

        struct rlimit rl;

        if (getrlimit(RLIMIT_AS, &rl)) {
            throw TSystemError() << "Cannot getrlimit(RLIMIT_AS)";
        }

        rl.rlim_cur = memLimit;
        if (setrlimit(RLIMIT_AS, &rl)) {
            throw TSystemError() << "Cannot setrlimit(RLIMIT_AS) to " << memLimit << " bytes";
        }
#else
        Cerr << "Memory limit can not be set on this platfrom" << Endl;
        return 1;
#endif
    }

    IOutputStream* errStream = &Cerr;
    THolder<TFixedBufferFileOutput> errFileHolder;
    if (!errFile.empty()) {
        errFileHolder.Reset(new TFixedBufferFileOutput(errFile));
        errStream = errFileHolder.Get();
    }

    TExprContext ctx;
    IModuleResolver::TPtr moduleResolver;
    if (!mountConfig.empty()) {
        TModulesTable modules;
        NYqlMountConfig::TMountConfig mount;
        Y_ABORT_UNLESS(NKikimr::ParsePBFromFile(mountConfig, &mount));
        FillUserDataTableFromFileSystem(mount, filesMapping);

        if (!CompileLibraries(filesMapping, ctx, modules)) {
            *errStream << "Errors on compile libraries:" << Endl;
            ctx.IssueManager.GetIssues().PrintTo(*errStream);
            return -1;
        }

        moduleResolver = std::make_shared<TModuleResolver>(std::move(modules), ctx.NextUniqueId, clusterMapping, sqlFlags, hasValidate);
    } else {
        if (!GetYqlDefaultModuleResolver(ctx, moduleResolver, clusterMapping, hasValidate)) {
            *errStream << "Errors loading default YQL libraries:" << Endl;
            ctx.IssueManager.GetIssues().PrintTo(*errStream);
            return -1;
        }
    }

    TExprContext::TFreezeGuard freezeGuard(ctx);

    TString programText;
    if (programFile.empty()) {
        Cerr << "Missing --program argument\n";
        return -1;
    }

    if (programFile == TStringBuf("-")) {
        programFile = TStringBuf("-stdin-");
        programText = Cin.ReadAll();
    } else {
        programText = TFileInput(programFile).ReadAll();
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

    THolder<TGatewaysConfig> gatewaysConfig;
    if (!gatewaysCfgFile.empty()) {
        gatewaysConfig = ParseProtoConfig<TGatewaysConfig>(gatewaysCfgFile);
        if (!gatewaysConfig) {
            return 1;
        }
        if (gatewaysConfig->HasSqlCore()) {
            sqlFlags.insert(gatewaysConfig->GetSqlCore().GetTranslationFlags().begin(), gatewaysConfig->GetSqlCore().GetTranslationFlags().end());
        }
    }

    TFileStoragePtr fileStorage;
    if (hasValidate) {
        NMiniKQL::FindUdfsInDir(udfsDir, &udfsPaths);

        fileStorage = WithAsync(CreateFileStorage(*fsConfig));
    }

    IUdfResolver::TPtr udfResolver;
    auto funcRegistry = CreateFunctionRegistry(&NYql::NBacktrace::KikimrBackTrace, CreateBuiltinRegistry(), true, udfsPaths);
    TUdfIndex::TPtr udfIndex;
    CommonInit(res, udfResolverPath, udfResolverFilterSyscalls, udfsPaths, fileStorage, udfResolver, funcRegistry, udfIndex);

    TAutoPtr<IThreadPool> ytExecutionQueue;
    TVector<TDataProviderInitializer> dataProvidersInit;

    auto dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
        NKikimr::NMiniKQL::GetYqlFactory(),
        NYql::GetPgFactory()
    });

    dataProvidersInit.push_back(GetDqDataProviderInitializer([](const TDqStatePtr&){
       return new TNullTransformer;
    }, {}, dqCompFactory, {}, fileStorage));
    dataProvidersInit.push_back(GetPgDataProviderInitializer());

    bool emulateOutputForMultirun = false;
    if (hasValidate) {
        if (gatewayTypes.contains(YtProviderName) || res.Has("opt-collision")) {
            auto yqlNativeServices = NFile::TYtFileServices::Make(funcRegistry.Get(), tablesMapping, fileStorage, tmpDir, res.Has("keep-temp"));
            auto ytNativeGateway = CreateYtFileGateway(yqlNativeServices, &emulateOutputForMultirun);
            dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytNativeGateway));
        }
    }

    if (hasValidate && res.Has("opt-collision")) {
        ExtProviderSpecific(funcRegistry.Get(), dataProvidersInit);
    }

    TProgramFactory factory(true, funcRegistry.Get(), ctx.NextUniqueId, dataProvidersInit, "yqlrun");
    factory.AddUserDataTable(filesMapping);
    factory.SetModules(moduleResolver);
    factory.SetFileStorage(fileStorage);
    if (gatewaysConfig && gatewaysConfig->HasFs()) {
        factory.SetUrlPreprocessing(new NYql::TUrlPreprocessing(*gatewaysConfig));
    }
    factory.SetUdfIndex(udfIndex, new TUdfIndexPackageSet());
    factory.SetUdfResolver(udfResolver);
    factory.SetGatewaysConfig(gatewaysConfig.Get());
    factory.EnableRangeComputeFor();

    auto program = MakeHolder<TMultiProgs>(factory, programFile, programText, progsConcurrentCount);
    if (res.Has("show-progress")) {
        program->SetProgressWriter([](const TOperationProgress& progress) {
            Cerr << "Operation: [" << progress.Category << "] " << progress.Id << ", state: " << progress.State << "\n";
        });
    }

    if (paramsFile) {
        TString parameters = TFileInput(paramsFile).ReadAll();
        program->SetParametersYson(parameters);
    }

    if (res.Has("sql") || res.Has("pg")) {
        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;
        settings.PgParser = res.Has("pg");
        settings.ClusterMapping = clusterMapping;
        settings.Flags = sqlFlags;
        settings.SyntaxVersion = syntaxVersion;
        settings.AnsiLexer = res.Has("ansi-lexer");
        settings.V0Behavior = NSQLTranslation::EV0Behavior::Report;
        settings.AssumeYdbOnClusterWithSlash = res.Has("assume-ydb-on-slash");
        if (res.Has("discover")) {
            settings.Mode = NSQLTranslation::ESqlMode::DISCOVERY;
        }
        if (!program->ParseSql(settings)) {
            program->PrintErrorsTo(*errStream);
            return 1;
        }
        if (res.Has("test-format") && syntaxVersion == 1) {
            TString formattedProgramText;
            NYql::TIssues issues;
            auto formatter = NSQLFormat::MakeSqlFormatter(settings);
            if (!formatter->Format(programText, formattedProgramText, issues)) {
                Cerr << "Format failed: ";
                issues.PrintTo(Cerr);
                return 1;
            }

            auto frmProgram = MakeHolder<TMultiProgs>(factory, "formatted SQL", formattedProgramText, progsConcurrentCount);
            if (!frmProgram->ParseSql(settings)) {
                frmProgram->PrintErrorsTo(*errStream);
                return 1;
            }

            TStringStream SrcQuery, FrmQuery;

            program->PrintAstTo(SrcQuery);
            frmProgram->PrintAstTo(FrmQuery);
            if (SrcQuery.Str() != FrmQuery.Str()) {
                Cerr << "source query's AST and formatted query's AST are not same\n";
                return 1;
            }
        }
    } else {
        if (!program->ParseYql()) {
            program->PrintErrorsTo(*errStream);
            return 1;
        }
    }

    if (res.Has("print-ast")) {
        program->PrintAstTo(Cout);
    }


    if (res.Has("parse-only"))
        return 0;

    const TString username = GetUsername();
    bool withTypes = res.Has("with-types");
    IOutputStream* traceOut = res.Has("trace-opt") ? &Cerr : nullptr;

    IOutputStream* exprOut = nullptr;
    THolder<TFixedBufferFileOutput> exprFileHolder;
    if (res.Has("print-expr")) {
        exprOut = &Cout;
    } else if (!exprFile.empty()) {
        exprFileHolder.Reset(new TFixedBufferFileOutput(exprFile));
        exprOut = exprFileHolder.Get();
    }

    IOutputStream* tracePlan = nullptr;
    THolder<TFixedBufferFileOutput> planFileHolder;
    if (res.Has("trace-plan")) {
        tracePlan = &Cout;
    }
    else if (!planFile.empty()) {
        planFileHolder.Reset(new TFixedBufferFileOutput(planFile));
        tracePlan = planFileHolder.Get();
    }

    if (res.Has("show-log")) {
        using namespace ::NYql::NLog;
        InitLogger(&Cerr);
        NLog::EComponentHelpers::ForEach([](NLog::EComponent c) {
            YqlLogger().SetComponentLevel(c, ELevel::DEBUG);
        });
    }
    if (res.Has("trace-opt")) {
        NLog::YqlLogger().SetComponentLevel(NLog::EComponent::Core, NLog::ELevel::TRACE);
        NLog::YqlLogger().SetComponentLevel(NLog::EComponent::CoreEval, NLog::ELevel::TRACE);
        NLog::YqlLogger().SetComponentLevel(NLog::EComponent::CorePeepHole, NLog::ELevel::TRACE);
    }

    program->SetValidateOptions(NUdf::ValidateModeByStr(validateModeStr));

    TProgram::TStatus status = TProgram::TStatus::Ok;
    const bool fullExpr = res.Has("full-expr");
    IOutputStream* fullTracePlan = fullExpr ? tracePlan : nullptr;
    IOutputStream* fullExprOut = fullExpr ? exprOut : nullptr;

    if (!program->Compile(username)) {
        program->PrintErrorsTo(*errStream);
        return 1;
    }

    if (res.Has("compile-only")) {
        if (res.Has("print-expr")) {
            program->PrintExprTo(Cout);
        }
        return 0;
    }

    if (res.Has("multirun")) {
        status = program->RunAsyncAndWait(username, traceOut, fullTracePlan, fullExprOut, withTypes, emulateOutputForMultirun);
    } else if (res.Has("peephole")) {
        status = program->Peephole(username, exprOut, withTypes);
    } else if (res.Has("run")) {
        status = program->Run(username, traceOut, fullTracePlan, fullExprOut, withTypes);
    } else if (res.Has("optimize")) {
        status = program->Optimize(username, traceOut, fullTracePlan, fullExprOut, withTypes);
    } else if (res.Has("validate")) {
        status = program->Validate(username, exprOut, withTypes);
    } else if (res.Has("discover")) {
        status = program->Discover(username);
    } else if (res.Has("lineage")) {
        status = program->Lineage(username, traceOut, exprOut, withTypes);
    }

    program->PrintErrorsTo(*errStream);
    if (status == TProgram::TStatus::Error) {
        return 1;
    }

    if (!fullExpr && !res.Has("peephole")) {
        program->Print(exprOut, tracePlan);
    }

    IOutputStream* resultOut = nullptr;
    THolder<TFixedBufferFileOutput> resultFileHolder;
    if (res.Has("print-result")) {
        resultOut = &Cout;
    } else if (!resultFile.empty()) {
        resultFileHolder.Reset(new TFixedBufferFileOutput(resultFile));
        resultOut = resultFileHolder.Get();
    }

    if (resultOut) {
        if (res.Has("discover")) {
            program->DiscoveredDataOut(*resultOut);
        } else if (res.Has("lineage")) {
            program->LineageOut(*resultOut);
        } else {
            program->ResultsOut(*resultOut);
        }
    }

    NLog::CleanupLogger();

    return 0;
}

int RunUI(int argc, const char* argv[])
{
    TVector<TString> udfsPaths;
    TString udfsDir;
    TString mountConfig;
    TVector<TString> filesMappingList;
    TString udfResolverPath;
    bool udfResolverFilterSyscalls = false;
    TString gatewaysCfgFile;
    TString fsCfgFile;

    THashMap<TString, TString> clusterMapping;
    clusterMapping["plato"] = YtProviderName;

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

    THolder<TGatewaysConfig> gatewaysConfig;
    if (!gatewaysCfgFile.empty()) {
        gatewaysConfig = ParseProtoConfig<TGatewaysConfig>(gatewaysCfgFile);
        if (!gatewaysConfig) {
            return -1;
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

    TExprContext ctx;
    IModuleResolver::TPtr moduleResolver;
    if (!mountConfig.empty()) {
        TModulesTable modules;
        NYqlMountConfig::TMountConfig mount;
        Y_ABORT_UNLESS(NKikimr::ParsePBFromFile(mountConfig, &mount));
        FillUserDataTableFromFileSystem(mount, userData);

        if (!CompileLibraries(userData, ctx, modules)) {
            Cerr << "Errors on compile libraries:" << Endl;
            ctx.IssueManager.GetIssues().PrintTo(Cerr);
            return -1;
        }

        moduleResolver = std::make_shared<TModuleResolver>(std::move(modules), ctx.NextUniqueId, clusterMapping, THashSet<TString>());
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
                moduleResolver, udfResolver, fileStorage);
    server->Start();
    server->Wait();

    return 0;
}

int main(int argc, const char *argv[]) {
    if (argc > 1 && TString(argv[1]) != TStringBuf("--ndebug")) {
        Cerr << "yqlrun ABI version: " << NKikimr::NUdf::CurrentAbiVersionStr() << Endl;
    }

    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    try {
        if (argc > 1 && TString(argv[1]) == TStringBuf("ui")) {
            return RunUI(argc, argv);
        } else {
            return Main(argc, argv);
        }
    }
    catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
