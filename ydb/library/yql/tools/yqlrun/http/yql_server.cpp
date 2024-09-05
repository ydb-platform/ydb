#include "yql_server.h"

#include <ydb/library/yql/tools/yqlrun/gateway_spec.h>

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_provider.h>
#include <ydb/library/yql/providers/pg/provider/yql_pg_provider.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider_impl.h>
#include <ydb/library/yql/core/url_preprocessing/url_preprocessing.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/comp_factory.h>
#include <ydb/library/yql/sql/v1/format/sql_format.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/log/tls_backend.h>
#include <ydb/library/yql/core/services/yql_out_transformers.h>
#include <ydb/library/yql/utils/utf8.h>

#include <library/cpp/logger/stream.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_builder.h>
#include <library/cpp/openssl/io/stream.h>
#include <library/cpp/charset/ci_string.h>
#include <library/cpp/yson/parser.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <google/protobuf/arena.h>

#include <util/folder/tempdir.h>
#include <util/system/fstat.h>
#include <util/system/tempfile.h>
#include <util/string/escape.h>

namespace NYql {
namespace NHttp {

namespace {

#ifdef _unix_
static volatile sig_atomic_t Terminated = 0;

void OnTerminate(int)
{
    Terminated = 1;
}
#endif

class TTempLogRedirector: private NLog::TScopedBackend<TStreamLogBackend> {
    using TBase = NLog::TScopedBackend<TStreamLogBackend>;

public:
    TTempLogRedirector(IOutputStream* redirectTo)
        : TBase(redirectTo)
    {
    }
};

class TLogLevelPromouter {
public:
    TLogLevelPromouter(bool promote) {
        PrevLevelCore = NLog::YqlLogger().GetComponentLevel(NLog::EComponent::Core);
        PrevLevelEval = NLog::YqlLogger().GetComponentLevel(NLog::EComponent::CoreEval);
        PrevLevelPeepHole = NLog::YqlLogger().GetComponentLevel(NLog::EComponent::CorePeepHole);

        if (promote) {
            NLog::YqlLogger().SetComponentLevel(NLog::EComponent::Core, NLog::ELevel::TRACE);
            NLog::YqlLogger().SetComponentLevel(NLog::EComponent::CoreEval, NLog::ELevel::TRACE);
            NLog::YqlLogger().SetComponentLevel(NLog::EComponent::CorePeepHole, NLog::ELevel::TRACE);
        }
    }

    ~TLogLevelPromouter() {
        NLog::YqlLogger().SetComponentLevel(NLog::EComponent::Core, PrevLevelCore);
        NLog::YqlLogger().SetComponentLevel(NLog::EComponent::CoreEval, PrevLevelEval);
        NLog::YqlLogger().SetComponentLevel(NLog::EComponent::CorePeepHole, PrevLevelPeepHole);
    }

private:
    NLog::ELevel PrevLevelCore;
    NLog::ELevel PrevLevelEval;
    NLog::ELevel PrevLevelPeepHole;
};


class TPeepHolePipelineConfigurator : public IPipelineConfigurator, TLogLevelPromouter {
public:
    TPeepHolePipelineConfigurator(bool promote)
        : TLogLevelPromouter(promote)
    {
    }

    void AfterCreate(TTransformationPipeline* pipeline) const final {
        Y_UNUSED(pipeline);
    }

    void AfterTypeAnnotation(TTransformationPipeline* pipeline) const final {
        pipeline->Add(TExprLogTransformer::Sync("OptimizedExpr", NLog::EComponent::Core, NLog::ELevel::TRACE),
            "OptTrace", TIssuesIds::CORE, "OptTrace");
    }

    void AfterOptimize(TTransformationPipeline* pipeline) const final {
        pipeline->Add(CreateYtWideFlowTransformer(nullptr), "WideFlow");
        pipeline->Add(MakePeepholeOptimization(pipeline->GetTypeAnnotationContext()), "PeepHole");
    }
};

class TOptPipelineConfigurator : public IPipelineConfigurator, TLogLevelPromouter {
public:
    TOptPipelineConfigurator(TProgramPtr prg, IOutputStream* stream)
        : TLogLevelPromouter(!!stream)
        , Program(std::move(prg))
        , Stream(stream)
    {
    }

    void AfterCreate(TTransformationPipeline* pipeline) const final {
        Y_UNUSED(pipeline);
    }

    void AfterTypeAnnotation(TTransformationPipeline* pipeline) const final {
        if (Stream) {
            pipeline->Add(TExprLogTransformer::Sync("OptimizedExpr", NLog::EComponent::Core, NLog::ELevel::TRACE),
                "OptTrace", TIssuesIds::CORE, "OptTrace");
        }
    }

    void AfterOptimize(TTransformationPipeline* pipeline) const final {
        pipeline->Add(TPlanOutputTransformer::Sync(Stream, Program->GetPlanBuilder(), Program->GetOutputFormat()), "PlanOutput");
    }
private:
    TProgramPtr Program;
    IOutputStream* Stream;
};

NSQLTranslation::TTranslationSettings GetTranslationSettings(const THolder<TGatewaysConfig>& gatewaysConfig) {
    static const THashMap<TString, TString> clusters = {
        { "plato", TString(YtProviderName) },
        { "plato_rtmr", TString(RtmrProviderName) },
        { "pg_catalog", TString(PgProviderName) },
        { "information_schema", TString(PgProviderName) },
    };

    NSQLTranslation::TTranslationSettings settings;
    settings.ClusterMapping = clusters;
    settings.SyntaxVersion = 1;
    settings.InferSyntaxVersion = true;
    settings.V0Behavior = NSQLTranslation::EV0Behavior::Report;
    if (gatewaysConfig && gatewaysConfig->HasSqlCore()) {
        settings.Flags.insert(gatewaysConfig->GetSqlCore().GetTranslationFlags().begin(), gatewaysConfig->GetSqlCore().GetTranslationFlags().end());
    }
    return settings;
}

void SetupProgram(TProgram& prg, const TString& program) {
    Y_UNUSED(program);
    prg.SetValidateOptions(NKikimr::NUdf::EValidateMode::Greedy);
    prg.EnableResultPosition();
}

struct TTableFileHolder {
    TTempFile Main;
    TTempFile Attr;

    TTableFileHolder(const TString& path)
        : Main(path)
        , Attr(path + ".attr")
    {}
};

TProgramPtr MakeFileProgram(const TString& program, TYqlServer& yqlServer,
    const THashMap<TString, TString>& tables, const THashMap<std::pair<TString, TString>, 
    TVector<std::pair<TString, TString>>>& rtmrTableAttributes, const TString& tmpDir) {

    TVector<TDataProviderInitializer> dataProvidersInit;

    auto ytNativeServices = NFile::TYtFileServices::Make(yqlServer.FunctionRegistry, tables, yqlServer.FileStorage, tmpDir);
    auto ytNativeGateway = CreateYtFileGateway(ytNativeServices);

    auto dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
        NKikimr::NMiniKQL::GetYqlFactory(),
        GetPgFactory()
    });

    dataProvidersInit.push_back(GetDqDataProviderInitializer([](const TDqStatePtr&){
       return new TNullTransformer;
    }, {}, dqCompFactory, {}, yqlServer.FileStorage));
    dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytNativeGateway));
    dataProvidersInit.push_back(GetPgDataProviderInitializer());

    ExtProviderSpecific(yqlServer.FunctionRegistry, dataProvidersInit, rtmrTableAttributes);

    TProgramFactory programFactory(
        true,
        yqlServer.FunctionRegistry,
        yqlServer.NextUniqueId,
        dataProvidersInit,
        "yqlrun");

    programFactory.AddUserDataTable(yqlServer.FilesMapping);
    programFactory.SetModules(yqlServer.Modules);
    programFactory.SetUdfResolver(yqlServer.UdfResolver);
    programFactory.SetUdfIndex(yqlServer.UdfIndex, new TUdfIndexPackageSet());
    programFactory.SetFileStorage(yqlServer.FileStorage);
    programFactory.EnableRangeComputeFor();
    programFactory.SetGatewaysConfig(yqlServer.GatewaysConfig.Get());
    if (yqlServer.GatewaysConfig && yqlServer.GatewaysConfig->HasFs()) {
        programFactory.SetUrlPreprocessing(new NYql::TUrlPreprocessing(*yqlServer.GatewaysConfig));
    }

    auto prg = programFactory.Create("-stdin-", program);
    SetupProgram(*prg, program);
    return prg;
}

TProgramPtr MakeFileProgram(const TString& program, const TString& input, const TString& attr,
    TAutoPtr<TTableFileHolder>& inputFile, TTempFile& outputFile, TYqlServer& yqlServer, const TString& tmpDir) {
    TString cluster = "plato";

    THashMap<TString, TString> tables;
    inputFile.Reset(new TTableFileHolder(MakeTempName()));
    TFile mainFile(inputFile->Main.Name(), CreateAlways | RdWr);
    TFile attrFile(inputFile->Attr.Name(), CreateAlways | RdWr);
    mainFile.Write(input.data(), input.size());
    attrFile.Write(attr.data(), attr.size());
    mainFile.Close();
    attrFile.Close();
    tables[TString(YtProviderName).append('.').append(cluster).append(TStringBuf(".Input"))] = inputFile->Main.Name();
    tables[TString(YtProviderName).append('.').append(cluster).append(TStringBuf(".Output"))] = outputFile.Name();
    NFs::Remove(outputFile.Name());

    THashMap<std::pair<TString, TString>, TVector<std::pair<TString, TString>>> rtmrTableAttributes;
    auto node = NYT::NodeFromYsonString(attr);
    if (node.IsMap() && node.HasKey(YqlRowSpecAttribute)) {
        rtmrTableAttributes[std::make_pair("plato_rtmr", "Input")] = {{"_yql_row_spec", NYT::NodeToYsonString(node[YqlRowSpecAttribute])}};
    }

    return MakeFileProgram(program, yqlServer, tables, rtmrTableAttributes, tmpDir);
}

YQL_ACTION(Paste)
    void Perform(const TString& program, const TString& input, const TString& attr, ui32 options, const TString& parameters) {
        Y_UNUSED(input);
        Y_UNUSED(attr);
        Y_UNUSED(options);
        Y_UNUSED(parameters);

        const static TString pasteHost(TStringBuf("paste.yandex-team.ru"));

        TSocket s(TNetworkAddress(pasteHost, 443));
        TSocketOutput so(s);
        TSocketInput si(s);
        TOpenSslClientIO ssl(&si, &so);

        {
            THttpOutput output(&ssl);

            TStringBuf data = "syntax=yql&text=";
            TString quotedProgram(program);
            Quote(quotedProgram);

            output << TStringBuf("POST / HTTP/1.1\r\n")
                   << TStringBuf("Host: ") << pasteHost << TStringBuf("\r\n")
                   << TStringBuf("Content-Type: application/x-www-form-urlencoded\r\n")
                   << TStringBuf("Content-Length: ") << (data.size() + quotedProgram.size())
                   << TStringBuf("\r\n\r\n")
                   <<  data << quotedProgram
                   << TStringBuf("\r\n");

            output.Finish();
        }

        {
            THttpInput input(&ssl);
            unsigned httpCode = ParseHttpRetCode(input.FirstLine());
            Cout << "return code: " << httpCode << Endl;

            for (auto i = input.Headers().Begin(), e = input.Headers().End(); i != e; ++i) {
                if (0 == TCiString::compare(i->Name(), TStringBuf("location"))) {
                    Writer.Write(TStringBuf("location"), i->Value());
                    return;
                }
            }
        }

        ythrow yexception() << "Unknown redirect location";
    }
};

YQL_ACTION(Format)
    void Perform(const TString& program, const TString& input, const TString& attr, ui32 options, const TString& parameters) {
        Y_UNUSED(input);
        Y_UNUSED(attr);
        Y_UNUSED(options);
        Y_UNUSED(parameters);
        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;
        auto formatter = NSQLFormat::MakeSqlFormatter(settings);
        TString frm_query;
        TString error;
        NYql::TIssues issues;
        if (!formatter->Format(program, frm_query, issues)) {
            WriteStatus(false, issues);
        } else {
            Writer.Write(TStringBuf("sql"), frm_query);
        }
    }
};

///////////////////////////////////////////////////////////////////////////////
// parse action
///////////////////////////////////////////////////////////////////////////////
YQL_ACTION(Parse)
    void Perform(const TString& program, const TString& input, const TString& attr, ui32 options, const TString& parameters) {
        Y_UNUSED(input);
        Y_UNUSED(attr);
        Y_UNUSED(parameters);
        TTempDir tmpDir;
        TProgramPtr prg = MakeFileProgram(program, YqlServer, {}, {}, tmpDir.Name());

        bool parsed = (options & TYqlAction::YqlProgram)
                ? prg->ParseYql()
                : prg->ParseSql(GetTranslationSettings(YqlServer.GatewaysConfig));

        if (parsed) {
            ui32 prettyFlg = TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote;
            Writer.Write(TStringBuf("expr"), prg->AstRoot()->ToString(prettyFlg));

            if (options & EOptions::PrintAst) {
                Writer.Write(TStringBuf("ast"));
                WriteAstTree(prg->AstRoot());
            }
        }

        WriteStatus(parsed, prg->Issues());
    }
};

///////////////////////////////////////////////////////////////////////////////
// compile action
///////////////////////////////////////////////////////////////////////////////
YQL_ACTION(Compile)
    void Perform(const TString& program, const TString& input, const TString& attr, ui32 options, const TString& parameters) {
        Y_UNUSED(input);
        Y_UNUSED(attr);
        TTempDir tmpDir;
        TProgramPtr prg = MakeFileProgram(program, YqlServer, {}, {}, tmpDir.Name());
        prg->SetParametersYson(parameters);

        bool noError = (options & TYqlAction::YqlProgram) ? prg->ParseYql() : prg->ParseSql(GetTranslationSettings(YqlServer.GatewaysConfig));
        noError = noError && prg->Compile(GetUsername());

        if (options & (EOptions::PrintAst | EOptions::PrintExpr)) {
            if (prg->ExprRoot()) {
                auto ast = ConvertToAst(*prg->ExprRoot(), prg->ExprCtx(), TExprAnnotationFlags::None, true);

                if (options & EOptions::PrintAst) {
                    Writer.Write(TStringBuf("ast"));
                    WriteAstTree(ast.Root);
                }

                if (options & EOptions::PrintExpr) {
                    ui32 prettyFlg = TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote;
                    Writer.Write(TStringBuf("expr"), ast.Root->ToString(prettyFlg));
                }
            }
        }

        WriteStatus(noError, prg->Issues());
    }
};

///////////////////////////////////////////////////////////////////////////////
// optimize, validate and peephole actions
///////////////////////////////////////////////////////////////////////////////
YQL_ACTION(OptimizeOrValidateFile)
    void Perform(const TString& program, const TString& input, const TString& attr, ui32 options, const TString& parameters) {
        TAutoPtr<TTableFileHolder> inputFile;
        TTempFile outputFile(MakeTempName());
        TTempFile outputFileAttr(outputFile.Name() + ".attr");
        TTempDir tmpDir;
        TProgramPtr prg = MakeFileProgram(program, input, attr, inputFile, outputFile, YqlServer, tmpDir.Name());

        bool noError = (options & TYqlAction::YqlProgram) ? prg->ParseYql() : prg->ParseSql(GetTranslationSettings(YqlServer.GatewaysConfig));

        prg->SetParametersYson(parameters);
        prg->SetDiagnosticFormat(NYson::EYsonFormat::Pretty);
        THolder<TStringStream> traceOut;
        THolder<TTempLogRedirector> logRedirector;
        if (options & EOptions::PrintTraceOpt) {
            traceOut.Reset(new TStringStream);
            logRedirector.Reset(new TTempLogRedirector(traceOut.Get()));
        }

        noError = noError && prg->Compile(GetUsername());
        if (noError) {
            TProgram::TStatus status = TProgram::TStatus::Error;
            auto name = TStringBuf(Req.RD.ScriptName());
            if (name.Contains(TStringBuf("/optimize"))) {
                auto config = TOptPipelineConfigurator(prg, traceOut.Get());
                status = prg->OptimizeWithConfig(GetUsername(), config);
            } else if (name.Contains(TStringBuf("/validate"))) {
                status = prg->Validate(GetUsername());
            } else if (name.Contains(TStringBuf("/peephole"))) {
                auto config = TPeepHolePipelineConfigurator(options & EOptions::PrintTraceOpt);
                status = prg->OptimizeWithConfig(GetUsername(), config);
            }
            noError = status == TProgram::TStatus::Ok;
        }

        if (options & (EOptions::PrintAst | EOptions::PrintExpr)) {
            if (prg->ExprRoot()) {
                auto ast = ConvertToAst(*prg->ExprRoot(), prg->ExprCtx(), TExprAnnotationFlags::None, true);

                if (options & EOptions::PrintAst) {
                    Writer.Write(TStringBuf("ast"));
                    WriteAstTree(ast.Root);
                }

                if (options & EOptions::PrintExpr) {
                    ui32 prettyFlg = TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote;
                    Writer.Write(TStringBuf("expr"), ast.Root->ToString(prettyFlg));
                }
            }
        }

        auto diagnostics = prg->GetDiagnostics();
        if (diagnostics) {
            Cerr << *diagnostics;
        }

        if (!!traceOut && !traceOut->Str().empty()) {
            if (diagnostics) {
                traceOut->Write(*diagnostics);
            }

            Writer.Write(TStringBuf("opttrace"), traceOut->Str());
        }
        if (options & TYqlAction::WithFinalIssues) {
            prg->FinalizeIssues();
        }
        WriteStatus(noError, prg->Issues());
    }
};


///////////////////////////////////////////////////////////////////////////////
// run actions
///////////////////////////////////////////////////////////////////////////////
YQL_ACTION(FileRun)
    void Perform(const TString& program, const TString& input, const TString& attr, ui32 options, const TString& parameters) {
        auto name = TStringBuf(Req.RD.ScriptName());
        TAutoPtr<TTableFileHolder> inputFile;
        TTempFile outputFile(MakeTempName());
        TTempFile outputFileAttr(outputFile.Name() + ".attr");
        TTempDir tmpDir;
        TProgramPtr prg = MakeFileProgram(program, input, attr, inputFile, outputFile, YqlServer, tmpDir.Name());

        bool noError = (options & TYqlAction::YqlProgram) ? prg->ParseYql() : prg->ParseSql(GetTranslationSettings(YqlServer.GatewaysConfig));

        prg->SetDiagnosticFormat(NYson::EYsonFormat::Pretty);
        prg->SetParametersYson(parameters);
        THolder<TStringStream> traceOut;
        THolder<TTempLogRedirector> logRedirector;
        if (options & EOptions::PrintTraceOpt) {
            traceOut.Reset(new TStringStream);
            logRedirector.Reset(new TTempLogRedirector(traceOut.Get()));
        }

        noError = noError && prg->Compile(GetUsername());
        TProgram::TStatus status = TProgram::TStatus::Error;
        if (noError) {
            auto config = TOptPipelineConfigurator(prg, traceOut.Get());
            if (name.Contains(TStringBuf("/lineage"))) {
                status = prg->LineageWithConfig(GetUsername(), config);
            } else {
                status = prg->RunWithConfig(GetUsername(), config);
            }
        }
        if (options & (EOptions::PrintAst | EOptions::PrintExpr)) {
            if (prg->ExprRoot()) {
                auto ast = ConvertToAst(*prg->ExprRoot(), prg->ExprCtx(), TExprAnnotationFlags::None, true);

                if (options & EOptions::PrintAst) {
                    Writer.Write(TStringBuf("ast"));
                    WriteAstTree(ast.Root);
                }

                if (options & EOptions::PrintExpr) {
                    ui32 prettyFlg = TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote;
                    Writer.Write(TStringBuf("expr"), ast.Root->ToString(prettyFlg));
                }
            }
        }

        auto diagnostics = prg->GetDiagnostics();
        if (diagnostics) {
            Cerr << *diagnostics;
        }

        if (!!traceOut && !traceOut->Str().empty()) {
            if (diagnostics) {
                traceOut->Write(*diagnostics);
            }

            Writer.Write(TStringBuf("opttrace"), traceOut->Str());
        }

        if (options & TYqlAction::WithFinalIssues) {
            prg->FinalizeIssues();
        }
        WriteStatus(status != TProgram::TStatus::Error, prg->Issues());

        if (status != TProgram::TStatus::Error) {
            // write output
            Writer.Write(TStringBuf("output"));
            Writer.OpenMap();
            if (TFileStat(outputFile.Name()).IsFile()) {
                TFileInput fileInput(outputFile.Name());

                NYT::TNode list = NYT::TNode::CreateList();
                NYT::TNodeBuilder builder(&list);
                NYson::TYsonParser parser(&builder, &fileInput, ::NYson::EYsonType::ListFragment);
                parser.Parse();

                std::set<TString> headers;
                for (auto& row: list.AsList()) {
                    for (auto& val: row.AsMap()) {
                        headers.insert(val.first);
                    }
                }
                { // headers
                    Writer.Write(TStringBuf("headers"));
                    Writer.OpenArray();
                    for (const auto& header : headers) {
                        Writer.Write(header);
                    }
                    Writer.CloseArray();
                }

                { // rows
                    Writer.Write(TStringBuf("rows"));
                    Writer.OpenArray();
                    for (auto& row: list.AsList()) {
                        Writer.OpenArray();
                        for (const auto& header : headers) {
                            if (auto p = row.AsMap().FindPtr(header)) {
                                if (p->IsString()) {
                                    const auto& str = p->AsString();
                                    Writer.Write(IsUtf8(str) ? str : EscapeC(str));
                                } else {
                                    Writer.Write(NYT::NodeToYsonString(*p, NYson::EYsonFormat::Text));
                                }
                            } else {
                                Writer.Write(TString());
                            }
                        }
                        Writer.CloseArray();
                    }
                    Writer.CloseArray();
                }
            }

            Writer.CloseMap();
        }

        if (name.Contains(TStringBuf("/lineage"))) {
            if (auto data = prg->GetLineage()) {
                TString str;
                TStringOutput out(str);
                TStringInput in(*data);
                NYson::ReformatYsonStream(&in, &out, NYson::EYsonFormat::Pretty);
                Writer.Write(TStringBuf("results"), str);
            }
        } else {
            Writer.Write(TStringBuf("results"), prg->ResultsAsString());
        }
    }
};

} // namespace

void TYqlServer::Start()
{
#ifdef _unix_
    ShutdownOn(SIGINT);
    ShutdownOn(SIGTERM);
#endif

    bool started = HttpServer.Start();
    if (!started) {
        ythrow yexception() << "YqlServer not started. Error: "
                            << HttpServer.GetErrorCode()
                            << ": " << HttpServer.GetError();
    }
}

void TYqlServer::ShutdownOn(int signal)
{
#ifdef _unix_
    struct sigaction sa = {};
    sa.sa_handler = OnTerminate;
    sigfillset(&sa.sa_mask); // block every signal during the handler

    if (sigaction(signal, &sa, nullptr) < 0) {
        ythrow yexception() << "Error: cannot handle signal " << signal;
    }
#else
    Y_UNUSED(signal);
#endif
}

void TYqlServer::Wait()
{
#ifdef _unix_
    while (!Terminated) {
        sleep(1);
    }
#else
    HttpServer.Wait();
#endif
}

TAutoPtr<TYqlServer> CreateYqlServer(
        TServerConfig config,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TUdfIndex::TPtr udfIndex,
        ui64 nextUniqueId,
        TUserDataTable filesMapping,
        THolder<TGatewaysConfig>&& gatewaysConfig,
        IModuleResolver::TPtr modules,
        IUdfResolver::TPtr udfResolver,
        TFileStoragePtr fileStorage)
{
    TAutoPtr<TYqlServer> server = new TYqlServer(
        config, functionRegistry, udfIndex, nextUniqueId,
        std::move(filesMapping), std::move(gatewaysConfig), modules, udfResolver, fileStorage);

    server->RegisterAction<TYqlActionPaste>("/api/yql/paste");
    server->RegisterAction<TYqlActionParse>("/api/yql/parse");
    server->RegisterAction<TYqlActionCompile>("/api/yql/compile");
    server->RegisterAction<TYqlActionFormat>("/api/yql/format");
    server->RegisterAction<TYqlActionOptimizeOrValidateFile>("/api/yql/validate");
    server->RegisterAction<TYqlActionOptimizeOrValidateFile>("/api/yql/optimize");
    server->RegisterAction<TYqlActionOptimizeOrValidateFile>("/api/yql/peephole");

    server->RegisterServlet("/js/yql-functions.js", new TYqlFunctoinsServlet());

    server->RegisterAction<TYqlActionFileRun>("/api/yql/lineage");
    server->RegisterAction<TYqlActionFileRun>("/api/yql/run");

    server->RegisterServlet("/",
            new TAssetsServlet("/", config.GetAssetsPath(), "file-index.html"));

    return server;
}

} // namspace NHttp
} // namspace NYql
