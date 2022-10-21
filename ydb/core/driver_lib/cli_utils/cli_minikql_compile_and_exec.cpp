#include "cli.h"

#include <ydb/library/yql/ast/yql_ast.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/client/minikql_compile/yql_expr_minikql.h>


namespace NKikimr {
namespace NDriverClient {

using namespace NYql;

struct TCmdCompileAndExecMiniKQLConfig : public TCliCmdConfig {
    TCmdCompileAndExecMiniKQLConfig();

    void Parse(int argc, char **argv);

    TString PathToTextPgm;
    TString PathToBinPgm;

    TString PathToTextParams;
    TString PathToBinParams;
};

int CompileAndExecMiniKQL(TCommandConfig &cmdConf, int argc, char **argv) {
    Y_UNUSED(cmdConf);

#ifdef _win32_
    WSADATA dummy;
    WSAStartup(MAKEWORD(2, 2), &dummy);
#endif

    TCmdCompileAndExecMiniKQLConfig config;
    config.Parse(argc, argv);

    auto functionRegistry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
    TAlignedPagePoolCounters countersStub;
    NMiniKQL::TScopedAlloc alloc(__LOCATION__, countersStub);
    NMiniKQL::TTypeEnvironment TypeEnv(alloc);

    TAutoPtr<NMsgBusProxy::TBusRequest> request(new NMsgBusProxy::TBusRequest());

    auto* mkqlTx = request->Record.MutableTransaction()->MutableMiniKQLTransaction();
    if (config.PathToBinPgm) {
        TString pgmBin = TFileInput(config.PathToBinPgm).ReadAll();
        mkqlTx->MutableProgram()->SetBin(pgmBin);
    } else if (config.PathToTextPgm) {
        TString pgmText = TFileInput(config.PathToTextPgm).ReadAll();
        mkqlTx->MutableProgram()->SetText(pgmText);
    }

    if (config.PathToBinParams) {
        TString paramsBin = TFileInput(config.PathToBinParams).ReadAll();
        mkqlTx->MutableParams()->SetBin(paramsBin);
    } else if (config.PathToTextParams) {
        TString paramsText = TFileInput(config.PathToTextParams).ReadAll();
        mkqlTx->MutableParams()->SetText(paramsText);
    }

    mkqlTx->SetFlatMKQL(true);

    TAutoPtr<NBus::TBusMessage> reply;
    NBus::EMessageStatus msgStatus = config.SyncCall(request, reply);

    if (msgStatus != NBus::MESSAGE_OK) {
        Cerr << "Can't send request, msgstatus=" << msgStatus << ".\n";
        return 1;
    }
    const NKikimrClient::TResponse& response = static_cast<NMsgBusProxy::TBusResponse*>(reply.Get())->Record;

    auto txRes = response.GetMiniKQLCompileResults();

    if (txRes.ProgramCompileErrorsSize() > 0) {
        TIssues errors;
        NYql::IssuesFromMessage(txRes.GetProgramCompileErrors(), errors);
        Cerr << "Program compile errors:\n";
        if (config.PathToBinPgm) {
            errors.PrintTo(Cerr);
        } else {
            const TString pgmText = TFileInput(config.PathToTextPgm).ReadAll();
            errors.PrintWithProgramTo(Cerr, config.PathToTextPgm, pgmText);
        }
    }

    if (txRes.ParamsCompileErrorsSize() > 0) {
        TIssues errors;
        NYql::IssuesFromMessage(txRes.GetParamsCompileErrors(), errors);
        Cerr << "Params compile errors:\n";
        if (config.PathToBinParams) {
            errors.PrintTo(Cerr);
        } else {
            const TString paramsText = TFileInput(config.PathToTextParams).ReadAll();
            errors.PrintWithProgramTo(Cerr, config.PathToTextParams, paramsText);
        }
    }


    if (!response.GetDataShardErrors().empty()) {
        Cerr << "DataShard errors:\n " << response.GetDataShardErrors() << Endl;
    }
    if (!response.GetMiniKQLErrors().empty()) {
        Cerr << "MiniKQL errors:\n" << response.GetMiniKQLErrors() << Endl;
    }
    if (response.HasProxyErrorCode()) {
        Cerr << "Proxy status: " << static_cast<TEvTxUserProxy::TResultStatus::EStatus>(response.GetProxyErrorCode()) << Endl;
    }
    if (response.HasProxyErrors()) {
        Cerr << "Proxy errors: " << response.GetProxyErrors() << Endl;
    }
    if (response.UnresolvedKeysSize() > 0) {
        for (size_t i = 0, end = response.UnresolvedKeysSize(); i < end; ++i) {
            Cerr << response.GetUnresolvedKeys(i) << Endl;
        }
    }
    if ((ui32) NMsgBusProxy::MSTATUS_OK != response.GetStatus()) {
        Cerr << "Bad Response status: " << (NMsgBusProxy::EResponseStatus) response.GetStatus() << Endl;
        return 1;
    }

    auto execEngineStatus = static_cast<NMiniKQL::IEngineFlat::EStatus>(response.GetExecutionEngineResponseStatus());
    Cerr << "ExecutionEngineResponse status: " << (ui32) execEngineStatus << Endl;
    if (execEngineStatus != NMiniKQL::IEngineFlat::EStatus::Complete
        && execEngineStatus != NMiniKQL::IEngineFlat::EStatus::Aborted)
    {
        return 1;
    }
    const auto& evaluatedResp = response.GetExecutionEngineEvaluatedResponse();
    Cout << evaluatedResp.DebugString() << Endl;

    return 0;
}


TCmdCompileAndExecMiniKQLConfig::TCmdCompileAndExecMiniKQLConfig()
{}

void TCmdCompileAndExecMiniKQLConfig::Parse(int argc, char **argv) {
    using namespace NLastGetopt;

    TOpts opts = TOpts::Default();
    opts.AddLongOption('f', "pgm-txt", "path to file with text program").RequiredArgument("PATH").StoreResult(&PathToTextPgm);
    opts.AddLongOption('b', "pgm-bin", "path to file with serialized program").RequiredArgument("PATH").StoreResult(&PathToBinPgm);
    opts.AddLongOption("params-txt", "path to file with text program params").RequiredArgument("PATH").StoreResult(&PathToTextParams);
    opts.AddLongOption("params-bin", "path to file with serialized program params").RequiredArgument("PATH").StoreResult(&PathToBinParams);

    ConfigureBaseLastGetopt(opts);

    TOptsParseResult res(&opts, argc, argv);

    ConfigureMsgBusLastGetopt(res, argc, argv);

    if (!PathToBinPgm && !PathToTextPgm) {
        ythrow yexception() << "One of the ['pgm-txt', 'pgm-bin'] options must be set.";
    }
    if (PathToBinPgm && PathToTextPgm) {
        ythrow yexception() << "Only one of the ['pgm-txt', 'pgm-bin'] options must be set.";
    }
}

} // namespace NDriverClient
} // namespace NKikimr
