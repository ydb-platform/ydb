#include "replay_runner.h"

#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <util/folder/path.h>
#include <util/string/split.h>

TVector<std::pair<TString, TString>> GetJobFiles(TVector<TString> udfs) {
    TVector<std::pair<TString, TString>> result;

    for (const TString& udf : udfs) {
        TVector<TString> splitResult;
        Split(udf.data(), "/", splitResult);
        while (!splitResult.empty() && splitResult.back().empty()) {
            splitResult.pop_back();
        }

        Y_ENSURE(!splitResult.empty());

        result.push_back(std::make_pair(udf, splitResult.back()));
    }

    return result;
}

void TQueryReplayRunner::Init(
    const TVector<TString>& udfFiles,
    ui32 actorSystemThreadsCount,
    bool antlr4ParserIsAmbiguityError,
    NActors::NLog::EPriority yqlLogPriority)
{
    Antlr4ParserIsAmbiguityError = antlr4ParserIsAmbiguityError;
    YqlLogger = std::make_unique<NYql::NLog::YqlLoggerScope>(&Cerr);
    NYql::NDq::SetYqlLogLevels(yqlLogPriority);

    TypeRegistry.Reset(new NKikimr::NScheme::TKikimrTypeRegistry());
    FunctionRegistry.Reset(NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone());
    NKikimr::NMiniKQL::FillStaticModules(*FunctionRegistry);
    NKikimr::NMiniKQL::TUdfModuleRemappings remappings;
    THashSet<TString> usedUdfPaths;

    for (const auto& [realPath, udfPath] : GetJobFiles(udfFiles)) {
        if (usedUdfPaths.insert(udfPath).second) {
            if (NFs::Exists(udfPath)) {
                FunctionRegistry->LoadUdfs(udfPath, remappings, 0);
                continue;
            }

            if (NFs::Exists(realPath)) {
                FunctionRegistry->LoadUdfs(realPath, remappings, 0);
                continue;
            }
        }
    }

    AppData.Reset(new NKikimr::TAppData(0, 0, 0, 0, {}, TypeRegistry.Get(), FunctionRegistry.Get(), nullptr, nullptr));
    AppData->Counters = MakeIntrusive<NMonitoring::TDynamicCounters>(new NMonitoring::TDynamicCounters());
    auto setup = BuildActorSystemSetup(actorSystemThreadsCount);
    ActorSystem.Reset(new NActors::TActorSystem(setup, AppData.Get()));
    ActorSystem->Start();
    ActorSystem->Register(NKikimr::NKqp::CreateKqpResourceManagerActor({}, nullptr));
    ModuleResolverState = MakeIntrusive<NKikimr::NKqp::TModuleResolverState>();
    HttpGateway = NYql::IHTTPGateway::Make();
    Y_ABORT_UNLESS(GetYqlDefaultModuleResolver(ModuleResolverState->ExprCtx, ModuleResolverState->ModuleResolver));
}

THolder<TQueryReplayEvents::TEvCompileResponse> TQueryReplayRunner::RunReplay(NJson::TJsonValue replayDetails) {
    const TString queryType = replayDetails["query_type"].GetStringSafe();
    if (queryType == "QUERY_TYPE_AST_SCAN" || queryType == "QUERY_TYPE_AST_DML") {
        return nullptr;
    }

    if (queryType == "QUERY_TYPE_SQL_GENERIC_SCRIPT") {
        return nullptr;
    }

    auto compileActorId = ActorSystem->Register(
        CreateQueryCompiler(ModuleResolverState, FunctionRegistry.Get(), HttpGateway, Antlr4ParserIsAmbiguityError));

    auto future = ActorSystem->Ask<TQueryReplayEvents::TEvCompileResponse>(
        compileActorId,
        THolder(new TQueryReplayEvents::TEvCompileRequest(std::move(replayDetails))),
        TDuration::Seconds(600));

    return THolder(future.ExtractValueSync().Release());
}

void TQueryReplayRunner::Stop() {
    if (ActorSystem) {
        ActorSystem->Stop();
    }
}
