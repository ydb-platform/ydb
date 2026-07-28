#pragma once

#include "query_replay.h"

#include <util/generic/vector.h>
#include <util/generic/string.h>

TVector<std::pair<TString, TString>> GetJobFiles(TVector<TString> udfs);

#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/library/yql/utils/actor_log/log.h>

#include <library/cpp/json/json_value.h>

#include <memory>

class TQueryReplayRunner {
public:
    void Init(
        const TVector<TString>& udfFiles,
        ui32 actorSystemThreadsCount,
        bool antlr4ParserIsAmbiguityError,
        NActors::NLog::EPriority yqlLogPriority = NActors::NLog::EPriority::PRI_ERROR);

    THolder<TQueryReplayEvents::TEvCompileResponse> RunReplay(NJson::TJsonValue replayDetails);

    void Stop();

private:
    THolder<NActors::TActorSystem> ActorSystem;
    THolder<NKikimr::TAppData> AppData;
    TIntrusivePtr<NKikimr::NScheme::TKikimrTypeRegistry> TypeRegistry;
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FunctionRegistry;
    TIntrusivePtr<NKikimr::NKqp::TModuleResolverState> ModuleResolverState;
    NYql::IHTTPGateway::TPtr HttpGateway;
    std::unique_ptr<NYql::NLog::YqlLoggerScope> YqlLogger;
    bool Antlr4ParserIsAmbiguityError = false;
};
