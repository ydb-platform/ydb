#pragma once

#include "query_replay.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/scheme/scheme_type_registry.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>

#include <ydb/library/yql/core/services/mounts/yql_mounts.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/json/json_value.h>

struct TQueryReplayConfig {
    TString Cluster;
    TString SrcPath;
    TString DstPath;
    ui32 ActorSystemThreadsCount = 5;
    TVector<TString> UdfFiles;
    TString QueryFile;
    NActors::NLog::EPriority YqlLogLevel = NActors::NLog::EPriority::PRI_ERROR;

    void ParseConfig(int argc, const char** argv);
};

namespace NYql {
    class IHTTPGateway;
}

using namespace NActors;

THolder<TActorSystemSetup> BuildActorSystemSetup(ui32 threads, ui32 pools = 1);

struct TQueryReplayEvents {
    enum EEv {
        EvCompileRequest = EventSpaceBegin(NActors::TEvents::ES_USERSPACE + 1),
        EvCompileResponse,
    };

    enum TCheckQueryPlanStatus {
        Success,
        CompileError,
        CompileTimeout,
        TableMissing,
        ExtraReadingOldEngine,
        ExtraReadingNewEngine,
        ReadTypesMismatch,
        ReadLimitsMismatch,
        ReadColumnsMismatch,
        ExtraWriting,
        WriteColumnsMismatch,
        UncategorizedPlanMismatch,
        MissingTableMetadata,
        Unspecified,
    };

    struct TEvCompileRequest: public NActors::TEventLocal<TEvCompileRequest, EvCompileRequest> {
        NJson::TJsonValue ReplayDetails;

        TEvCompileRequest(NJson::TJsonValue replayDetails)
            : ReplayDetails(std::move(replayDetails))
        {
        }
    };

    struct TEvCompileResponse: public NActors::TEventLocal<TEvCompileResponse, EvCompileResponse> {

        bool Success;
        TCheckQueryPlanStatus Status = Unspecified;
        TString Message;
        TString Plan;

        TEvCompileResponse(bool success)
            : Success(success)
        {
        }
    };
};

NActors::IActor* CreateQueryCompiler(TIntrusivePtr<NKikimr::NKqp::TModuleResolverState> moduleResolverState,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, std::shared_ptr<NYql::IHTTPGateway> httpGateway);
