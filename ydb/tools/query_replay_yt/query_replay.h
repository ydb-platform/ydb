#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/scheme/scheme_type_registry.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>

#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/json/json_value.h>

struct TQueryReplayConfig {
    TString Cluster;
    TString SrcPath;
    TString DstPath;
    TString CoreTablePath;
    ui32 ActorSystemThreadsCount = 5;
    TVector<TString> UdfFiles;
    TString QueryFile;
    NActors::NLog::EPriority YqlLogLevel = NActors::NLog::EPriority::PRI_ERROR;
    bool EnableAntlr4Parser = false;
    bool EnableOltpSinkSideBySinkCompare = false;

    void ParseConfig(int argc, const char** argv);
};

namespace NYql {
    class IHTTPGateway;
}

using namespace NActors;

THolder<TActorSystemSetup> BuildActorSystemSetup(ui32 threads, ui32 pools = 1);

struct TTableReadAccessInfo {
    std::string ReadType;
    i32 PushedLimit = -1;
    std::vector<std::string> ReadColumns;

    constexpr bool operator==(const TTableReadAccessInfo& other) const {
        return std::tie(ReadType, PushedLimit, ReadColumns) == std::tie(other.ReadType, other.PushedLimit, other.ReadColumns);
    }

    constexpr bool operator<(const TTableReadAccessInfo& other) const  {
        return std::tie(ReadType, PushedLimit, ReadColumns) < std::tie(other.ReadType, other.PushedLimit, other.ReadColumns);
    }
};

enum EWriteType : ui32 {
    Upsert = 1,
    Erase = 2
};

struct TTableWriteInfo {
    std::string WriteType;
    std::vector<std::string> WriteColumns;

    constexpr bool operator==(const TTableWriteInfo& other) const {
        return std::tie(WriteType, WriteColumns) == std::tie(other.WriteType, other.WriteColumns);
    }

    constexpr bool operator<(const TTableWriteInfo& other) const  {
        return std::tie(WriteType, WriteColumns) < std::tie(other.WriteType, other.WriteColumns);
    }
};

struct TTableStats {
    TString Name;
    std::vector<TTableReadAccessInfo> Reads;
    std::vector<TTableWriteInfo> Writes;

    constexpr bool operator==(const TTableStats& other) const {
        return std::tie(Name, Reads, Writes) == std::tie(other.Name, other.Reads, other.Writes);
    }

    constexpr bool operator<(const TTableStats& other) const {
        return std::tie(Name, Reads, Writes) < std::tie(other.Name, other.Reads, other.Writes);
    }
};

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
        UncategorizedFailure,
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
        std::map<std::string, TTableStats> EngineTableStats;

        TEvCompileResponse(bool success)
            : Success(success)
        {
        }
    };
};

THashMap<TString, NYql::TKikimrTableMetadataPtr> ExtractStaticMetadata(const NJson::TJsonValue& data);

NActors::IActor* CreateQueryCompiler(TIntrusivePtr<NKikimr::NKqp::TModuleResolverState> moduleResolverState,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, std::shared_ptr<NYql::IHTTPGateway> httpGateway, bool enableAntlr4Parser,
    bool enableOltpSink);
