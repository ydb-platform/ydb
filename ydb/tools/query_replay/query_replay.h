#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/logger/backend.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/core/scheme/scheme_type_registry.h>
#include <library/cpp/json/json_value.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <optional>
#include <unordered_set>
#include <deque>

#include <ydb/library/yql/core/services/mounts/yql_mounts.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

struct TQueryReplayStats;

struct TQueryReplayApp {
    TString Endpoint;
    TString Database;
    TString Path;
    TString StatsPath;
    TString AuthToken;
    ui32 ActorSystemThreadsCount = 15;
    ui32 MaxInFlight = 1000;
    ui32 Modulo = 1;
    ui32 ShardId = 0;
    TVector<TString> Queries;

    THolder<NActors::TActorSystem> ActorSystem;
    THolder<NKikimr::TAppData> AppData;
    TIntrusivePtr<NKikimr::NScheme::TKikimrTypeRegistry> TypeRegistry;
    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FunctionRegistry;
    TIntrusivePtr<NActors::NLog::TSettings> LogSettings;
    TAutoPtr<TLogBackend> LogBackend;
    NYdb::TDriverConfig DriverConfig;
    NActors::TActorId ReplayActor;
    std::unique_ptr<NYdb::TDriver> Driver;
    std::shared_ptr<TQueryReplayStats> QueryReplayStats;
    TIntrusivePtr<IRandomProvider> RandomProvider;

    void InitializeLogger();
    void InitializeDriver();

    int ParseConfig(int argc, char** argv);
    void Start();
    void Stop();
};

struct TQueryReplayStats {
    std::atomic<ui32> TotalQueries;
    std::atomic<ui32> CompilationErrors;
    std::atomic<ui32> CompilationSuccess;
    std::atomic<bool> ReplayComplete;

    TQueryReplayStats()
        : TotalQueries(0)
        , CompilationErrors(0)
        , CompilationSuccess(0)
        , ReplayComplete(false)
    {
    }

    void AddNew(ui32 val) {
        TotalQueries.fetch_add(val);
    }

    void OnSuccess() {
        ++CompilationSuccess;
    }

    void OnFailure() {
        ++CompilationErrors;
    }

    ui32 GetTotalCount() {
        return TotalQueries.load();
    }

    ui32 GetCompletedCount() {
        return CompilationSuccess.load();
    }

    ui32 GetErrorsCount() {
        return CompilationErrors.load();
    }

    void Complete() {
        ReplayComplete.store(true);
    }

    bool IsComplete() {
        return ReplayComplete.load();
    }
};

struct TQueryReplayEvents {
    enum EEv {
        EvReadFailed = EventSpaceBegin(NActors::TEvents::ES_USERSPACE + 1),
        EvReadStarted,
        EvQueryBatch,
        EvQueryProcessorResult,
        EvQueryResolveResult,
        EvCompileResponse,
    };

    struct TEvReadFailed: public NActors::TEventLocal<TEvReadFailed, EvReadFailed> {
    };

    struct TEvReadStarted: public NActors::TEventLocal<TEvReadStarted, EvReadStarted> {
        NYdb::NTable::TTablePartIterator PartIterator;

        TEvReadStarted(NYdb::NTable::TTablePartIterator&& partIterator)
            : PartIterator(partIterator)
        {
        }
    };

    struct TEvQueryBatch: public NActors::TEventLocal<TEvQueryBatch, EvQueryBatch> {
        NYdb::NTable::TSimpleStreamPart<NYdb::TResultSet> TablePart;

        TEvQueryBatch(NYdb::NTable::TSimpleStreamPart<NYdb::TResultSet>&& tablePart)
            : TablePart(tablePart)
        {
        }
    };

    struct TEvQueryProcessorResult: public NActors::TEventLocal<TEvQueryProcessorResult, EvQueryProcessorResult> {
        bool Success;

        TEvQueryProcessorResult(bool success)
            : Success(success)
        {
        }
    };

    struct TEvQueryResolveResult: public NActors::TEventLocal<TEvQueryResolveResult, EvQueryResolveResult> {
        NYdb::NTable::TDataQueryResult DataQueryResult;

        TEvQueryResolveResult(NYdb::NTable::TDataQueryResult&& result)
            : DataQueryResult(result)
        {
        }
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
        Unspecified,
    };

    struct TEvCompileResponse: public NActors::TEventLocal<TEvCompileResponse, EvCompileResponse> {
        bool Success;
        TCheckQueryPlanStatus Status = Unspecified;
        TString Message;

        TEvCompileResponse(bool success)
            : Success(success)
        {
        }
    };
};


NActors::IActor* CreateQueryCompiler(
    const NActors::TActorId& ownerId, TIntrusivePtr<NKikimr::NKqp::TModuleResolverState> moduleResolverState,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, NJson::TJsonValue&& replayDetails);
NActors::IActor* CreateQueryProcessorActor(std::shared_ptr<NYdb::NTable::TTableClient> client, const TString& runId, const TString& queryId, const TString& tablePath, const TString& statsTablePath,
                                           const NActors::TActorId& ownerId, TIntrusivePtr<NKikimr::NKqp::TModuleResolverState> moduleResolverState, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry);
NActors::IActor* CreateQueryReplayActor(NYdb::TDriver& driver, const TString& runId, const TString& queryTablePath, const TString& statsTablePath, std::shared_ptr<TQueryReplayStats> queryReplayStats,
                                        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, ui32 maxInFlight, ui32 modulo, ui32 shardId);
NActors::IActor* CreateQueryReplayActorSimple(
    std::vector<TString>&& queries, std::shared_ptr<TQueryReplayStats> queryReplayStats, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry);
