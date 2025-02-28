#pragma once

#include <util/generic/string.h>

#include <ydb/core/fq/libs/config/protos/fq_config.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>
#include <ydb/tests/tools/kqprun/runlib/settings.h>

#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NFqRun {

constexpr char YQL_TOKEN_VARIABLE[] = "YQL_TOKEN";
constexpr i64 MAX_RESULT_SET_ROWS = 1000;

struct TExternalDatabase {
    TString Endpoint;
    TString Database;
    TString Token;

    static TExternalDatabase Parse(const TString& optionValue, const TString& tokenVar);
};

struct TFqSetupSettings : public NKikimrRun::TServerSettings {
    enum class EVerbose {
        None,
        Info,
        QueriesText,
        InitLogs,
        Max
    };

    bool EmulateS3 = false;
    bool EnableTraceOpt = false;

    bool EnableQuotas = false;
    std::optional<TExternalDatabase> RateLimiterDatabase;

    bool EnableCheckpoints = false;
    std::optional<TExternalDatabase> CheckpointsDatabase;

    bool EnableCpStorage = false;
    std::optional<TExternalDatabase> CpStorageDatabase;

    bool EnableRemoteRd = false;
    std::optional<TExternalDatabase> RowDispatcherDatabase;

    EVerbose VerboseLevel = EVerbose::Info;

    TString YqlToken;
    NYql::IPqGatewayFactory::TPtr PqGatewayFactory;
    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FunctionRegistry;
    NFq::NConfig::TConfig FqConfig;
    NKikimrConfig::TLogConfig LogConfig;
    std::optional<NKikimrConfig::TActorSystemConfig> ActorSystemConfig;
    NKikimrRun::TAsyncQueriesSettings AsyncQueriesSettings;
};

struct TRunnerOptions {
    bool TraceOptAll = false;
    std::unordered_set<ui64> TraceOptIds;

    IOutputStream* ResultOutput = nullptr;
    NKikimrRun::EResultOutputFormat ResultOutputFormat = NKikimrRun::EResultOutputFormat::RowsJson;

    TDuration PingPeriod;
    TFqSetupSettings FqSettings;
};

struct TRequestOptions {
    TString Query;
    ui64 QueryId;
};

void SetupAcl(FederatedQuery::Acl* acl);

NYql::TIssue GroupIssues(NYql::TIssue rootIssue, const NYql::TIssues& childrenIssues);

bool IsFinalStatus(FederatedQuery::QueryMeta::ComputeStatus status);

}  // namespace NFqRun
