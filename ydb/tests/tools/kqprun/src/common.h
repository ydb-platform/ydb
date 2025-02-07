#pragma once

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/kqp.pb.h>

#include <ydb/library/actors/core/log_iface.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/lib/ydb_cli/common/formats.h>

#include <ydb/tests/tools/kqprun/src/proto/storage_meta.pb.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>


namespace NKqpRun {

constexpr char YQL_TOKEN_VARIABLE[] = "YQL_TOKEN";
constexpr ui64 DEFAULT_STORAGE_SIZE = 32_GB;

struct TAsyncQueriesSettings {
    enum class EVerbose {
        EachQuery,
        Final,
    };

    ui64 InFlightLimit = 0;
    EVerbose Verbose = EVerbose::EachQuery;
};

struct TYdbSetupSettings {
    enum class EVerbose {
        None,
        Info,
        QueriesText,
        InitLogs,
        Max
    };

    enum class EHealthCheck {
        None,
        NodesCount,
        ScriptRequest,
        Max
    };

    ui32 NodeCount = 1;
    TString DomainName = "Root";
    std::map<TString, TStorageMeta::TTenant> Tenants;
    TDuration HealthCheckTimeout = TDuration::Seconds(10);
    EHealthCheck HealthCheckLevel = EHealthCheck::NodesCount;
    bool SameSession = false;

    bool DisableDiskMock = false;
    bool FormatStorage = false;
    std::optional<TString> PDisksPath;
    std::optional<ui64> DiskSize;

    bool MonitoringEnabled = false;
    ui16 MonitoringPortOffset = 0;

    bool GrpcEnabled = false;
    ui16 GrpcPort = 0;

    bool TraceOptEnabled = false;
    TString LogOutputFile;
    EVerbose VerboseLevel = EVerbose::Info;

    TString YqlToken;
    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FunctionRegistry;
    NKikimr::NMiniKQL::TComputationNodeFactory ComputationFactory;
    TIntrusivePtr<NYql::IYtGateway> YtGateway;
    NKikimrConfig::TAppConfig AppConfig;
    TAsyncQueriesSettings AsyncQueriesSettings;
};


struct TRunnerOptions {
    enum class ETraceOptType {
        Disabled,
        Scheme,
        Script,
        All,
    };

    enum class EResultOutputFormat {
        RowsJson,  // Rows in json format
        FullJson,  // Columns, rows and types in json format
        FullProto,  // Columns, rows and types in proto string format
    };

    IOutputStream* ResultOutput = nullptr;
    IOutputStream* SchemeQueryAstOutput = nullptr;
    std::vector<IOutputStream*> ScriptQueryAstOutputs;
    std::vector<IOutputStream*> ScriptQueryPlanOutputs;
    std::vector<TString> ScriptQueryTimelineFiles;
    std::vector<TString> InProgressStatisticsOutputFiles;

    EResultOutputFormat ResultOutputFormat = EResultOutputFormat::RowsJson;
    NYdb::NConsoleClient::EDataFormat PlanOutputFormat = NYdb::NConsoleClient::EDataFormat::Default;
    ETraceOptType TraceOptType = ETraceOptType::Disabled;
    std::optional<size_t> TraceOptScriptId;

    TDuration ScriptCancelAfter;

    TYdbSetupSettings YdbSettings;
};


struct TRequestOptions {
    TString Query;
    NKikimrKqp::EQueryAction Action;
    TString TraceId;
    TString PoolId;
    TString UserSID;
    TString Database;
    TDuration Timeout;
    size_t QueryId = 0;
};

template <typename TValue>
TValue GetValue(size_t index, const std::vector<TValue>& values, TValue defaultValue) {
    if (values.empty()) {
        return defaultValue;
    }
    return values[std::min(index, values.size() - 1)];
}

NKikimrServices::EServiceKikimr GetLogService(const TString& serviceName);
void ModifyLogPriorities(std::unordered_map<NKikimrServices::EServiceKikimr, NActors::NLog::EPriority> logPriorities, NKikimrConfig::TLogConfig& logConfig);

}  // namespace NKqpRun
