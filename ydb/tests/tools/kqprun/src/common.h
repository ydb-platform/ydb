#pragma once

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/log_iface.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/lib/ydb_cli/common/formats.h>
#include <ydb/tests/tools/kqprun/runlib/settings.h>
#include <ydb/tests/tools/kqprun/src/proto/storage_meta.pb.h>


namespace NKqpRun {

constexpr char YQL_TOKEN_VARIABLE[] = "YQL_TOKEN";
constexpr ui64 DEFAULT_STORAGE_SIZE = 32_GB;
constexpr TDuration TENANT_CREATION_TIMEOUT = TDuration::Seconds(30);

struct TYdbSetupSettings : public NKikimrRun::TServerSettings {
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
        FetchDatabase,
        ScriptRequest,
        Max
    };

    ui32 DcCount = 1;
    std::map<TString, TStorageMeta::TTenant> Tenants;
    TDuration HealthCheckTimeout = TDuration::Seconds(10);
    EHealthCheck HealthCheckLevel = EHealthCheck::NodesCount;
    bool SameSession = false;

    bool DisableDiskMock = false;
    bool FormatStorage = false;
    std::optional<TString> PDisksPath;
    std::optional<ui64> DiskSize;

    bool TraceOptEnabled = false;
    EVerbose VerboseLevel = EVerbose::Info;
    NKikimrRun::TAsyncQueriesSettings AsyncQueriesSettings;

    NYql::IPqGateway::TPtr PqGateway;
};


struct TRunnerOptions {
    enum class ETraceOptType {
        Disabled,
        Scheme,
        Script,
        All,
    };

    IOutputStream* ResultOutput = nullptr;
    IOutputStream* SchemeQueryAstOutput = nullptr;
    std::vector<IOutputStream*> ScriptQueryAstOutputs;
    std::vector<IOutputStream*> ScriptQueryPlanOutputs;
    std::vector<TString> ScriptQueryTimelineFiles;
    std::vector<TString> InProgressStatisticsOutputFiles;

    NKikimrRun::EResultOutputFormat ResultOutputFormat = NKikimrRun::EResultOutputFormat::RowsJson;
    NYdb::NConsoleClient::EDataFormat PlanOutputFormat = NYdb::NConsoleClient::EDataFormat::Default;
    ETraceOptType TraceOptType = ETraceOptType::Disabled;
    std::optional<size_t> TraceOptScriptId;

    TDuration ScriptCancelAfter;
    std::unordered_set<Ydb::StatusIds::StatusCode> RetryableStatuses;

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
    std::unordered_map<TString, Ydb::TypedValue> Params;
    std::optional<TVector<NACLib::TSID>> GroupSIDs = std::nullopt;
};

}  // namespace NKqpRun
