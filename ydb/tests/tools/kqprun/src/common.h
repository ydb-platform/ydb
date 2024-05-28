#pragma once

#include <ydb/core/protos/config.pb.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>

#include <ydb/public/lib/ydb_cli/common/formats.h>


namespace NKqpRun {

constexpr char YQL_TOKEN_VARIABLE[] = "YQL_TOKEN";

struct TYdbSetupSettings {
    i32 NodeCount = 1;
    TString DomainName = "Root";
    TDuration InitializationTimeout = TDuration::Seconds(10);

    bool MonitoringEnabled = false;
    bool TraceOptEnabled = false;
    TMaybe<TString> LogOutputFile;

    TString YqlToken;
    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FunctionRegistry;
    NKikimr::NMiniKQL::TComputationNodeFactory ComputationFactory;
    TIntrusivePtr<NYql::IYtGateway> YtGateway;
    NKikimrConfig::TAppConfig AppConfig;
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

    IOutputStream* ResultOutput = &Cout;
    IOutputStream* SchemeQueryAstOutput = nullptr;
    IOutputStream* ScriptQueryAstOutput = nullptr;
    IOutputStream* ScriptQueryPlanOutput = nullptr;
    TMaybe<TString> InProgressStatisticsOutputFile;

    EResultOutputFormat ResultOutputFormat = EResultOutputFormat::RowsJson;
    NYdb::NConsoleClient::EOutputFormat PlanOutputFormat = NYdb::NConsoleClient::EOutputFormat::Default;
    ETraceOptType TraceOptType = ETraceOptType::Disabled;

    TYdbSetupSettings YdbSettings;
};

}  // namespace NKqpRun
