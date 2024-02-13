#pragma once

#include <ydb/core/protos/config.pb.h>

#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <ydb/public/lib/ydb_cli/common/formats.h>


namespace NKqpRun {

struct TYdbSetupSettings {
    TString DomainName = "Root";

    bool TraceOptEnabled = false;
    TMaybe<TString> LogOutputFile;

    TString YqlToken;
    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FunctionRegistry = nullptr;
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
    };

    IOutputStream* ResultOutput = &Cout;
    IOutputStream* SchemeQueryAstOutput = nullptr;
    IOutputStream* ScriptQueryAstOutput = nullptr;
    IOutputStream* ScriptQueryPlanOutput = nullptr;

    EResultOutputFormat ResultOutputFormat = EResultOutputFormat::RowsJson;
    NYdb::NConsoleClient::EOutputFormat PlanOutputFormat = NYdb::NConsoleClient::EOutputFormat::Default;
    ETraceOptType TraceOptType = ETraceOptType::Disabled;

    TYdbSetupSettings YdbSettings;
};

}  // namespace NKqpRun
