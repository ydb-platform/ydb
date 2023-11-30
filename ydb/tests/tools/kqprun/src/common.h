#pragma once

#include <ydb/core/protos/config.pb.h>

#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <ydb/public/lib/ydb_cli/common/formats.h>


namespace NKqpRun {

struct TYdbSetupSettings {
    TString DomainName = "Root";

    bool TraceOpt = false;
    TMaybe<TString> LogOutputFile;

    TString YqlToken;
    NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
    NKikimrConfig::TAppConfig AppConfig;
};


struct TRunnerOptions {
    enum class EResultOutputFormat {
        RowsJson,  // Rows in json format
        FullJson,  // Columns, rows and types in json format
    };

    i64 ResultsRowsLimit = 1000;

    IOutputStream* ResultOutput = &Cout;
    IOutputStream* SchemeQueryAstOutput = nullptr;
    IOutputStream* ScriptQueryAstOutput = nullptr;
    IOutputStream* ScriptQueryPlanOutput = nullptr;

    EResultOutputFormat ResultOutputFormat = EResultOutputFormat::RowsJson;
    NYdb::NConsoleClient::EOutputFormat PlanOutputFormat = NYdb::NConsoleClient::EOutputFormat::Default;

    TYdbSetupSettings YdbSettings;
};

}  // namespace NKqpRun
