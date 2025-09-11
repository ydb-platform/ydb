#pragma once

#include <util/generic/string.h>

#include <ydb/core/protos/config.pb.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>

namespace NKikimrRun {

constexpr char YQL_TOKEN_VARIABLE[] = "YQL_TOKEN";

struct TAsyncQueriesSettings {
    enum class EVerbosity {
        EachQuery,
        Final,
    };

    ui64 InFlightLimit = 0;
    EVerbosity Verbosity = EVerbosity::EachQuery;
};

struct TServerSettings {
    ui32 NodeCount = 1;
    TString DomainName = "Root";

    bool MonitoringEnabled = false;
    ui16 FirstMonitoringPort = 0;

    bool GrpcEnabled = false;
    ui16 FirstGrpcPort = 0;

    TString LogOutputFile;

    TString YqlToken;
    NKikimrConfig::TAppConfig AppConfig;
    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FunctionRegistry;
    NKikimr::NMiniKQL::TComputationNodeFactory ComputationFactory;
    TIntrusivePtr<NYql::IYtGateway> YtGateway;
};

enum class EResultOutputFormat {
    RowsJson,  // Rows in json format
    FullJson,  // Columns, rows and types in json format
    FullProto,  // Columns, rows and types in proto string format
};

}  // namespace NKikimrRun
