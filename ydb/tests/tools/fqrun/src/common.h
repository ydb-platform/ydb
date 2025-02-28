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

struct TFqSetupSettings : public NKikimrRun::TServerSettings {
    enum class EVerbose {
        None,
        Info,
        QueriesText,
        InitLogs,
        Max
    };

    bool EmulateS3 = false;

    EVerbose VerboseLevel = EVerbose::Info;

    TString YqlToken;
    NYql::IPqGatewayFactory::TPtr PqGatewayFactory;
    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FunctionRegistry;
    NFq::NConfig::TConfig FqConfig;
    NKikimrConfig::TLogConfig LogConfig;
    std::optional<NKikimrConfig::TActorSystemConfig> ActorSystemConfig;
};

struct TRunnerOptions {
    IOutputStream* ResultOutput = nullptr;
    NKikimrRun::EResultOutputFormat ResultOutputFormat = NKikimrRun::EResultOutputFormat::RowsJson;

    TFqSetupSettings FqSettings;
};

struct TRequestOptions {
    TString Query;
};

void SetupAcl(FederatedQuery::Acl* acl);

}  // namespace NFqRun
