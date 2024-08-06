#include "kqp_ut_common.h"

#include <ydb/core/base/backtrace.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/testing/common/env.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb::NTable;

const TString EXPECTED_EIGHTSHARD_VALUE1 = R"(
[
    [[1];[101u];["Value1"]];
    [[2];[201u];["Value1"]];
    [[3];[301u];["Value1"]];
    [[1];[401u];["Value1"]];
    [[2];[501u];["Value1"]];
    [[3];[601u];["Value1"]];
    [[1];[701u];["Value1"]];
    [[2];[801u];["Value1"]]
])";

SIMPLE_UDF(TTestFilter, bool(i64)) {
    Y_UNUSED(valueBuilder);
    const i64 arg = args[0].Get<i64>();

    return NUdf::TUnboxedValuePod(arg >= 0);
}

SIMPLE_UDF(TTestFilterTerminate, bool(i64, i64)) {
    Y_UNUSED(valueBuilder);
    const i64 arg1 = args[0].Get<i64>();
    const i64 arg2 = args[1].Get<i64>();

    if (arg1 < arg2) {
        UdfTerminate("Bad filter value.");
    }

    return NUdf::TUnboxedValuePod(true);
}

SIMPLE_UDF(TRandString, char*(ui32)) {
    Y_UNUSED(valueBuilder);
    const ui32 size = args[0].Get<ui32>();

    auto str = valueBuilder->NewStringNotFilled(size);
    auto strRef = str.AsStringRef();

    for (ui32 i = 0; i < size; ++i) {
        *(strRef.Data() + i) = '0' + RandomNumber<ui32>() % 10;
    }

    return str;
}

SIMPLE_MODULE(TTestUdfsModule, TTestFilter, TTestFilterTerminate, TRandString);
NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateJson2Module();
NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateRe2Module();
NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateStringModule();
NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateDateTime2Module();

NMiniKQL::IFunctionRegistry* UdfFrFactory(const NScheme::TTypeRegistry& typeRegistry) {
    Y_UNUSED(typeRegistry);
    auto funcRegistry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry())->Clone();
    funcRegistry->AddModule("", "TestUdfs", new TTestUdfsModule());
    funcRegistry->AddModule("", "Json2", CreateJson2Module());
    funcRegistry->AddModule("", "Re2", CreateRe2Module());
    funcRegistry->AddModule("", "String", CreateStringModule());
    funcRegistry->AddModule("", "DateTime", CreateDateTime2Module());
    NKikimr::NMiniKQL::FillStaticModules(*funcRegistry);
    return funcRegistry.Release();
}

TVector<NKikimrKqp::TKqpSetting> SyntaxV1Settings() {
    auto setting = NKikimrKqp::TKqpSetting();
    setting.SetName("_KqpYqlSyntaxVersion");
    setting.SetValue("1");
    return {setting};
}

TKikimrRunner::TKikimrRunner(const TKikimrSettings& settings) {
    EnableYDBBacktraceFormat();

    auto mbusPort = PortManager.GetPort();
    auto grpcPort = PortManager.GetPort();

    Cerr << "Trying to start YDB, gRPC: " << grpcPort << ", MsgBus: " << mbusPort << Endl;

    TVector<NKikimrKqp::TKqpSetting> effectiveKqpSettings;

    bool enableSpilling = false;
    if (settings.AppConfig.GetTableServiceConfig().GetSpillingServiceConfig().GetLocalFileConfig().GetEnable()) {
        NKikimrKqp::TKqpSetting setting;
        setting.SetName("_KqpEnableSpilling");
        setting.SetValue("true");
        effectiveKqpSettings.push_back(setting);
        enableSpilling = true;
    }

    effectiveKqpSettings.insert(effectiveKqpSettings.end(), settings.KqpSettings.begin(), settings.KqpSettings.end());

    NKikimrProto::TAuthConfig authConfig;
    authConfig.SetUseBuiltinDomain(true);
    ServerSettings.Reset(MakeHolder<Tests::TServerSettings>(mbusPort, authConfig, settings.PQConfig));
    ServerSettings->SetDomainName(settings.DomainRoot);
    ServerSettings->SetKqpSettings(effectiveKqpSettings);

    NKikimrConfig::TAppConfig appConfig = settings.AppConfig;
    appConfig.MutableColumnShardConfig()->SetDisabledOnSchemeShard(false);
    appConfig.MutableTableServiceConfig()->SetEnableRowsDuplicationCheck(true);
    ServerSettings->SetAppConfig(appConfig);
    ServerSettings->SetFeatureFlags(settings.FeatureFlags);
    ServerSettings->SetNodeCount(settings.NodeCount);
    ServerSettings->SetEnableKqpSpilling(enableSpilling);
    ServerSettings->SetEnableDataColumnForIndexTable(true);
    ServerSettings->SetKeepSnapshotTimeout(settings.KeepSnapshotTimeout);
    ServerSettings->SetFrFactory(&UdfFrFactory);
    ServerSettings->SetEnableNotNullColumns(true);
    ServerSettings->SetEnableMoveIndex(true);
    ServerSettings->SetEnableUniqConstraint(true);
    ServerSettings->SetUseRealThreads(settings.UseRealThreads);
    ServerSettings->SetEnableTablePgTypes(true);
    ServerSettings->S3ActorsFactory = settings.S3ActorsFactory;

    if (settings.Storage) {
        ServerSettings->SetCustomDiskParams(*settings.Storage);
        ServerSettings->SetEnableMockOnSingleNode(false);
    }

    if (settings.LogStream)
        ServerSettings->SetLogBackend(new TStreamLogBackend(settings.LogStream));

    if (settings.FederatedQuerySetupFactory) {
        ServerSettings->SetFederatedQuerySetupFactory(settings.FederatedQuerySetupFactory);
    }

    Server.Reset(MakeHolder<Tests::TServer>(*ServerSettings));
    Server->EnableGRpc(grpcPort);

    RunCall([this, domain = settings.DomainRoot] {
        this->Server->SetupDefaultProfiles();
        return true;
    });

    Client.Reset(MakeHolder<Tests::TClient>(*ServerSettings));

    Endpoint = "localhost:" + ToString(grpcPort);

    DriverConfig = NYdb::TDriverConfig()
        .SetEndpoint(Endpoint)
        .SetDatabase("/" + settings.DomainRoot)
        .SetDiscoveryMode(NYdb::EDiscoveryMode::Async)
        .SetAuthToken(settings.AuthToken);
    Driver.Reset(MakeHolder<NYdb::TDriver>(DriverConfig));

    CountersRoot = settings.CountersRoot;

    Initialize(settings);
}

TKikimrRunner::TKikimrRunner(const TVector<NKikimrKqp::TKqpSetting>& kqpSettings, const TString& authToken,
    const TString& domainRoot, ui32 nodeCount)
    : TKikimrRunner(TKikimrSettings()
        .SetKqpSettings(kqpSettings)
        .SetAuthToken(authToken)
        .SetDomainRoot(domainRoot)
        .SetNodeCount(nodeCount)) {}

TKikimrRunner::TKikimrRunner(const NKikimrConfig::TAppConfig& appConfig, const TString& authToken,
    const TString& domainRoot, ui32 nodeCount)
    : TKikimrRunner(TKikimrSettings()
        .SetAppConfig(appConfig)
        .SetAuthToken(authToken)
        .SetDomainRoot(domainRoot)
        .SetNodeCount(nodeCount)) {}

TKikimrRunner::TKikimrRunner(const NKikimrConfig::TAppConfig& appConfig,
    const TVector<NKikimrKqp::TKqpSetting>& kqpSettings, const TString& authToken, const TString& domainRoot,
    ui32 nodeCount)
    : TKikimrRunner(TKikimrSettings()
        .SetAppConfig(appConfig)
        .SetKqpSettings(kqpSettings)
        .SetAuthToken(authToken)
        .SetDomainRoot(domainRoot)
        .SetNodeCount(nodeCount)) {}

TKikimrRunner::TKikimrRunner(const NKikimrConfig::TFeatureFlags& featureFlags, const TString& authToken,
    const TString& domainRoot, ui32 nodeCount)
    : TKikimrRunner(TKikimrSettings()
        .SetFeatureFlags(featureFlags)
        .SetAuthToken(authToken)
        .SetDomainRoot(domainRoot)
        .SetNodeCount(nodeCount)) {}

TKikimrRunner::TKikimrRunner(const TString& authToken, const TString& domainRoot, ui32 nodeCount)
    : TKikimrRunner(TKikimrSettings()
        .SetAuthToken(authToken)
        .SetDomainRoot(domainRoot)
        .SetNodeCount(nodeCount)) {}

TKikimrRunner::TKikimrRunner(const NFake::TStorage& storage)
    : TKikimrRunner(TKikimrSettings()
        .SetStorage(storage)) {}

void TKikimrRunner::CreateSampleTables() {
    Client->CreateTable("/Root", R"(
        Name: "TwoShard"
        Columns { Name: "Key", Type: "Uint32" }
        Columns { Name: "Value1", Type: "String" }
        Columns { Name: "Value2", Type: "Int32" }
        KeyColumnNames: ["Key"]
        UniformPartitionsCount: 2
    )");

    Client->CreateTable("/Root", R"(
        Name: "EightShard"
        Columns { Name: "Key", Type: "Uint64" }
        Columns { Name: "Text", Type: "String" }
        Columns { Name: "Data", Type: "Int32" }
        KeyColumnNames: ["Key"],
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 300 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 400 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 500 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 600 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 700 } } } }
    )");

    Client->CreateTable("/Root", R"(
        Name: "Logs"
        Columns { Name: "App", Type: "Utf8" }
        Columns { Name: "Message", Type: "Utf8" }
        Columns { Name: "Ts", Type: "Int64" }
        Columns { Name: "Host", Type: "Utf8" }
        KeyColumnNames: ["App", "Ts", "Host"],
        SplitBoundary { KeyPrefix { Tuple { Optional { Text: "a" } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Text: "b" } } } }
    )");

    Client->CreateTable("/Root", R"(
        Name: "BatchUpload"
        Columns {
            Name: "Key1"
            Type: "Uint32"
        }
        Columns {
            Name: "Key2"
            Type: "String"
        }
        Columns {
            Name: "Value1"
            Type: "Int64"
        }
        Columns {
            Name: "Value2"
            Type: "Double"
        }
        Columns {
            Name: "Blob1"
            Type: "String"
        }
        Columns {
            Name: "Blob2"
            Type: "String"
        }
        KeyColumnNames: ["Key1", "Key2"]
        UniformPartitionsCount: 10
    )");

    // TODO: Reuse driver (YDB-626)
    NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(Endpoint).SetDatabase("/Root"));
    NYdb::NTable::TTableClient client(driver);
    auto session = client.CreateSession().GetValueSync().GetSession();

    AssertSuccessResult(session.ExecuteSchemeQuery(R"(
        --!syntax_v1

        CREATE TABLE `KeyValue` (
            Key Uint64,
            Value String,
            PRIMARY KEY (Key)
        );

        CREATE TABLE `KeyValue2` (
            Key String,
            Value String,
            PRIMARY KEY (Key)
        );

        CREATE TABLE `KeyValueLargePartition` (
            Key Uint64,
            Value String,
            PRIMARY KEY (Key)
        );

        CREATE TABLE `Test` (
            Group Uint32,
            Name String,
            Amount Uint64,
            Comment String,
            PRIMARY KEY (Group, Name)
        );

        CREATE TABLE `Join1` (
            Key Int32,
            Fk21 Uint32,
            Fk22 String,
            Value String,
            PRIMARY KEY (Key)
        )
        WITH (
            PARTITION_AT_KEYS = (5)
        );

        CREATE TABLE `Join2` (
            Key1 Uint32,
            Key2 String,
            Name String,
            Value2 String,
            PRIMARY KEY (Key1, Key2)
        )
        WITH (
            PARTITION_AT_KEYS = (105)
        );
    )").GetValueSync());

    AssertSuccessResult(session.ExecuteDataQuery(R"(

        REPLACE INTO `KeyValueLargePartition` (Key, Value) VALUES
            (101u, "Value1"),
            (102u, "Value2"),
            (103u, "Value3"),
            (201u, "Value1"),
            (202u, "Value2"),
            (203u, "Value3"),
            (301u, "Value1"),
            (302u, "Value2"),
            (303u, "Value3"),
            (401u, "Value1"),
            (402u, "Value2"),
            (403u, "Value3"),
            (501u, "Value1"),
            (502u, "Value2"),
            (503u, "Value3"),
            (601u, "Value1"),
            (602u, "Value2"),
            (603u, "Value3"),
            (701u, "Value1"),
            (702u, "Value2"),
            (703u, "Value3"),
            (801u, "Value1"),
            (802u, "Value2"),
            (803u, "Value3");

        REPLACE INTO `TwoShard` (Key, Value1, Value2) VALUES
            (1u, "One", -1),
            (2u, "Two", 0),
            (3u, "Three", 1),
            (4000000001u, "BigOne", -1),
            (4000000002u, "BigTwo", 0),
            (4000000003u, "BigThree", 1);

        REPLACE INTO `EightShard` (Key, Text, Data) VALUES
            (101u, "Value1",  1),
            (201u, "Value1",  2),
            (301u, "Value1",  3),
            (401u, "Value1",  1),
            (501u, "Value1",  2),
            (601u, "Value1",  3),
            (701u, "Value1",  1),
            (801u, "Value1",  2),
            (102u, "Value2",  3),
            (202u, "Value2",  1),
            (302u, "Value2",  2),
            (402u, "Value2",  3),
            (502u, "Value2",  1),
            (602u, "Value2",  2),
            (702u, "Value2",  3),
            (802u, "Value2",  1),
            (103u, "Value3",  2),
            (203u, "Value3",  3),
            (303u, "Value3",  1),
            (403u, "Value3",  2),
            (503u, "Value3",  3),
            (603u, "Value3",  1),
            (703u, "Value3",  2),
            (803u, "Value3",  3);

        REPLACE INTO `KeyValue` (Key, Value) VALUES
            (1u, "One"),
            (2u, "Two");

        REPLACE INTO `KeyValue2` (Key, Value) VALUES
            ("1", "One"),
            ("2", "Two");

        REPLACE INTO `Test` (Group, Name, Amount, Comment) VALUES
            (1u, "Anna", 3500ul, "None"),
            (1u, "Paul", 300ul, "None"),
            (2u, "Tony", 7200ul, "None");

        REPLACE INTO `Logs` (App, Ts, Host, Message) VALUES
            ("apache", 0, "front-42", " GET /index.html HTTP/1.1"),
            ("nginx", 1, "nginx-10", "GET /index.html HTTP/1.1"),
            ("nginx", 2, "nginx-23", "PUT /form HTTP/1.1"),
            ("nginx", 3, "nginx-23", "GET /cat.jpg HTTP/1.1"),
            ("kikimr-db", 1, "kikimr-db-10", "Write Data"),
            ("kikimr-db", 2, "kikimr-db-21", "Read Data"),
            ("kikimr-db", 3, "kikimr-db-21", "Stream Read Data"),
            ("kikimr-db", 4, "kikimr-db-53", "Discover"),
            ("ydb", 0, "ydb-1000", "some very very very very long string");

        REPLACE INTO `Join1` (Key, Fk21, Fk22, Value) VALUES
            (1, 101, "One", "Value1"),
            (2, 102, "Two", "Value1"),
            (3, 103, "One", "Value2"),
            (4, 104, "Two", "Value2"),
            (5, 105, "One", "Value3"),
            (6, 106, "Two", "Value3"),
            (7, 107, "One", "Value4"),
            (8, 108, "One", "Value5"),
            (9, 101, "Two", "Value1");

        REPLACE INTO `Join2` (Key1, Key2, Name, Value2) VALUES
            (101, "One",   "Name1", "Value21"),
            (101, "Two",   "Name1", "Value22"),
            (101, "Three", "Name3", "Value23"),
            (102, "One",   "Name2", "Value24"),
            (103, "One",   "Name1", "Value25"),
            (104, "One",   "Name3", "Value26"),
            (105, "One",   "Name2", "Value27"),
            (105, "Two",   "Name4", "Value28"),
            (106, "One",   "Name3", "Value29"),
            (108, "One",    NULL,   "Value31");
    )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync());

}

static TMaybe<NActors::NLog::EPriority> ParseLogLevel(const TString& level) {
    static const THashMap<TString, NActors::NLog::EPriority> levels = {
        { "TRACE", NActors::NLog::PRI_TRACE },
        { "DEBUG", NActors::NLog::PRI_DEBUG },
        { "INFO", NActors::NLog::PRI_INFO },
        { "NOTICE", NActors::NLog::PRI_NOTICE },
        { "WARN", NActors::NLog::PRI_WARN },
        { "ERROR", NActors::NLog::PRI_ERROR },
        { "CRIT", NActors::NLog::PRI_CRIT },
        { "ALERT", NActors::NLog::PRI_ALERT },
        { "EMERG", NActors::NLog::PRI_EMERG },
    };

    TString l = level;
    l.to_upper();
    const auto levelIt = levels.find(l);
    if (levelIt != levels.end()) {
        return levelIt->second;
    } else {
        Cerr << "Failed to parse test log level [" << level << "]" << Endl;
        return Nothing();
    }
}

void TKikimrRunner::SetupLogLevelFromTestParam(NKikimrServices::EServiceKikimr service) {
    if (const TString paramForService = GetTestParam(TStringBuilder() << "KQP_LOG_" << NKikimrServices::EServiceKikimr_Name(service))) {
        if (const TMaybe<NActors::NLog::EPriority> level = ParseLogLevel(paramForService)) {
            Server->GetRuntime()->SetLogPriority(service, *level);
            return;
        }
    }
    if (const TString commonParam = GetTestParam("KQP_LOG")) {
        if (const TMaybe<NActors::NLog::EPriority> level = ParseLogLevel(commonParam)) {
            Server->GetRuntime()->SetLogPriority(service, *level);
        }
    }
}

void TKikimrRunner::Initialize(const TKikimrSettings& settings) {
    // You can enable logging for these services in test using test option:
    // `--test-param KQP_LOG=<level>`
    // or `--test-param KQP_LOG_<service>=<level>`
    // For example:
    // --test-param KQP_LOG=TRACE
    // --test-param KQP_LOG_FLAT_TX_SCHEMESHARD=debug
    SetupLogLevelFromTestParam(NKikimrServices::FLAT_TX_SCHEMESHARD);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_YQL);
    SetupLogLevelFromTestParam(NKikimrServices::TX_DATASHARD);
    SetupLogLevelFromTestParam(NKikimrServices::TX_COORDINATOR);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_COMPUTE);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_TASKS_RUNNER);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_EXECUTER);
    SetupLogLevelFromTestParam(NKikimrServices::TX_PROXY_SCHEME_CACHE);
    SetupLogLevelFromTestParam(NKikimrServices::TX_PROXY);
    SetupLogLevelFromTestParam(NKikimrServices::SCHEME_BOARD_REPLICA);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_WORKER);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_SESSION);
    SetupLogLevelFromTestParam(NKikimrServices::TABLET_EXECUTOR);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_SLOW_LOG);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_PROXY);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_COMPILE_SERVICE);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_COMPILE_ACTOR);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_COMPILE_REQUEST);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_GATEWAY);
    SetupLogLevelFromTestParam(NKikimrServices::RPC_REQUEST);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_RESOURCE_MANAGER);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_NODE);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_BLOBS_STORAGE);
    SetupLogLevelFromTestParam(NKikimrServices::KQP_WORKLOAD_SERVICE);
    SetupLogLevelFromTestParam(NKikimrServices::TX_COLUMNSHARD);
    SetupLogLevelFromTestParam(NKikimrServices::LOCAL_PGWIRE);

    RunCall([this, domain = settings.DomainRoot]{
        this->Client->InitRootScheme(domain);
        return true;
    });

    if (settings.WithSampleTables) {
        RunCall([this] {
            this->CreateSampleTables();
            return true;
        });
    }
}

TString ReformatYson(const TString& yson) {
    TStringStream ysonInput(yson);
    TStringStream output;
    NYson::ReformatYsonStream(&ysonInput, &output, NYson::EYsonFormat::Text);
    return output.Str();
}

void CompareYson(const TString& expected, const TString& actual) {
    UNIT_ASSERT_NO_DIFF(ReformatYson(expected), ReformatYson(actual));
}

void CompareYson(const TString& expected, const NKikimrMiniKQL::TResult& actual) {
    TStringStream ysonStream;
    NYson::TYsonWriter writer(&ysonStream, NYson::EYsonFormat::Text);
    NYql::IDataProvider::TFillSettings fillSettings;
    bool truncated;
    KikimrResultToYson(ysonStream, writer, actual, {}, fillSettings, truncated);
    UNIT_ASSERT(!truncated);

    CompareYson(expected, ysonStream.Str());
}

bool HasIssue(const NYql::TIssues& issues, ui32 code,
    std::function<bool(const NYql::TIssue& issue)> predicate)
{
    bool hasIssue = false;

    for (auto& issue : issues) {
        WalkThroughIssues(issue, false, [code, predicate, &hasIssue] (const NYql::TIssue& issue, int level) {
            Y_UNUSED(level);
            if (issue.GetCode() == code) {
                bool issueMatch = predicate
                    ? predicate(issue)
                    : true;

                hasIssue = hasIssue || issueMatch;
            }
        });
    }

    return hasIssue;
}

void PrintQueryStats(const TDataQueryResult& result) {
    if (!result.GetStats().Defined()) {
        return;
    }

    auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

    Cerr << "------- query stats -----------" << Endl;
    for (const auto& qp : stats.query_phases()) {
        Cerr << "-- phase" << Endl
             << "     duration: " << qp.duration_us() << Endl
             << "     access:   " << Endl;
        for (const auto& ta : qp.table_access()) {
            Cerr << "       name:    " << ta.name() << Endl
                 << "       reads:   " << ta.reads().rows() << Endl
                 << "       updates: " << ta.updates().rows() << Endl
                 << "       deletes: " << ta.deletes().rows() << Endl;
        }
    }
}

void AssertTableStats(const Ydb::TableStats::QueryStats& stats, TStringBuf table,
    const TExpectedTableStats& expectedStats)
{
    ui64 actualReads = 0;
    ui64 actualUpdates = 0;
    ui64 actualDeletes = 0;

    for (const auto& phase : stats.query_phases()) {
        for (const auto& access : phase.table_access()) {
            if (access.name() == table) {
                actualReads += access.reads().rows();
                actualUpdates += access.updates().rows();
                actualDeletes += access.deletes().rows();
            }
        }
    }

    if (expectedStats.ExpectedReads) {
        UNIT_ASSERT_EQUAL_C(*expectedStats.ExpectedReads, actualReads, "table: " << table
            << ", reads expected " << *expectedStats.ExpectedReads << ", actual " << actualReads);
    }

    if (expectedStats.ExpectedUpdates) {
        UNIT_ASSERT_EQUAL_C(*expectedStats.ExpectedUpdates, actualUpdates, "table: " << table
            << ", updates expected " << *expectedStats.ExpectedUpdates << ", actual " << actualUpdates);
    }

    if (expectedStats.ExpectedDeletes) {
        UNIT_ASSERT_EQUAL_C(*expectedStats.ExpectedDeletes, actualDeletes, "table: " << table
            << ", deletes expected " << *expectedStats.ExpectedDeletes << ", actual " << actualDeletes);
    }
}

void AssertTableStats(const TDataQueryResult& result, TStringBuf table, const TExpectedTableStats& expectedStats) {
    auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
    return AssertTableStats(stats, table, expectedStats);
}

TDataQueryResult ExecQueryAndTestResult(TSession& session, const TString& query, const NYdb::TParams& params,
    const TString& expectedYson)
{
    NYdb::NTable::TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Profile);

    TDataQueryResult result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), params, settings)
            .ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    if (!result.GetIssues().Empty()) {
        Cerr << result.GetIssues().ToString() << Endl;
    }

    CompareYson(expectedYson, FormatResultSetYson(result.GetResultSet(0)));

    return result;
}

void FillProfile(NYdb::NQuery::TExecuteQueryPart& streamPart, NYson::TYsonWriter& writer, TVector<TString>* profiles,
    ui32 profileIndex)
{
    Y_UNUSED(streamPart);
    Y_UNUSED(writer);
    Y_UNUSED(profiles);
    Y_UNUSED(profileIndex);
}

void FillProfile(NYdb::NTable::TScanQueryPart& streamPart, NYson::TYsonWriter& writer, TVector<TString>* profiles,
    ui32 profileIndex)
{
    Y_UNUSED(streamPart);
    Y_UNUSED(writer);
    Y_UNUSED(profiles);
    Y_UNUSED(profileIndex);
}

void CreateLargeTable(TKikimrRunner& kikimr, ui32 rowsPerShard, ui32 keyTextSize,
    ui32 dataTextSize, ui32 batchSizeRows, ui32 fillShardsCount, ui32 largeTableKeysPerShard)
{
    kikimr.GetTestClient().CreateTable("/Root", R"(
        Name: "LargeTable"
        Columns { Name: "Key", Type: "Uint64" }
        Columns { Name: "KeyText", Type: "String" }
        Columns { Name: "Data", Type: "Int64" }
        Columns { Name: "DataText", Type: "String" }
        KeyColumnNames: ["Key", "KeyText"],
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 1000000 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 2000000 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 3000000 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 4000000 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 5000000 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 6000000 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 7000000 } } } }
    )");

    auto client = kikimr.GetTableClient();

    for (ui32 shardIdx = 0; shardIdx < fillShardsCount; ++shardIdx) {
        ui32 rowIndex = 0;
        while (rowIndex < rowsPerShard) {

            auto rowsBuilder = NYdb::TValueBuilder();
            rowsBuilder.BeginList();
            for (ui32 i = 0; i < batchSizeRows; ++i) {
                rowsBuilder.AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint64(shardIdx * largeTableKeysPerShard + rowIndex)
                    .AddMember("KeyText")
                        .OptionalString(TString(keyTextSize, '0' + (i + shardIdx) % 10))
                    .AddMember("Data")
                        .OptionalInt64(rowIndex)
                    .AddMember("DataText")
                        .OptionalString(TString(dataTextSize, '0' + (i + shardIdx + 1) % 10))
                    .EndStruct();

                ++rowIndex;
                if (rowIndex == rowsPerShard) {
                    break;
                }
            }
            rowsBuilder.EndList();

            auto result = client.BulkUpsert("/Root/LargeTable", rowsBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }
}

void CreateManyShardsTable(TKikimrRunner& kikimr, ui32 totalRows, ui32 shards, ui32 batchSizeRows)
{
    kikimr.GetTestClient().CreateTable("/Root", R"(
        Name: "ManyShardsTable"
        Columns { Name: "Key", Type: "Uint32" }
        Columns { Name: "Data", Type: "Int32" }
        KeyColumnNames: ["Key"]
        UniformPartitionsCount:
    )" + std::to_string(shards));

    auto client = kikimr.GetTableClient();

    for (ui32 rows = 0; rows < totalRows; rows += batchSizeRows) {
        auto rowsBuilder = NYdb::TValueBuilder();
        rowsBuilder.BeginList();
        for (ui32 i = 0; i < batchSizeRows && rows + i < totalRows; ++i) {
            rowsBuilder.AddListItem()
                .BeginStruct()
                .AddMember("Key")
                    .OptionalUint32((std::numeric_limits<ui32>::max() / totalRows) * (rows + i))
                .AddMember("Data")
                    .OptionalInt32(i)
                .EndStruct();
        }
        rowsBuilder.EndList();

        auto result = client.BulkUpsert("/Root/ManyShardsTable", rowsBuilder.Build()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

void PrintResultSet(const NYdb::TResultSet& resultSet, NYson::TYsonWriter& writer) {
    auto columns = resultSet.GetColumnsMeta();

    NYdb::TResultSetParser parser(resultSet);
    while (parser.TryNextRow()) {
        writer.OnListItem();
        writer.OnBeginList();
        for (ui32 i = 0; i < columns.size(); ++i) {
            writer.OnListItem();
            FormatValueYson(parser.GetValue(i), writer);
        }
        writer.OnEndList();
    }
}

bool IsTimeoutError(NYdb::EStatus status) {
    return status == NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED || status == NYdb::EStatus::TIMEOUT || status == NYdb::EStatus::CANCELLED;
}

// IssueMessageSubString - uses only in case if !streamPart.IsSuccess()
template<typename TIterator>
TString StreamResultToYsonImpl(TIterator& it, TVector<TString>* profiles, bool throwOnTimeout = false, const NYdb::EStatus& opStatus = NYdb::EStatus::SUCCESS, const TString& issueMessageSubString = "") {
    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text, ::NYson::EYsonType::Node, true);
    writer.OnBeginList();

    ui32 profileIndex = 0;

    for (;;) {
        auto streamPart = it.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
            if (opStatus != NYdb::EStatus::SUCCESS) {
                UNIT_ASSERT_VALUES_EQUAL_C(streamPart.GetStatus(), opStatus, streamPart.GetIssues().ToString());
                UNIT_ASSERT_C(streamPart.GetIssues().ToString().Contains(issueMessageSubString), TStringBuilder() << "Issue should contain '" << issueMessageSubString << "'. " << streamPart.GetIssues().ToString());
                break;
            }
            if (throwOnTimeout && IsTimeoutError(streamPart.GetStatus())) {
                throw TStreamReadError(streamPart.GetStatus());
            }
            UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
            break;
        }

        if (streamPart.HasResultSet()) {
            auto resultSet = streamPart.ExtractResultSet();
            PrintResultSet(resultSet, writer);
        }

        FillProfile(streamPart, writer, profiles, profileIndex);
        profileIndex++;
    }

    writer.OnEndList();

    return out.Str();
}

TString StreamResultToYson(NYdb::NQuery::TExecuteQueryIterator& it, bool throwOnTimeout, const NYdb::EStatus& opStatus, const TString& issueMessageSubString) {
    return StreamResultToYsonImpl(it, nullptr, throwOnTimeout, opStatus, issueMessageSubString);
}

TString StreamResultToYson(NYdb::NTable::TScanQueryPartIterator& it, bool throwOnTimeout, const NYdb::EStatus& opStatus, const TString& issueMessageSubString) {
    return StreamResultToYsonImpl(it, nullptr, throwOnTimeout, opStatus, issueMessageSubString);
}

TString StreamResultToYson(NYdb::NTable::TTablePartIterator& it, bool throwOnTimeout, const NYdb::EStatus& opStatus) {
    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text, ::NYson::EYsonType::Node, true);
    writer.OnBeginList();

    for (;;) {
        auto streamPart = it.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
            if (opStatus != NYdb::EStatus::SUCCESS) {
                UNIT_ASSERT_VALUES_EQUAL_C(streamPart.GetStatus(), opStatus, streamPart.GetIssues().ToString());
                 break;
            }
            if (throwOnTimeout && IsTimeoutError(streamPart.GetStatus())) {
                throw TStreamReadError(streamPart.GetStatus());
            }
            UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
            break;
        }

        auto resultSet = streamPart.ExtractPart();
        PrintResultSet(resultSet, writer);
    }

    writer.OnEndList();

    return out.Str();
}

TString StreamResultToYson(NYdb::NScripting::TYqlResultPartIterator& it, bool throwOnTimeout, const NYdb::EStatus& opStatus) {
    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text, ::NYson::EYsonType::Node, true);
    writer.OnBeginList();

    ui32 currentIndex = 0;
    writer.OnListItem();
    writer.OnBeginList();

    for (;;) {
        auto streamPart = it.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
            if (opStatus != NYdb::EStatus::SUCCESS) {
                UNIT_ASSERT_VALUES_EQUAL_C(streamPart.GetStatus(), opStatus, streamPart.GetIssues().ToString());
                break;
            }
            if (throwOnTimeout && IsTimeoutError(streamPart.GetStatus())) {
                throw TStreamReadError(streamPart.GetStatus());
            }
            UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
            break;
        }

        if (streamPart.HasPartialResult()) {
            const auto& partialResult = streamPart.GetPartialResult();

            ui32 resultSetIndex = partialResult.GetResultSetIndex();
            if (currentIndex != resultSetIndex) {
                currentIndex = resultSetIndex;
                writer.OnEndList();
                writer.OnListItem();
                writer.OnBeginList();
            }

            PrintResultSet(partialResult.GetResultSet(), writer);
        }
    }

    writer.OnEndList();
    writer.OnEndList();

    return out.Str();
}

static void FillPlan(const NYdb::NTable::TScanQueryPart& streamPart, TCollectedStreamResult& res) {
    if (streamPart.HasQueryStats() ) {
        res.QueryStats = NYdb::TProtoAccessor::GetProto(streamPart.GetQueryStats());

        auto plan = res.QueryStats->query_plan();
        if (!plan.empty()) {
            res.PlanJson = plan;
        }
    }
}

static void FillPlan(const NYdb::NScripting::TYqlResultPart& streamPart, TCollectedStreamResult& res) {
    if (streamPart.HasQueryStats() ) {
        res.QueryStats = NYdb::TProtoAccessor::GetProto(streamPart.GetQueryStats());

        auto plan = res.QueryStats->query_plan();
        if (!plan.empty()) {
            res.PlanJson = plan;
        }
    }
}

static void FillPlan(const NYdb::NQuery::TExecuteQueryPart& streamPart, TCollectedStreamResult& res) {
    if (streamPart.GetStats() ) {
        res.QueryStats = NYdb::TProtoAccessor::GetProto(*streamPart.GetStats());

        auto plan = res.QueryStats->query_plan();
        if (!plan.empty()) {
            res.PlanJson = plan;
        }
    }
}

template<typename TIterator>
TCollectedStreamResult CollectStreamResultImpl(TIterator& it) {
    TCollectedStreamResult res;

    TStringStream out;
    NYson::TYsonWriter resultSetWriter(&out, NYson::EYsonFormat::Text, ::NYson::EYsonType::Node, true);
    resultSetWriter.OnBeginList();

    for (;;) {
        auto streamPart = it.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
            UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
            const auto& meta = streamPart.GetResponseMetadata();
            auto mit = meta.find("x-ydb-consumed-units");
            if (mit != meta.end()) {
                res.ConsumedRuFromHeader = std::stol(mit->second);
            }
            break;
        }

        if constexpr (std::is_same_v<TIterator, NYdb::NTable::TScanQueryPartIterator>) {
            UNIT_ASSERT_C(streamPart.HasResultSet() || streamPart.HasQueryStats(),
                "Unexpected empty scan query response.");

            if (streamPart.HasResultSet()) {
                auto resultSet = streamPart.ExtractResultSet();
                PrintResultSet(resultSet, resultSetWriter);
                res.RowsCount += resultSet.RowsCount();
            }
        }

        if constexpr (std::is_same_v<TIterator, NYdb::NQuery::TExecuteQueryIterator>) {
            if (streamPart.HasResultSet()) {
                auto resultSet = streamPart.ExtractResultSet();
                PrintResultSet(resultSet, resultSetWriter);
                res.RowsCount += resultSet.RowsCount();
            }
        }

        if constexpr (std::is_same_v<TIterator, NYdb::NScripting::TYqlResultPartIterator>) {
            if (streamPart.HasPartialResult()) {
                const auto& partialResult = streamPart.GetPartialResult();
                const auto& resultSet = partialResult.GetResultSet();
                PrintResultSet(resultSet, resultSetWriter);
                res.RowsCount += resultSet.RowsCount();
            }
        }

        if constexpr (std::is_same_v<TIterator, NYdb::NTable::TScanQueryPartIterator>
                || std::is_same_v<TIterator, NYdb::NScripting::TYqlResultPartIterator>
                || std::is_same_v<TIterator, NYdb::NQuery::TExecuteQueryIterator>) {
            FillPlan(streamPart, res);
        } else {
            if (streamPart.HasPlan()) {
                res.PlanJson = streamPart.ExtractPlan();
            }
        }
    }

    resultSetWriter.OnEndList();

    res.ResultSetYson = out.Str();
    return res;
}

template<typename TIterator>
TCollectedStreamResult CollectStreamResult(TIterator& it) {
    return CollectStreamResultImpl(it);
}

template TCollectedStreamResult CollectStreamResult(NYdb::NTable::TScanQueryPartIterator& it);
template TCollectedStreamResult CollectStreamResult(NYdb::NScripting::TYqlResultPartIterator& it);
template TCollectedStreamResult CollectStreamResult(NYdb::NQuery::TExecuteQueryIterator& it);

TString ReadTableToYson(NYdb::NTable::TSession session, const TString& table) {
    TReadTableSettings settings;
    settings.Ordered(true);
    auto it = session.ReadTable(table, settings).GetValueSync();
    UNIT_ASSERT(it.IsSuccess());
    return StreamResultToYson(it);
}

TString ReadTablePartToYson(NYdb::NTable::TSession session, const TString& table) {
    TReadTableSettings settings;
    settings.Ordered(true);
    auto it = session.ReadTable(table, settings).GetValueSync();
    UNIT_ASSERT(it.IsSuccess());

    TReadTableResultPart streamPart = it.ReadNext().GetValueSync();
    if (!streamPart.IsSuccess()) {
        streamPart.GetIssues().PrintTo(Cerr);
        if (streamPart.EOS()) {
            return {};
        }
        UNIT_ASSERT_C(false, "Status: " << streamPart.GetStatus());
    }
    return NYdb::FormatResultSetYson(streamPart.ExtractPart());
}

bool ValidatePlanNodeIds(const NJson::TJsonValue& plan) {
    ui32 planNodeId = 0;
    ui32 count = 0;

    do {
        count = CountPlanNodesByKv(plan, "PlanNodeId", std::to_string(++planNodeId));
        if (count > 1) {
            return false;
        }
    } while (count > 0);

    return true;
}

ui32 CountPlanNodesByKv(const NJson::TJsonValue& plan, const TString& key, const TString& value) {
    ui32 result = 0;

    if (plan.IsArray()) {
        for (const auto &node: plan.GetArray()) {
            result += CountPlanNodesByKv(node, key, value);
        }
        return result;
    }

    UNIT_ASSERT(plan.IsMap());

    auto map = plan.GetMap();
    if (map.contains(key) && map.at(key).GetStringRobust() == value) {
        return 1;
    }

    if (map.contains("Plans")) {
        for (const auto &node: map["Plans"].GetArraySafe()) {
            result += CountPlanNodesByKv(node, key, value);
        }
    }

    if (map.contains("Plan")) {
        result += CountPlanNodesByKv(map.at("Plan"), key, value);
    }

    if (map.contains("Operators")) {
        for (const auto &node : map["Operators"].GetArraySafe()) {
            result += CountPlanNodesByKv(node, key, value);
        }
    }

    return result;
}

NJson::TJsonValue FindPlanNodeByKv(const NJson::TJsonValue& plan, const TString& key, const TString& value) {
    if (plan.IsArray()) {
        for (const auto &node: plan.GetArray()) {
            auto stage = FindPlanNodeByKv(node, key, value);
            if (stage.IsDefined()) {
                return stage;
            }
        }
    } else if (plan.IsMap()) {
        auto map = plan.GetMap();
        if (map.contains(key) && map.at(key).GetStringRobust() == value) {
            return plan;
        }
        if (map.contains("Plans")) {
            for (const auto &node: map["Plans"].GetArraySafe()) {
                auto stage = FindPlanNodeByKv(node, key, value);
                if (stage.IsDefined()) {
                    return stage;
                }
            }
        } else if (map.contains("Plan")) {
            auto stage = FindPlanNodeByKv(map.at("Plan"), key, value);
            if (stage.IsDefined()) {
                return stage;
            }
        }

        if (map.contains("Operators")) {
            for (const auto &node : map["Operators"].GetArraySafe()) {
                auto op = FindPlanNodeByKv(node, key, value);
                if (op.IsDefined()) {
                    return op;
                }
            }
        }

        if (map.contains("queries")) {
            for (const auto &node : map["queries"].GetArraySafe()) {
                auto op = FindPlanNodeByKv(node, key, value);
                if (op.IsDefined()) {
                    return op;
                }
            }
        }
    } else {
        Y_ASSERT(false);
    }

    return NJson::TJsonValue();
}

void FindPlanNodesImpl(const NJson::TJsonValue& node, const TString& key, std::vector<NJson::TJsonValue>& results) {
    if (node.IsArray()) {
        for (const auto& item: node.GetArray()) {
            FindPlanNodesImpl(item, key, results);
        }
    }

    if (!node.IsMap()) {
        return;
    }

    if (auto* valueNode = node.GetValueByPath(key)) {
        results.push_back(*valueNode);
    }

    for (const auto& [_, value]: node.GetMap()) {
        FindPlanNodesImpl(value, key, results);
    }
}

void FindPlanStagesImpl(const NJson::TJsonValue& node, std::vector<NJson::TJsonValue>& stages) {
    if (node.IsArray()) {
        for (const auto& item: node.GetArray()) {
            FindPlanStagesImpl(item, stages);
        }
    }

    if (!node.IsMap()) {
        return;
    }

    auto map = node.GetMap();
    // TODO: Use explicit PlanNodeType for stages
    if (map.contains("Node Type") && !map.contains("PlanNodeType")) {
        stages.push_back(node);
    }

    for (const auto& [_, value]: map) {
        FindPlanStagesImpl(value, stages);
    }
}

std::vector<NJson::TJsonValue> FindPlanNodes(const NJson::TJsonValue& plan, const TString& key) {
    std::vector<NJson::TJsonValue> results;
    FindPlanNodesImpl(plan, key, results);
    return results;
}

std::vector<NJson::TJsonValue> FindPlanStages(const NJson::TJsonValue& plan) {
    std::vector<NJson::TJsonValue> stages;
    FindPlanStagesImpl(plan.GetMapSafe().at("Plan"), stages);    
    return stages;
}

void CreateSampleTablesWithIndex(TSession& session, bool populateTables) {
    auto res = session.ExecuteSchemeQuery(R"(
        --!syntax_v1
        CREATE TABLE `/Root/SecondaryKeys` (
            Key Int32,
            Fk Int32,
            Value String,
            PRIMARY KEY (Key),
            INDEX Index GLOBAL ON (Fk)
        );
        CREATE TABLE `/Root/SecondaryComplexKeys` (
            Key Int32,
            Fk1 Int32,
            Fk2 String,
            Value String,
            PRIMARY KEY (Key),
            INDEX Index GLOBAL ON (Fk1, Fk2)
        );
        CREATE TABLE `/Root/SecondaryWithDataColumns` (
            Key String,
            Index2 String,
            Value String,
            ExtPayload String,
            PRIMARY KEY (Key),
            INDEX Index GLOBAL ON (Index2)
            COVER (Value)
        )

    )").GetValueSync();
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

    if (!populateTables)
        return;

    auto result = session.ExecuteDataQuery(R"(

        REPLACE INTO `KeyValue` (Key, Value) VALUES
            (3u,   "Three"),
            (4u,   "Four"),
            (10u,  "Ten"),
            (NULL, "Null Value");

        REPLACE INTO `Test` (Group, Name, Amount, Comment) VALUES
            (1u, "Jack",     100500ul, "Just Jack"),
            (3u, "Harry",    5600ul,   "Not Potter"),
            (3u, "Joshua",   8202ul,   "Very popular name in GB"),
            (3u, "Muhammad", 887773ul, "Also very popular name in GB"),
            (4u, "Hugo",     77,       "Boss");

        REPLACE INTO `/Root/SecondaryKeys` (Key, Fk, Value) VALUES
            (1,    1,    "Payload1"),
            (2,    2,    "Payload2"),
            (5,    5,    "Payload5"),
            (NULL, 6,    "Payload6"),
            (7,    NULL, "Payload7"),
            (NULL, NULL, "Payload8");

        REPLACE INTO `/Root/SecondaryComplexKeys` (Key, Fk1, Fk2, Value) VALUES
            (1,    1,    "Fk1", "Payload1"),
            (2,    2,    "Fk2", "Payload2"),
            (5,    5,    "Fk5", "Payload5"),
            (NULL, 6,    "Fk6", "Payload6"),
            (7,    NULL, "Fk7", "Payload7"),
            (NULL, NULL, NULL,  "Payload8");

        REPLACE INTO `/Root/SecondaryWithDataColumns` (Key, Index2, Value) VALUES
            ("Primary1", "Secondary1", "Value1");

    )", TTxControl::BeginTx().CommitTx()).GetValueSync();

    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void InitRoot(Tests::TServer::TPtr server, TActorId sender) {
    server->SetupRootStoragePools(sender);
}

THolder<NSchemeCache::TSchemeCacheNavigate> Navigate(TTestActorRuntime& runtime, const TActorId& sender,
                                                     const TString& path, NSchemeCache::TSchemeCacheNavigate::EOp op)
{
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TEvRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TEvResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;

    auto request = MakeHolder<TNavigate>();
    auto& entry = request->ResultSet.emplace_back();
    entry.Path = SplitPath(path);
    entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
    entry.Operation = op;
    entry.ShowPrivatePath = true;
    runtime.Send(MakeSchemeCacheID(), sender, new TEvRequest(request.Release()));

    auto ev = runtime.GrabEdgeEventRethrow<TEvResponse>(sender);
    UNIT_ASSERT(ev);
    UNIT_ASSERT(ev->Get());

    auto* response = ev->Get()->Request.Release();
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->ResultSet.size(), 1);

    return THolder(response);
}

 NKikimrScheme::TEvDescribeSchemeResult DescribeTable(Tests::TServer* server,
                                                        TActorId sender,
                                                        const TString &path)
{
    auto &runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;

    auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
    request->Record.MutableDescribePath()->SetPath(path);
    request->Record.MutableDescribePath()->MutableOptions()->SetShowPrivateTable(true);
    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
    auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(handle);

    return *reply->MutableRecord();
}

TVector<ui64> GetTableShards(Tests::TServer* server,
                            TActorId sender,
                            const TString &path)
{
    TVector<ui64> shards;
    auto lsResult = DescribeTable(server, sender, path);
    for (auto &part : lsResult.GetPathDescription().GetTablePartitions())
        shards.push_back(part.GetDatashardId());

    return shards;
}

TVector<ui64> GetColumnTableShards(Tests::TServer* server,
                                   TActorId sender,
                                   const TString &path)
{
    TVector<ui64> shards;
    auto lsResult = DescribeTable(server, sender, path);
    for (auto &part : lsResult.GetPathDescription().GetColumnTableDescription().GetSharding().GetColumnShards())
        shards.push_back(part);

    return shards;
}

TVector<ui64> GetTableShards(Tests::TServer::TPtr server,
                                TActorId sender,
                                const TString &path) {
    return GetTableShards(server.Get(), sender, path);
 }

void WaitForZeroSessions(const NKqp::TKqpCounters& counters) {
    int count = 60;
    while (counters.GetActiveSessionActors()->Val() != 0 && count) {
        count--;
        Sleep(TDuration::Seconds(1));
    }

    UNIT_ASSERT_C(count, "Unable to wait for proper active session count, it looks like cancelation doesn`t work");
}

NJson::TJsonValue SimplifyPlan(NJson::TJsonValue& opt) {
    if (auto ops = opt.GetMapSafe().find("Operators"); ops != opt.GetMapSafe().end()) {
        auto opName = ops->second.GetArraySafe()[0].GetMapSafe().at("Name").GetStringSafe();
        if (opName.find("Join") != TString::npos || opName.find("Union") != TString::npos ) {
            NJson::TJsonValue newChildren;

            for (auto c : opt.GetMapSafe().at("Plans").GetArraySafe()) {
                newChildren.AppendValue(SimplifyPlan(c));
            }

            opt["Plans"] = newChildren;
            return opt;
        }
        else if (opName.find("Table") != TString::npos ) {
            return opt;
        }
    }

    auto firstPlan = opt.GetMapSafe().at("Plans").GetArraySafe()[0];
    return SimplifyPlan(firstPlan);
}

bool JoinOrderAndAlgosMatch(const NJson::TJsonValue& opt, const NJson::TJsonValue& ref) {
    auto op = opt.GetMapSafe().at("Operators").GetArraySafe()[0];
    if (op.GetMapSafe().at("Name").GetStringSafe() != ref.GetMapSafe().at("op_name").GetStringSafe()) {
        return false;
    }

    auto refMap = ref.GetMapSafe();
    if (auto args = refMap.find("args"); args != refMap.end()){
        if (!opt.GetMapSafe().contains("Plans")){
            return false;
        }
        auto subplans = opt.GetMapSafe().at("Plans").GetArraySafe();
        if (args->second.GetArraySafe().size() != subplans.size()) {
            return false;
        }
        for (size_t i=0; i<subplans.size(); i++) {
            if (!JoinOrderAndAlgosMatch(subplans[i],args->second.GetArraySafe()[i])) {
                return false;
            }
        }
        return true;
    } else {
        if (!op.GetMapSafe().contains("Table")) {
            return false;
        }
        return op.GetMapSafe().at("Table").GetStringSafe() == refMap.at("table").GetStringSafe();
    }
}

bool JoinOrderAndAlgosMatch(const TString& optimized, const TString& reference){
    NJson::TJsonValue optRoot;
    NJson::ReadJsonTree(optimized, &optRoot, true);
    optRoot = SimplifyPlan(optRoot.GetMapSafe().at("SimplifiedPlan"));
    
    NJson::TJsonValue refRoot;
    NJson::ReadJsonTree(reference, &refRoot, true);

    return JoinOrderAndAlgosMatch(optRoot, refRoot);
}

/* Temporary solution to canonize tests */
NJson::TJsonValue CanonizeJoinOrderImpl(const NJson::TJsonValue& opt) {
    NJson::TJsonValue res;

    auto op = opt.GetMapSafe().at("Operators").GetArraySafe()[0];
    res["op_name"] = op.GetMapSafe().at("Name").GetStringSafe();


    if (!opt.GetMapSafe().contains("Plans")) {
        res["table"] = op.GetMapSafe().at("Table").GetStringSafe();
        return res;
    }
    
    auto subplans = opt.GetMapSafe().at("Plans").GetArraySafe();
    for (size_t i = 0; i< subplans.size(); ++i) {
        res["args"].AppendValue(CanonizeJoinOrderImpl(subplans[i]));
    }
    return res;
}

/* Temporary solution to canonize tests */
NJson::TJsonValue CanonizeJoinOrder(const TString& deserializedPlan) {
    NJson::TJsonValue optRoot;
    NJson::ReadJsonTree(deserializedPlan, &optRoot, true);
    optRoot = SimplifyPlan(optRoot.GetMapSafe().at("SimplifiedPlan"));
    return CanonizeJoinOrderImpl(SimplifyPlan(optRoot));
}

} // namspace NKqp
} // namespace NKikimr
