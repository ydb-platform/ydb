#include "kqp_ut_common.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/utils/yql_panic.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb::NTable;

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

NMiniKQL::IFunctionRegistry* UdfFrFactory(const NScheme::TTypeRegistry& typeRegistry) {
    Y_UNUSED(typeRegistry);
    auto funcRegistry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry())->Clone();
    funcRegistry->AddModule("", "TestUdfs", new TTestUdfsModule());
    funcRegistry->AddModule("", "Json2", CreateJson2Module());
    funcRegistry->AddModule("", "Re2", CreateRe2Module());
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
    // EnableKikimrBacktraceFormat(); // Very slow, enable only when required locally

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

    ServerSettings.Reset(MakeHolder<Tests::TServerSettings>(mbusPort, NKikimrProto::TAuthConfig(), settings.PQConfig));
    ServerSettings->SetDomainName(settings.DomainRoot);
    ServerSettings->SetKqpSettings(effectiveKqpSettings);
    ServerSettings->SetAppConfig(settings.AppConfig);
    ServerSettings->SetFeatureFlags(settings.FeatureFlags);
    ServerSettings->SetNodeCount(settings.NodeCount);
    ServerSettings->SetEnableKqpSpilling(enableSpilling);
    ServerSettings->SetEnableDataColumnForIndexTable(true);
    ServerSettings->SetKeepSnapshotTimeout(settings.KeepSnapshotTimeout);
    ServerSettings->SetFrFactory(&UdfFrFactory);
    ServerSettings->SetEnableNotNullColumns(true);
    ServerSettings->SetEnableMoveIndex(true);
  
    if (settings.FeatureFlags.GetEnableKqpImmediateEffects()) {
        Tests::TServerSettings::TControls controls;
        controls.MutableDataShardControls()->SetPrioritizedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetUnprotectedMvccSnapshotReads(1);
        controls.MutableDataShardControls()->SetEnableLockedWrites(1);

        ServerSettings->SetControls(controls);
    }

    if (settings.LogStream)
        ServerSettings->SetLogBackend(new TStreamLogBackend(settings.LogStream));

    Server.Reset(MakeHolder<Tests::TServer>(*ServerSettings));
    Server->EnableGRpc(grpcPort);
    Server->SetupDefaultProfiles();

    Client.Reset(MakeHolder<Tests::TClient>(*ServerSettings));

    Endpoint = "localhost:" + ToString(grpcPort);

    DriverConfig = NYdb::TDriverConfig()
        .SetEndpoint(Endpoint)
        .SetDatabase("/" + settings.DomainRoot)
        .SetDiscoveryMode(NYdb::EDiscoveryMode::Async)
        .SetAuthToken(settings.AuthToken);
    Driver.Reset(MakeHolder<NYdb::TDriver>(DriverConfig));

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

void TKikimrRunner::Initialize(const TKikimrSettings& settings) {
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_TASKS_RUNNER, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_TRACE);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_WORKER, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_SESSION, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_SLOW_LOG, NActors::NLog::PRI_TRACE);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_ACTOR, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_REQUEST, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_GATEWAY, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::RPC_REQUEST, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_NODE, NActors::NLog::PRI_DEBUG);
    // Server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_BLOBS_STORAGE, NActors::NLog::PRI_DEBUG);

    Client->InitRootScheme(settings.DomainRoot);

    NKikimr::NKqp::WaitForKqpProxyInit(GetDriver());

    if (settings.WithSampleTables) {
        CreateSampleTables();
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

void AssertTableStats(const TDataQueryResult& result, TStringBuf table, const TExpectedTableStats& expectedStats) {
    auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

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

void FillProfile(NYdb::NTable::TScanQueryPart& streamPart, NYson::TYsonWriter& writer, TVector<TString>* profiles,
    ui32 profileIndex)
{
    Y_UNUSED(streamPart);
    Y_UNUSED(writer);
    Y_UNUSED(profiles);
    Y_UNUSED(profileIndex);
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
    return status == NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED || status == NYdb::EStatus::TIMEOUT;
}

template<typename TIterator>
TString StreamResultToYsonImpl(TIterator& it, TVector<TString>* profiles, bool throwOnTimeout = false) {
    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text, ::NYson::EYsonType::Node, true);
    writer.OnBeginList();

    ui32 profileIndex = 0;

    for (;;) {
        auto streamPart = it.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
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

TString StreamResultToYson(NYdb::NTable::TScanQueryPartIterator& it, bool throwOnTimeout) {
    return StreamResultToYsonImpl(it, nullptr, throwOnTimeout);
}

TString StreamResultToYson(NYdb::NTable::TTablePartIterator& it, bool throwOnTimeout) {
    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text, ::NYson::EYsonType::Node, true);
    writer.OnBeginList();

    ui32 profileIndex = 0;

    for (;;) {
        auto streamPart = it.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
            if (throwOnTimeout && IsTimeoutError(streamPart.GetStatus())) {
                throw TStreamReadError(streamPart.GetStatus());
            }
            UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
            break;
        }

        auto resultSet = streamPart.ExtractPart();
        PrintResultSet(resultSet, writer);

        profileIndex++;
    }

    writer.OnEndList();

    return out.Str();
}

TString StreamResultToYson(NYdb::NScripting::TYqlResultPartIterator& it, bool throwOnTimeout) {
    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text, ::NYson::EYsonType::Node, true);
    writer.OnBeginList();

    ui32 currentIndex = 0;
    writer.OnListItem();
    writer.OnBeginList();

    for (;;) {
        auto streamPart = it.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
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
            break;
        }

        if constexpr (std::is_same_v<TIterator, NYdb::NTable::TScanQueryPartIterator>) {
            UNIT_ASSERT_C(streamPart.HasResultSet() || streamPart.HasQueryStats(),
                "Unexpected empty scan query response.");
        }

        if (streamPart.HasResultSet()) {
            auto resultSet = streamPart.ExtractResultSet();
            PrintResultSet(resultSet, resultSetWriter);
            res.RowsCount += resultSet.RowsCount();
        }

        if constexpr (std::is_same_v<TIterator, NYdb::NTable::TScanQueryPartIterator>) {
            if (streamPart.HasQueryStats() ) {
                res.QueryStats = NYdb::TProtoAccessor::GetProto(streamPart.GetQueryStats());

                auto plan = res.QueryStats->query_plan();
                if (!plan.empty()) {
                    res.PlanJson = plan;
                }
            }
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

TCollectedStreamResult CollectStreamResult(NYdb::NTable::TScanQueryPartIterator& it) {
    return CollectStreamResultImpl(it);
}

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

    auto map = node.GetMap();
    if (map.contains(key)) {
        results.push_back(map.at(key));
    }

    for (const auto& [_, value]: map) {
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
    FindPlanStagesImpl(plan, stages);
    return stages;
}

void CreateSampleTablesWithIndex(TSession& session) {
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

void WaitForKqpProxyInit(const NYdb::TDriver& driver) {
    NYdb::NTable::TTableClient client(driver);

    while (true) {
        auto it = client.RetryOperationSync([=](TSession session) {
            return session.ExecuteDataQuery(R"(
                        SELECT 1;
                    )",
                    TTxControl::BeginTx().CommitTx()
                ).GetValueSync();
        });

        if (it.IsSuccess()) {
            break;
        }
        Sleep(TDuration::MilliSeconds(100));
    }
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
    TVector<ui64> shards;

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

TVector<ui64> GetTableShards(Tests::TServer::TPtr server,
                                TActorId sender,
                                const TString &path) {
    return GetTableShards(server.Get(), sender, path);
 }

} // namspace NKqp
} // namespace NKikimr
