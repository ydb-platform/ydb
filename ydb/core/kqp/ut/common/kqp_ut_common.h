#pragma once

#include <ydb/core/testlib/test_client.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/writer.h>
#include <library/cpp/threading/future/async.h>


#define Y_UNIT_TEST_TWIN(N, OPT)                                                                                   \
    template <bool OPT>                                                                                            \
    struct TTestCase##N : public TCurrentTestCase {                                                                \
        TTestCase##N() : TCurrentTestCase() {                                                                      \
            if constexpr (OPT) { Name_ = #N "+" #OPT; } else { Name_ = #N "-" #OPT; }                              \
        }                                                                                                          \
        static THolder<NUnitTest::TBaseTestCase> CreateOn()  { return ::MakeHolder<TTestCase##N<true>>();  }       \
        static THolder<NUnitTest::TBaseTestCase> CreateOff() { return ::MakeHolder<TTestCase##N<false>>(); }       \
        void Execute_(NUnitTest::TTestContext&) override;                                                          \
    };                                                                                                             \
    struct TTestRegistration##N {                                                                                  \
        TTestRegistration##N() {                                                                                   \
            TCurrentTest::AddTest(TTestCase##N<true>::CreateOn);                                                   \
            TCurrentTest::AddTest(TTestCase##N<false>::CreateOff);                                                 \
        }                                                                                                          \
    };                                                                                                             \
    static TTestRegistration##N testRegistration##N;                                                               \
    template <bool OPT>                                                                                            \
    void TTestCase##N<OPT>::Execute_(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)

#define Y_UNIT_TEST_QUAD(N, OPT1, OPT2)                                                                                              \
    template<bool OPT1, bool OPT2> void N(NUnitTest::TTestContext&);                                                                 \
    struct TTestRegistration##N {                                                                                                    \
        TTestRegistration##N() {                                                                                                     \
            TCurrentTest::AddTest(#N "-" #OPT1 "-" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false, false>), false); \
            TCurrentTest::AddTest(#N "+" #OPT1 "-" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true, false>), false);  \
            TCurrentTest::AddTest(#N "-" #OPT1 "+" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false, true>), false);  \
            TCurrentTest::AddTest(#N "+" #OPT1 "+" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true, true>), false);   \
        }                                                                                                                            \
    };                                                                                                                               \
    static TTestRegistration##N testRegistration##N;                                                                                 \
    template<bool OPT1, bool OPT2>                                                                                                   \
    void N(NUnitTest::TTestContext&)

template <bool ForceVersionV1>
TString MakeQuery(const TString& tmpl) {
    return TStringBuilder()
        << (ForceVersionV1 ? "--!syntax_v1\n" : "")
        << tmpl;
}

#define Q_(expr) MakeQuery<false>(expr)
#define Q1_(expr) MakeQuery<true>(expr)

namespace NKikimr {
namespace NKqp {

class TKqpCounters;
const TString KikimrDefaultUtDomainRoot = "Root";

extern const TString EXPECTED_EIGHTSHARD_VALUE1;

TVector<NKikimrKqp::TKqpSetting> SyntaxV1Settings();

struct TKikimrSettings: public TTestFeatureFlagsHolder<TKikimrSettings> {
    NKikimrConfig::TAppConfig AppConfig;
    NKikimrPQ::TPQConfig PQConfig;
    TVector<NKikimrKqp::TKqpSetting> KqpSettings;
    TString AuthToken;
    TString DomainRoot = KikimrDefaultUtDomainRoot;
    ui32 NodeCount = 1;
    bool WithSampleTables = true;
    bool UseRealThreads = true;
    TDuration KeepSnapshotTimeout = TDuration::Zero();
    IOutputStream* LogStream = nullptr;
    TMaybe<NFake::TStorage> Storage = Nothing();
    NKqp::IKqpFederatedQuerySetupFactory::TPtr FederatedQuerySetupFactory = std::make_shared<NKqp::TKqpFederatedQuerySetupFactoryNoop>();
    NMonitoring::TDynamicCounterPtr CountersRoot = MakeIntrusive<NMonitoring::TDynamicCounters>();
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> S3ActorsFactory = NYql::NDq::CreateDefaultS3ActorsFactory();

    TKikimrSettings()
    {
        auto* tableServiceConfig = AppConfig.MutableTableServiceConfig();
        auto* infoExchangerRetrySettings = tableServiceConfig->MutableResourceManager()->MutableInfoExchangerSettings();
        auto* exchangerSettings = infoExchangerRetrySettings->MutableExchangerSettings();
        exchangerSettings->SetStartDelayMs(10);
        exchangerSettings->SetMaxDelayMs(10);
        AppConfig.MutableColumnShardConfig()->SetDisabledOnSchemeShard(false);
    }

    TKikimrSettings& SetAppConfig(const NKikimrConfig::TAppConfig& value) { AppConfig = value; return *this; }
    TKikimrSettings& SetFeatureFlags(const NKikimrConfig::TFeatureFlags& value) { FeatureFlags = value; return *this; }
    TKikimrSettings& SetPQConfig(const NKikimrPQ::TPQConfig& value) { PQConfig = value; return *this; };
    TKikimrSettings& SetKqpSettings(const TVector<NKikimrKqp::TKqpSetting>& value) { KqpSettings = value; return *this; }
    TKikimrSettings& SetAuthToken(const TString& value) { AuthToken = value; return *this; }
    TKikimrSettings& SetDomainRoot(const TString& value) { DomainRoot = value; return *this; }
    TKikimrSettings& SetNodeCount(ui32 value) { NodeCount = value; return *this; }
    TKikimrSettings& SetWithSampleTables(bool value) { WithSampleTables = value; return *this; }
    TKikimrSettings& SetKeepSnapshotTimeout(TDuration value) { KeepSnapshotTimeout = value; return *this; }
    TKikimrSettings& SetLogStream(IOutputStream* follower) { LogStream = follower; return *this; };
    TKikimrSettings& SetStorage(const NFake::TStorage& storage) { Storage = storage; return *this; };
    TKikimrSettings& SetFederatedQuerySetupFactory(NKqp::IKqpFederatedQuerySetupFactory::TPtr value) { FederatedQuerySetupFactory = value; return *this; };
    TKikimrSettings& SetUseRealThreads(bool value) { UseRealThreads = value; return *this; };
    TKikimrSettings& SetS3ActorsFactory(std::shared_ptr<NYql::NDq::IS3ActorsFactory> value) { S3ActorsFactory = std::move(value); return *this; };
};

class TKikimrRunner {
public:
    TKikimrRunner(const TKikimrSettings& settings);

    TKikimrRunner(const TVector<NKikimrKqp::TKqpSetting>& kqpSettings, const TString& authToken = "",
        const TString& domainRoot = KikimrDefaultUtDomainRoot, ui32 nodeCount = 1);

    TKikimrRunner(const NKikimrConfig::TAppConfig& appConfig, const TString& authToken = "",
        const TString& domainRoot = KikimrDefaultUtDomainRoot, ui32 nodeCount = 1);

    TKikimrRunner(const NKikimrConfig::TAppConfig& appConfig, const TVector<NKikimrKqp::TKqpSetting>& kqpSettings,
        const TString& authToken = "", const TString& domainRoot = KikimrDefaultUtDomainRoot, ui32 nodeCount = 1);

    TKikimrRunner(const NKikimrConfig::TFeatureFlags& featureFlags, const TString& authToken = "",
        const TString& domainRoot = KikimrDefaultUtDomainRoot, ui32 nodeCount = 1);

    TKikimrRunner(const TString& authToken = "", const TString& domainRoot = KikimrDefaultUtDomainRoot,
        ui32 nodeCount = 1);

    TKikimrRunner(const NFake::TStorage& storage);

    ~TKikimrRunner() {
        RunCall([&] { Driver->Stop(true); return false; });
        if (ThreadPoolStarted_) {
            ThreadPool.Stop();
        }

        UNIT_ASSERT_C(WaitHttpGatewayFinalization(CountersRoot), "Failed to finalize http gateway before destruction");

        Server.Reset();
        Client.Reset();
    }

    const TString& GetEndpoint() const { return Endpoint; }
    const NYdb::TDriver& GetDriver() const { return *Driver; }
    NYdb::NScheme::TSchemeClient GetSchemeClient() const { return NYdb::NScheme::TSchemeClient(*Driver); }
    Tests::TClient& GetTestClient() const { return *Client; }
    Tests::TServer& GetTestServer() const { return *Server; }

    NYdb::TDriverConfig GetDriverConfig() const { return DriverConfig; }

    NYdb::NTable::TTableClient GetTableClient() const {
        return NYdb::NTable::TTableClient(*Driver, NYdb::NTable::TClientSettings()
            .UseQueryCache(false));
    }

    NYdb::NQuery::TQueryClient GetQueryClient(
        NYdb::NQuery::TClientSettings settings = NYdb::NQuery::TClientSettings()) const
    {
        return NYdb::NQuery::TQueryClient(*Driver, settings);
    }

    template <typename Func>
    NThreading::TFuture<NThreading::TFutureType<TFunctionResult<Func>>> RunInThreadPool(Func&& func) {
        if (!ThreadPoolStarted_) {
            ThreadPool.Start();
            ThreadPoolStarted_ = true;
        }

        return NThreading::Async(std::move(func), ThreadPool);
    }

    template <typename Func>
    TFunctionResult<Func> RunCall(Func&& func) {
        auto future = RunInThreadPool(std::move(func));
        return GetTestServer().GetRuntime()->WaitFuture(future);
    }

private:
    void Initialize(const TKikimrSettings& settings);
    void WaitForKqpProxyInit();
    void CreateSampleTables();
    void SetupLogLevelFromTestParam(NKikimrServices::EServiceKikimr service);

private:
    THolder<Tests::TServerSettings> ServerSettings;
    THolder<Tests::TServer> Server;
    THolder<Tests::TClient> Client;
    TAdaptiveThreadPool ThreadPool;
    bool ThreadPoolStarted_ = false;
    TPortManager PortManager;
    TString Endpoint;
    NYdb::TDriverConfig DriverConfig;
    THolder<NYdb::TDriver> Driver;
    NMonitoring::TDynamicCounterPtr CountersRoot;
};

inline TKikimrRunner DefaultKikimrRunner(TVector<NKikimrKqp::TKqpSetting> kqpSettings = {},
    const NKikimrConfig::TAppConfig& appConfig = {})
{
    auto settings = TKikimrSettings()
        .SetAppConfig(appConfig)
        .SetKqpSettings(kqpSettings)
        .SetEnableScriptExecutionOperations(true);

    return TKikimrRunner{settings};
}

struct TCollectedStreamResult {
    TString ResultSetYson;
    TMaybe<TString> PlanJson;
    TMaybe<Ydb::TableStats::QueryStats> QueryStats;
    ui64 RowsCount = 0;
    ui64 ConsumedRuFromHeader = 0;
};

template<typename TIterator>
TCollectedStreamResult CollectStreamResult(TIterator& it);

enum class EIndexTypeSql {
    Global,
    GlobalSync,
    GlobalAsync,
    GlobalVectorKMeansTree,
};

inline constexpr TStringBuf IndexTypeSqlString(EIndexTypeSql type) {
    switch (type) {
    case EIndexTypeSql::Global:
        return "GLOBAL";
    case EIndexTypeSql::GlobalSync:
        return "GLOBAL SYNC";
    case EIndexTypeSql::GlobalAsync:
        return "GLOBAL ASYNC";
    case NKqp::EIndexTypeSql::GlobalVectorKMeansTree:
        return "GLOBAL";
    }
}

inline NYdb::NTable::EIndexType IndexTypeSqlToIndexType(EIndexTypeSql type) {
    switch (type) {
    case EIndexTypeSql::Global:
    case EIndexTypeSql::GlobalSync:
        return NYdb::NTable::EIndexType::GlobalSync;
    case EIndexTypeSql::GlobalAsync:
        return NYdb::NTable::EIndexType::GlobalAsync;
    case EIndexTypeSql::GlobalVectorKMeansTree:
        return NYdb::NTable::EIndexType::GlobalVectorKMeansTree;
    }
}

inline constexpr TStringBuf IndexSubtypeSqlString(EIndexTypeSql type) {
    switch (type) {
    case EIndexTypeSql::Global:
    case EIndexTypeSql::GlobalSync:
    case EIndexTypeSql::GlobalAsync:
        return "";
    case NKqp::EIndexTypeSql::GlobalVectorKMeansTree:
        return "USING vector_kmeans_tree";
    }
}

inline constexpr TStringBuf IndexWithSqlString(EIndexTypeSql type) {
    switch (type) {
    case EIndexTypeSql::Global:
    case EIndexTypeSql::GlobalSync:
    case EIndexTypeSql::GlobalAsync:
        return "";
    case NKqp::EIndexTypeSql::GlobalVectorKMeansTree:
        return "WITH (similarity=inner_product, vector_type=float, vector_dimension=1024)";
    }
}

TString ReformatYson(const TString& yson);
void CompareYson(const TString& expected, const TString& actual);
void CompareYson(const TString& expected, const NKikimrMiniKQL::TResult& actual);

void CreateLargeTable(TKikimrRunner& kikimr, ui32 rowsPerShard, ui32 keyTextSize,
    ui32 dataTextSize, ui32 batchSizeRows = 100, ui32 fillShardsCount = 8, ui32 largeTableKeysPerShard = 1000000);

void CreateManyShardsTable(TKikimrRunner& kikimr, ui32 totalRows = 1000, ui32 shards = 100, ui32 batchSizeRows = 1000);

bool HasIssue(const NYql::TIssues& issues, ui32 code,
    std::function<bool(const NYql::TIssue& issue)> predicate = {});

void PrintQueryStats(const NYdb::NTable::TDataQueryResult& result);

struct TExpectedTableStats {
    TMaybe<ui64> ExpectedReads;
    TMaybe<ui64> ExpectedUpdates;
    TMaybe<ui64> ExpectedDeletes;
};

void AssertTableStats(const Ydb::TableStats::QueryStats& stats, TStringBuf table,
    const TExpectedTableStats& expectedStats);

void AssertTableStats(const NYdb::NTable::TDataQueryResult& result, TStringBuf table,
    const TExpectedTableStats& expectedStats);

void AssertTableStats(const NYdb::NTable::TDataQueryResult& result, TStringBuf table,
    const TExpectedTableStats& expectedStats);

inline void AssertTableReads(const NYdb::NTable::TDataQueryResult& result, TStringBuf table, ui64 expectedReads) {
    AssertTableStats(result, table, { .ExpectedReads = expectedReads });
}

NYdb::NTable::TDataQueryResult ExecQueryAndTestResult(NYdb::NTable::TSession& session, const TString& query,
    const NYdb::TParams& params, const TString& expectedYson);

inline NYdb::NTable::TDataQueryResult ExecQueryAndTestResult(NYdb::NTable::TSession& session, const TString& query,
    const TString& expectedYson)
{
    return ExecQueryAndTestResult(session, query, NYdb::TParamsBuilder().Build(), expectedYson);
}

class TStreamReadError : public yexception {
public:
    TStreamReadError(NYdb::EStatus status)
        : Status(status)
    {}
    NYdb::EStatus Status;
};

TString StreamResultToYson(NYdb::NQuery::TExecuteQueryIterator& it, bool throwOnTImeout = false, const NYdb::EStatus& opStatus = NYdb::EStatus::SUCCESS, const TString& issueMessageSubString = "");
TString StreamResultToYson(NYdb::NTable::TScanQueryPartIterator& it, bool throwOnTImeout = false, const NYdb::EStatus& opStatus = NYdb::EStatus::SUCCESS, const TString& issueMessageSubString = "");
TString StreamResultToYson(NYdb::NScripting::TYqlResultPartIterator& it, bool throwOnTImeout = false, const NYdb::EStatus& opStatus = NYdb::EStatus::SUCCESS);
TString StreamResultToYson(NYdb::NTable::TTablePartIterator& it, bool throwOnTImeout = false, const NYdb::EStatus& opStatus = NYdb::EStatus::SUCCESS);

bool ValidatePlanNodeIds(const NJson::TJsonValue& plan);
ui32 CountPlanNodesByKv(const NJson::TJsonValue& plan, const TString& key, const TString& value);
NJson::TJsonValue FindPlanNodeByKv(const NJson::TJsonValue& plan, const TString& key, const TString& value);
std::vector<NJson::TJsonValue> FindPlanNodes(const NJson::TJsonValue& plan, const TString& key);
std::vector<NJson::TJsonValue> FindPlanStages(const NJson::TJsonValue& plan);

TString ReadTableToYson(NYdb::NTable::TSession session, const TString& table);
TString ReadTablePartToYson(NYdb::NTable::TSession session, const TString& table);

inline void AssertSuccessResult(const NYdb::TStatus& result) {
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void CreateSampleTablesWithIndex(NYdb::NTable::TSession& session, bool populateTables = true);

void InitRoot(Tests::TServer::TPtr server, TActorId sender);

THolder<NKikimr::NSchemeCache::TSchemeCacheNavigate> Navigate(TTestActorRuntime& runtime, const TActorId& sender,
                                                     const TString& path, NKikimr::NSchemeCache::TSchemeCacheNavigate::EOp op);

NKikimrScheme::TEvDescribeSchemeResult DescribeTable(Tests::TServer* server, TActorId sender, const TString &path);

TVector<ui64> GetTableShards(Tests::TServer* server, TActorId sender, const TString &path);

TVector<ui64> GetTableShards(Tests::TServer::TPtr server, TActorId sender, const TString &path);
TVector<ui64> GetColumnTableShards(Tests::TServer* server, TActorId sender, const TString &path);

void WaitForZeroSessions(const NKqp::TKqpCounters& counters);

bool JoinOrderAndAlgosMatch(const TString& optimized, const TString& reference);

/* Gets join order with details as: join algo, join type and scan type. */
NJson::TJsonValue GetDetailedJoinOrder(const TString& deserializedPlan);

/* Gets tables join order without details : only tables. */
NJson::TJsonValue GetJoinOrder(const TString& deserializedPlan);

} // namespace NKqp
} // namespace NKikimr
