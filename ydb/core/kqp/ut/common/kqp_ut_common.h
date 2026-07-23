#pragma once

#include <ydb/core/testlib/test_client.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>
#include <yql/essentials/core/issue/yql_issue.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/library/testlib/helpers.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/writer.h>
#include <library/cpp/threading/future/async.h>


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

struct TTestLogSettings {
    NLog::EPriority DefaultLogPriority = NLog::PRI_WARN;
    std::unordered_map<NKikimrServices::EServiceKikimr, NLog::EPriority> LogPriorities;
    bool Freeze = false;

    TTestLogSettings& AddLogPriority(NKikimrServices::EServiceKikimr service, NLog::EPriority priority);
};

struct TKikimrSettings: public TTestFeatureFlagsHolder<TKikimrSettings> {
private:
    void InitDefaultConfig() {
        auto* tableServiceConfig = AppConfig.MutableTableServiceConfig();
        auto* infoExchangerRetrySettings = tableServiceConfig->MutableResourceManager()->MutableInfoExchangerSettings();
        auto* exchangerSettings = infoExchangerRetrySettings->MutableExchangerSettings();
        exchangerSettings->SetStartDelayMs(10);
        exchangerSettings->SetMaxDelayMs(10);
        FeatureFlags.SetEnableSparsedColumns(true);
        FeatureFlags.SetEnableWritePortionsOnInsert(true);
        FeatureFlags.SetEnableParameterizedDecimal(true);
        FeatureFlags.SetEnableTopicAutopartitioningForCDC(true);
        FeatureFlags.SetEnableFollowerStats(true);
        FeatureFlags.SetEnableColumnStore(true);

        if (!AppConfig.MutableColumnShardConfig()->HasReaderClassName()) {
            SetColumnShardReaderClassName("SIMPLE");
        }
        if (!AppConfig.MutableColumnShardConfig()->HasDisabledOnSchemeShard()) {
            AppConfig.MutableColumnShardConfig()->SetDisabledOnSchemeShard(false);
        }
        if (!AppConfig.MutableColumnShardConfig()->HasMaxInFlightIntervalsOnRequest()) {
            AppConfig.MutableColumnShardConfig()->SetMaxInFlightIntervalsOnRequest(1);
        }
    }
public:
    NKikimrConfig::TAppConfig AppConfig;
    NKikimrPQ::TPQConfig PQConfig;
    TVector<NKikimrKqp::TKqpSetting> KqpSettings;
    TString AuthToken;
    TString DomainRoot = KikimrDefaultUtDomainRoot;
    ui32 NodeCount = 1;
    bool WithSampleTables = true;
    bool UseRealThreads = true;
    bool EnableForceFollowers = false;
    bool EnableScriptExecutionBackgroundChecks = true;
    TDuration KeepSnapshotTimeout = TDuration::Zero();
    IOutputStream* LogStream = nullptr;
    TMaybe<NFake::TStorage> Storage = Nothing();
    bool InitFederatedQuerySetupFactory = false;
    NKqp::IKqpFederatedQuerySetupFactory::TPtr FederatedQuerySetupFactory = std::make_shared<NKqp::TKqpFederatedQuerySetupFactoryNoop>();
    NKqp::IDescribeSchemaSecretsServiceFactory::TPtr DescribeSchemaSecretsServiceFactory = std::make_shared<NKqp::TDescribeSchemaSecretsServiceFactory>();
    NMonitoring::TDynamicCounterPtr CountersRoot = MakeIntrusive<NMonitoring::TDynamicCounters>();
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> S3ActorsFactory = NYql::NDq::CreateDefaultS3ActorsFactory();
    NKikimrConfig::TImmediateControlsConfig Controls;
    TMaybe<NYdbGrpc::TServerOptions> GrpcServerOptions;
    bool EnableStorageProxy = false;
    bool UseLocalCheckpointsInStreamingQueries = false;
    TDuration CheckpointPeriod = TDuration::MilliSeconds(200);
    std::optional<TTestLogSettings> LogSettings;

    TKikimrSettings() {
        InitDefaultConfig();
    }

    explicit TKikimrSettings(const NKikimrConfig::TAppConfig& value)
        : AppConfig(value)
    {
        InitDefaultConfig();
    }

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
    TKikimrSettings& SetInitFederatedQuerySetupFactory(bool value) { InitFederatedQuerySetupFactory = value; return *this; };
    TKikimrSettings& SetFederatedQuerySetupFactory(NKqp::IKqpFederatedQuerySetupFactory::TPtr value) { FederatedQuerySetupFactory = value; return *this; };
    TKikimrSettings& SetDescribeSchemaSecretsServiceFactory(NKqp::IDescribeSchemaSecretsServiceFactory::TPtr value) { DescribeSchemaSecretsServiceFactory = value; return *this; };
    TKikimrSettings& SetUseRealThreads(bool value) { UseRealThreads = value; return *this; };
    TKikimrSettings& SetEnableForceFollowers(bool value) { EnableForceFollowers = value; return *this; };
    TKikimrSettings& SetS3ActorsFactory(std::shared_ptr<NYql::NDq::IS3ActorsFactory> value) { S3ActorsFactory = std::move(value); return *this; };
    TKikimrSettings& SetControls(const NKikimrConfig::TImmediateControlsConfig& value) { Controls = value; return *this; }
    TKikimrSettings& SetColumnShardReaderClassName(const TString& value) { AppConfig.MutableColumnShardConfig()->SetReaderClassName(value); return *this; }
    TKikimrSettings& SetColumnShardAlterObjectEnabled(bool enable) {
            AppConfig.MutableColumnShardConfig()->SetAlterObjectEnabled(enable);
            return *this;
    }
    TKikimrSettings& SetColumnShardDoubleOutOfRangeHandling(const NKikimrConfig::TColumnShardConfig_EJsonDoubleOutOfRangeHandlingPolicy value) {
        AppConfig.MutableColumnShardConfig()->SetDoubleOutOfRangeHandling(value);
        return *this;
    }
    TKikimrSettings& SetGrpcServerOptions(const NYdbGrpc::TServerOptions& grpcServerOptions) { GrpcServerOptions = grpcServerOptions; return *this; };
    TKikimrSettings& SetEnableStorageProxy(bool value) { EnableStorageProxy = value; return *this; };
    TKikimrSettings& SetUseLocalCheckpointsInStreamingQueries(bool value) { UseLocalCheckpointsInStreamingQueries = value; return *this; };
    TKikimrSettings& SetCheckpointPeriod(TDuration value) { CheckpointPeriod = value; return *this; };
    TKikimrSettings& SetLogSettings(TTestLogSettings value) { LogSettings = value; return *this; };
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
        Server->GetRuntime()->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        // Stop the driver to close all client-side gRPC connections
        RunCall([&] { Driver->Stop(true); Driver.Reset(); return false; });

        // In single-threaded mode (UseRealThreads=false), actor system events
        // need explicit dispatching. After Driver->Stop(), there may be pending
        // server-side session cleanup events in the actor system mailbox.
        // Dispatch them so gRPC server doesn't see stale in-progress requests
        // during shutdown (which would cause it to wait and leak memory).
        // In real-threads mode, the actor system threads handle this automatically.
        if (!Server->GetRuntime()->IsRealThreads()) {
            auto savedTimeout = Server->GetRuntime()->SetDispatchTimeout(TDuration::MilliSeconds(500));
            try {
                TDispatchOptions opts;
                Server->GetRuntime()->DispatchEvents(opts, TDuration::MilliSeconds(500));
            } catch (const TEmptyEventQueueException&) {
                // Timeout is expected when there are no more events to dispatch
            }
            Server->GetRuntime()->SetDispatchTimeout(savedTimeout);
        }

        if (ThreadPoolStarted_) {
            ThreadPool.Stop();
        }

        // Shutdown gRPC servers to stop accepting new connections
        // This prevents memory leaks from connections being established during shutdown
        Server->ShutdownGRpc();

        if (!WaitHttpGatewayFinalization(CountersRoot)) {
            Cerr << "Failed to finalize http gateway before destruction" << Endl;
        }

        Server.Reset();
        Client.Reset();
    }

    NYdb::TDriver* GetDriverMut() { return Driver.Get(); }
    const TString& GetEndpoint() const { return Endpoint; }
    const NYdb::TDriver& GetDriver() const { return *Driver; }
    Tests::TClient& GetTestClient() const { return *Client; }
    Tests::TServer& GetTestServer() const { return *Server; }

    NYdb::TDriverConfig GetDriverConfig() const { return DriverConfig; }

    NYdb::NScheme::TSchemeClient GetSchemeClient(NYdb::TCommonClientSettings settings = NYdb::TCommonClientSettings()) const {
        return NYdb::NScheme::TSchemeClient(*Driver, settings);
    }

    NYdb::NTable::TTableClient GetTableClient(
        NYdb::NTable::TClientSettings settings = NYdb::NTable::TClientSettings()) const {
        return NYdb::NTable::TTableClient(*Driver, settings.UseQueryCache(false));
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
    bool SetupLogLevelFromTestParam(NKikimrServices::EServiceKikimr service);

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
    auto settings = TKikimrSettings(appConfig)
        .SetKqpSettings(kqpSettings)
        .SetEnableScriptExecutionOperations(true);

    return TKikimrRunner{settings};
}

struct TCollectedStreamResult {
    TString ResultSetYson;
    TMaybe<TString> PlanJson;
    TMaybe<TString> Ast;
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

TString ReformatYson(const TString& yson);
void CompareYson(const TString& expected, const TString& actual, const TString& message = {});
void CompareYson(const TString& expected, const NKikimrMiniKQL::TResult& actual, const TString& message = {});

void CreateLargeTable(TKikimrRunner& kikimr, ui32 rowsPerShard, ui32 keyTextSize,
    ui32 dataTextSize, ui32 batchSizeRows = 100, ui32 fillShardsCount = 8, ui32 largeTableKeysPerShard = 1000000);

void CreateManyShardsTable(TKikimrRunner& kikimr, ui32 totalRows = 1000, ui32 shards = 100, ui32 batchSizeRows = 1000);

bool HasIssue(const NYql::TIssues& issues, ui32 code,
    std::function<bool(const NYql::TIssue& issue)> predicate = {});

bool HasIssue(const NYdb::NIssue::TIssues& issues, ui32 code,
    std::function<bool(const NYdb::NIssue::TIssue& issue)> predicate = {});

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

NYdb::NQuery::TExecuteQueryResult ExecQueryAndTestEmpty(NYdb::NQuery::TSession& session, const TString& query, NYdb::NQuery::TTxControl txControl = NYdb::NQuery::TTxControl::NoTx());

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

void CreateSampleTablesWithIndex(NYdb::NTable::TSession& session, bool populateTables = true, bool withPgTypes = false);

void InitRoot(Tests::TServer::TPtr server, TActorId sender);

void Grant(NYdb::NTable::TSession& adminSession, const char* permissions, const char* path, const char* user);

void Revoke(NYdb::NTable::TSession& adminSession, const char* permissions, const char* path, const char* user);

THolder<NKikimr::NSchemeCache::TSchemeCacheNavigate> Navigate(TTestActorRuntime& runtime, const TActorId& sender,
                                                     const TString& path, NKikimr::NSchemeCache::TSchemeCacheNavigate::EOp op);

NKikimrScheme::TEvDescribeSchemeResult DescribeTable(Tests::TServer* server, TActorId sender, const TString &path);

TVector<ui64> GetTableShards(Tests::TServer* server, TActorId sender, const TString &path);

TVector<ui64> GetTableShards(Tests::TServer::TPtr server, TActorId sender, const TString &path);
TVector<ui64> GetColumnTableShards(Tests::TServer* server, TActorId sender, const TString &path);

void WaitForZeroSessions(const NKqp::TKqpCounters& counters);
void WaitForZeroReadIterators(Tests::TServer& server, const TString& path);
int GetCumulativeCounterValue(Tests::TServer& server, const TString& path, const TString& counterName);

void CheckTableReads(NYdb::NTable::TSession& session, const TString& tableName, bool checkFollower, bool readsExpected);

void WaitForCompaction(Tests::TServer* server, const TString& path, bool compactBorrowed = false);

bool JoinOrderAndAlgosMatch(const TString& optimized, const TString& reference);

struct TGetPlanParams {
    bool IncludeFilters = false;
    bool IncludeOptimizerEstimation = false;
    bool IncludeTables = true;
    bool IncludeShuffles = false;
};

/* Gets join order with details as: join algo, join type and scan type. */
NJson::TJsonValue GetDetailedJoinOrder(const TString& deserializedPlan, const TGetPlanParams& params = {});

/* Gets tables join order without details : only tables. */
NJson::TJsonValue GetJoinOrder(const TString& deserializedPlan);

NJson::TJsonValue GetJoinOrderFromDetailedJoinOrder(const TString& deserializedDetailedJoinOrder);

class TTestExtEnv {
public:
    struct TEnvSettings {
        size_t StaticNodeCount = 1;
        size_t DynamicNodeCount = 1;
        TString PoolName = "hdd1";
        bool UseRealThreads = true;
        NKikimrConfig::TFeatureFlags FeatureFlags;
    };

    TTestExtEnv(TEnvSettings envSettings);
    ~TTestExtEnv();

    NYdb::TDriver& GetDriver() const {
        return *Driver;
    }

    void CreateDatabase(const TString& databaseName);
    Tests::TServer& GetServer() const;
    Tests::TClient& GetClient() const;

private:
    TPortManager PortManager;

    Tests::TServerSettings::TPtr Settings;
    Tests::TServer::TPtr Server;
    THolder<Tests::TClient> Client;
    THolder<Tests::TTenants> Tenants;

    TString Endpoint;
    NYdb::TDriverConfig DriverConfig;
    THolder<NYdb::TDriver> Driver;

    TEnvSettings EnvSettings;
};

void CheckOwner(NYdb::NTable::TSession& session, const TString& path, const TString& name);

} // namespace NKqp
} // namespace NKikimr
