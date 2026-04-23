#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/testlib/test_pq_client.h>

#include <ydb/library/testlib/common/test_utils.h>
#include <ydb/library/testlib/pq_helpers/mock_pq_gateway.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/error.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/connector_client_mock.h>

#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/generic/string.h>
#include <util/system/env.h>
#include <util/system/types.h>

#include <functional>
#include <string>
#include <vector>

namespace NKikimr::NKqp {

struct TScriptQuerySettings {
    bool SaveState = false;
    NKikimrKqp::TScriptExecutionRetryState::TMapping RetryMapping;
    std::optional<NKikimrKqp::TQueryPhysicalGraph> PhysicalGraph;
    TDuration Timeout = TDuration::Seconds(30);
    std::string CheckpointId = CreateGuidAsString();
};

struct TColumn {
    std::string Name;
    Ydb::Type::PrimitiveTypeId Type;
};

struct TMockConnectorTableDescriptionSettings {
    std::string TableName;
    std::vector<TColumn> Columns;
    ui64 DescribeCount = 1;
    ui64 ListSplitsCount = 1;
    bool ValidateListSplitsArgs = true;
};

struct TMockConnectorReadSplitsSettings {
    std::string TableName;
    std::vector<TColumn> Columns;
    ui64 NumberReadSplits;
    bool ValidateReadSplitsArgs = true;
    std::function<std::shared_ptr<arrow::RecordBatch>()> ResultFactory;
};

class TStreamingTestFixture : public NUnitTest::TBaseFixture {
    using TBase = NUnitTest::TBaseFixture;

public:
    // External YDB recipe
    inline static const std::string YDB_ENDPOINT = GetEnv("YDB_ENDPOINT");
    inline static const std::string YDB_DATABASE = GetEnv("YDB_DATABASE");

    // Local kikimr test cluster
    inline static constexpr char TEST_DATABASE[] = "/Root";

    inline static constexpr TDuration TEST_OPERATION_TIMEOUT = TDuration::Seconds(10);

public:
    // Local kikimr settings

    NKikimrConfig::TAppConfig& SetupAppConfig();
    void UpdateConfig(NKikimrConfig::TAppConfig& appConfig);

    TIntrusivePtr<NTestUtils::IMockPqGateway> SetupMockPqGateway();

    std::shared_ptr<NYql::NConnector::NTest::TConnectorClientMock> SetupMockConnectorClient();

    // Local kikimr test cluster

    std::shared_ptr<TKikimrRunner> GetKikimrRunner();

    TTestActorRuntime& GetRuntime();

    std::shared_ptr<NYdb::TDriver> GetInternalDriver();

    std::shared_ptr<NYdb::NQuery::TQueryClient> GetQueryClient();

    std::shared_ptr<NYdb::NTable::TTableClient> GetTableClient();

    std::shared_ptr<NYdb::NOperation::TOperationClient> GetOperationClient();

    std::shared_ptr<NYdb::NTable::TSession> GetTableClientSession();

    std::shared_ptr<NKikimr::NPersQueueTests::TFlatMsgBusPQClient> GetLocalFlatMsgBusPQClient();

    void KillTopicPqrbTablet(const std::string& topicPath);

    // External YDB recipe

    std::shared_ptr<NYdb::TDriver> GetExternalDriver();

    std::shared_ptr<NYdb::NTopic::TTopicClient> GetTopicClient(bool local = false);

    std::shared_ptr<NYdb::NQuery::TQueryClient> GetExternalQueryClient();

    std::vector<NYdb::TResultSet> ExecExternalQuery(const std::string& query, NYdb::EStatus expectedStatus = NYdb::EStatus::SUCCESS);

    // Topic client SDK (external YDB recipe)

    void CreateTopic(const std::string& topicName, std::optional<NYdb::NTopic::TCreateTopicSettings> settings = std::nullopt, bool local = false);

    void WriteTopicMessage(const std::string& topicName, const std::string& message, ui64 partition = 0, bool local = false);

    void WriteTopicMessages(const std::string& topicName, const std::vector<std::string>& messages, ui64 partition = 0);

    void ReadTopicMessage(const std::string& topicName, const std::string& expectedMessage, TInstant disposition = TInstant::Now() - TDuration::Seconds(100), bool local = false);

    void ReadTopicMessages(const std::string& topicName, std::vector<std::string> expectedMessages, TInstant disposition = TInstant::Now() - TDuration::Seconds(100), bool sort = false, bool local = false);

    void TestReadTopicBasic(const std::string& testSuffix);

    // Table client SDK

    void ExecSchemeQuery(const std::string& query, NYdb::EStatus expectedStatus = NYdb::EStatus::SUCCESS);

    // Query client SDK

    std::vector<NYdb::TResultSet> ExecQuery(const std::string& query, NYdb::EStatus expectedStatus = NYdb::EStatus::SUCCESS, const std::string& expectedError = "", std::function<void(const std::string&)> astValidator = nullptr);

    void CreatePqSource(const std::string& pqSourceName);

    void CreatePqSourceBasicAuth(const std::string& pqSourceName, const bool useSchemaSecrets = false);

    void CreateS3Source(const std::string& bucket, const std::string& s3SourceName);

    void CreateYdbSource(const std::string& ydbSourceName);

    void CreateSolomonSource(const std::string& solomonSourceName);

    // Script executions (using query client SDK)

    NYdb::TOperation::TOperationId ExecScript(const std::string& query, std::optional<NYdb::NQuery::TExecuteScriptSettings> settings = std::nullopt, bool waitRunning = true);

    NYdb::NQuery::TScriptExecutionOperation GetScriptExecutionOperation(const NYdb::TOperation::TOperationId& operationId, bool checkStatus = true);

    NYdb::NQuery::TScriptExecutionOperation WaitScriptExecution(const NYdb::TOperation::TOperationId& operationId, NYdb::NQuery::EExecStatus finalStatus = NYdb::NQuery::EExecStatus::Completed, bool waitRetry = false);

    NYdb::NQuery::TScriptExecutionOperation ExecAndWaitScript(const std::string& query, NYdb::NQuery::EExecStatus finalStatus = NYdb::NQuery::EExecStatus::Completed, std::optional<NYdb::NQuery::TExecuteScriptSettings> settings = std::nullopt);

    NYdb::TResultSet FetchScriptResult(const NYdb::TOperation::TOperationId& operationId, ui64 resultSetId = 0);

    void CheckScriptResult(const NYdb::TResultSet& result, ui64 columnsCount, ui64 rowsCount, std::function<void(NYdb::TResultSetParser&)> validator);

    void CheckScriptResult(const NYdb::TOperation::TOperationId& operationId, ui64 columnsCount, ui64 rowsCount, std::function<void(NYdb::TResultSetParser&)> validator, ui64 resultSetId = 0);

    void CancelScriptExecution(const NYdb::TOperation::TOperationId& operationId);

    // Script executions (native events API)

    std::pair<std::string, NYdb::TOperation::TOperationId> ExecScriptNative(const std::string& query, const TScriptQuerySettings& settings = {}, bool waitRunning = true);

    NKikimrKqp::TScriptExecutionRetryState::TMapping CreateRetryMapping(const std::vector<Ydb::StatusIds::StatusCode>& statuses, TDuration backoffDuration = TDuration::Seconds(5));

    NKikimrKqp::TQueryPhysicalGraph LoadPhysicalGraph(const std::string& executionId);

    void CheckScriptExecutionsCount(ui64 expectedExecutionsCount, ui64 expectedLeasesCount);

    void WaitCheckpointUpdate(const std::string& checkpointId);

    // Mock Connector utils

    static NYql::TGenericDataSourceInstance GetMockConnectorSourceInstance();

    template <typename TRequestBuilder>
    static void FillMockConnectorRequestColumns(TRequestBuilder& builder, const std::vector<TColumn>& columns) {
        for (const auto& column : columns) {
            builder.Column(TString{column.Name}, column.Type);
        }
    }

    // Should be called at most once
    static void SetupMockConnectorTableDescription(std::shared_ptr<NYql::NConnector::NTest::TConnectorClientMock> mockClient, const TMockConnectorTableDescriptionSettings& settings);

    // Should be called at most once
    static void SetupMockConnectorTableData(std::shared_ptr<NYql::NConnector::NTest::TConnectorClientMock> mockClient, const TMockConnectorReadSplitsSettings& settings);

    // Other helpers

    static std::function<void(const std::string&)> AstChecker(ui64 txCount, ui64 stagesCount);

private:
    void EnsureNotInitialized(const std::string& info);

    void TearDown(NUnitTest::TTestContext& context) final;

    virtual bool UseSchemaSecrets();

protected:
    ui32 NodeCount = 1;
    TDuration CheckpointPeriod = TDuration::MilliSeconds(200);
    TTestLogSettings LogSettings;
    bool InternalInitFederatedQuerySetupFactory = false;
    NYdb::NQuery::TClientSettings QueryClientSettings = NYdb::NQuery::TClientSettings().AuthToken(BUILTIN_ACL_ROOT);
    NYdb::NTopic::TTopicClientSettings TopicClientSettings = NYdb::NTopic::TTopicClientSettings().AuthToken(BUILTIN_ACL_ROOT);

private:
    std::optional<NKikimrConfig::TAppConfig> AppConfig;
    TIntrusivePtr<NYql::IPqGateway> PqGateway;
    NYql::NConnector::IClient::TPtr ConnectorClient;
    std::shared_ptr<TKikimrRunner> Kikimr;

    std::shared_ptr<NYdb::TDriver> InternalDriver;
    std::shared_ptr<NYdb::NOperation::TOperationClient> OperationClient;
    std::shared_ptr<NYdb::NQuery::TQueryClient> QueryClient;
    std::shared_ptr<NYdb::NTable::TTableClient> TableClient;
    std::shared_ptr<NYdb::NTable::TSession> TableClientSession;
    std::shared_ptr<NYdb::NTopic::TTopicClient> LocalTopicClient;
    std::shared_ptr<NKikimr::NPersQueueTests::TFlatMsgBusPQClient> LocalFlatMsgBusPQClient;

    // Attached to database from recipe (YDB_ENDPOINT / YDB_DATABASE)
    std::shared_ptr<NYdb::TDriver> ExternalDriver;
    std::shared_ptr<NYdb::NTopic::TTopicClient> TopicClient;
    std::shared_ptr<NYdb::NQuery::TQueryClient> ExternalQueryClient;
};

class TStreamingWithSchemaSecretsTestFixture : public TStreamingTestFixture {
public:
    bool UseSchemaSecrets() override;
};

class TStreamingSysViewTestFixture : public TStreamingTestFixture {
public:
    inline static constexpr ui64 SYS_VIEW_COLUMNS_COUNT = 13;
    inline static constexpr char INPUT_TOPIC_NAME[] = "sysViewInput";
    inline static constexpr char OUTPUT_TOPIC_NAME[] = "sysViewOutput";
    inline static constexpr char PQ_SOURCE[] = "sysViewSourceName";
    inline static constexpr TDuration STATS_WAIT_DURATION = TDuration::Seconds(2);

public:
    void Setup();

    void StartQuery(const std::string& name);

    struct TSysViewRow {
        std::string Name;
        std::string Status = "RUNNING";
        std::string Issues = "{}";
        std::optional<std::string> Ast;
        std::optional<std::string> Text;
        bool Run = true;
        std::string Pool = "default";
        ui64 RetryCount = 0;
        std::optional<TInstant> LastFailAt;
        std::optional<TInstant> SuspendedUntil;
        bool CheckPlan = false;
    };

    struct TSysViewResult {
        std::string ExecutionId;
        std::vector<std::string> PreviousExecutionIds;
    };

    std::vector<TSysViewResult> CheckSysView(std::vector<TSysViewRow> rows, const std::string& filter = "", const std::string& order = "Path ASC");

protected:
    std::string GetQueryText(const std::string& name) const;

protected:
    std::string InputTopic;
    std::string OutputTopic;
};

class TTestTopicLoader : public TActorBootstrapped<TTestTopicLoader> {
public:
    TTestTopicLoader(const std::string& endpoint, const std::string& database, const std::string& topic, NThreading::TFuture<void> feature);

    STRICT_STFUNC(StateFunc,
        sFunc(TEvents::TEvWakeup, WriteMessages)
    )

    void Bootstrap();

    void WriteMessages();

private:
    NYdb::NTopic::TTopicClient Client;
    const std::shared_ptr<NYdb::NTopic::IWriteSession> WriteSession;
    const NThreading::TFuture<void> Feature;
    const std::string Message;
    const TInstant Timeout = TInstant::Now() + TDuration::Seconds(60);
};

class TTabletKiller : public TActorBootstrapped<TTabletKiller> {
public:
    TTabletKiller(ui64 tabletId, TDuration killerInterval);

    STRICT_STFUNC(StateFunc,
        sFunc(TEvents::TEvWakeup, KillTablet)
    )

    void Bootstrap();

private:
    void KillTablet() const;

    const ui64 TabletId;
    const TDuration KillerInterval;
};

} // namespace NKikimr::NKqp
