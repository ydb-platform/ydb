#include "kqp_script_executions.h"
#include "kqp_script_execution_retries.h"
#include "kqp_script_executions_impl.h"

#include <ydb/core/base/backtrace.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/finalize_script_service/kqp_finalize_script_service.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/util/proto_duration.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>
#include <ydb/library/table_creator/table_creator.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/services/ydb/ydb_common_ut.h>

namespace NKikimr::NKqp {

using namespace Tests;
using namespace NSchemeShard;

namespace  {

constexpr TDuration TestLeaseDuration = TDuration::Seconds(1);
constexpr TDuration TestTimeout = TDuration::Seconds(10);
constexpr TDuration TestOperationTtl = TDuration::Minutes(1);
constexpr TDuration TestResultsTtl = TDuration::Minutes(1);
const TString TestDatabase = CanonizePath(TestDomainName);

NKikimrSchemeOp::TColumnDescription Col(const TString& columnName, const char* columnType) {
    NKikimrSchemeOp::TColumnDescription desc;
    desc.SetName(columnName);
    desc.SetType(columnType);
    return desc;
}

NKikimrSchemeOp::TColumnDescription Col(const TString& columnName, NScheme::TTypeId columnType) {
    return Col(columnName, NScheme::TypeName(columnType));
}

[[maybe_unused]] NKikimrSchemeOp::TTTLSettings TtlCol(const TString& columnName) {
    NKikimrSchemeOp::TTTLSettings settings;
    auto* deleteTier = settings.MutableEnabled()->AddTiers();
    deleteTier->MutableDelete();
    deleteTier->SetApplyAfterSeconds(TDuration::Minutes(20).Seconds());
    settings.MutableEnabled()->SetExpireAfterSeconds(TDuration::Minutes(20).Seconds());
    settings.MutableEnabled()->SetColumnName(columnName);
    settings.MutableEnabled()->MutableSysSettings()->SetRunInterval(TDuration::Minutes(60).MicroSeconds());
    return settings;
}

const TVector<NKikimrSchemeOp::TColumnDescription> DEFAULT_COLUMNS = {
    Col("col1", NScheme::NTypeIds::Int32),
    Col("col2", NScheme::NTypeIds::Int32),
    Col("col3", NScheme::NTypeIds::String)
};

const TVector<NKikimrSchemeOp::TColumnDescription> EXTENDED_COLUMNS = {
    Col("col1", NScheme::NTypeIds::Int32),
    Col("col2", NScheme::NTypeIds::Int32),
    Col("col3", NScheme::NTypeIds::String),
    Col("col4", NScheme::NTypeIds::JsonDocument),
    Col("col5", NScheme::NTypeIds::Interval)
};

const TVector<TString> TEST_TABLE_PATH = { "test", "test_table" };

const TVector<TString> TEST_KEY_COLUMNS = {"col1"};

struct TScriptExecutionsYdbSetup {
    explicit TScriptExecutionsYdbSetup(bool enableScriptExecutionBackgroundChecks = false) {
        Init(enableScriptExecutionBackgroundChecks);
    }

    static void BackTraceSignalHandler(int signal) {
        NColorizer::TColors colors = NColorizer::AutoColors(Cerr);

        Cerr << colors.Red() << "======= " << signal << " call stack ========" << colors.Default() << Endl;
        FormatBackTrace(&Cerr);
        Cerr << colors.Red() << "===============================================" << colors.Default() << Endl;

        abort();
    }

    void Init(bool enableScriptExecutionBackgroundChecks) {
        EnableYDBBacktraceFormat();
        for (auto sig : {SIGILL, SIGSEGV}) {
            signal(sig, &TScriptExecutionsYdbSetup::BackTraceSignalHandler);
        }

        MsgBusPort = PortManager.GetPort(2134);
        GrpcPort = PortManager.GetPort(2135);
        ServerSettings = MakeHolder<Tests::TServerSettings>(MsgBusPort);
        ServerSettings->SetEnableScriptExecutionOperations(true);
        ServerSettings->SetEnableScriptExecutionBackgroundChecks(enableScriptExecutionBackgroundChecks);
        ServerSettings->SetGrpcPort(GrpcPort);
        Server = MakeHolder<Tests::TServer>(*ServerSettings);
        Client = MakeHolder<Tests::TClient>(*ServerSettings);

        GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
        GetRuntime()->SetDispatchTimeout(TestTimeout);
        Server->EnableGRpc(GrpcPort);
        Client->InitRootScheme();

        WaitInitScriptExecutionsTables();

        // Init sdk
        NYdb::TDriverConfig driverCfg;
        driverCfg
            .SetEndpoint(TStringBuilder() << "localhost:" << GrpcPort)
            .SetDatabase(Tests::TestDomainName);
        YdbDriver = MakeHolder<NYdb::TDriver>(driverCfg);
        TableClient = MakeHolder<NYdb::NTable::TTableClient>(*YdbDriver);
        auto createSessionResult = TableClient->CreateSession().ExtractValueSync();
        UNIT_ASSERT_C(createSessionResult.IsSuccess(), createSessionResult.GetIssues().ToString());
        TableClientSession = MakeHolder<NYdb::NTable::TSession>(createSessionResult.GetSession());

        Cerr << "\n\n\n--------------------------- INIT FINISHED ---------------------------\n\n\n";
    }

    TTestActorRuntime* GetRuntime() {
        return Server->GetRuntime();
    }

    void WaitInitScriptExecutionsTables() {
        const auto timeout = TInstant::Now() + TestTimeout;
        while (!RunSelect42Script()) {
            if (TInstant::Now() > timeout) {
                UNIT_FAIL("Failed to init script executions tables");
            }
            Sleep(TDuration::MilliSeconds(10));
        }
    }

    bool RunSelect42Script() {
        const auto reply = RunQueryInDb("SELECT 42");
        if (reply->Get()->Status != Ydb::StatusIds::SUCCESS) {
            return false;
        }

        WaitQueryFinish(reply->Get()->ExecutionId);
        return true;
    }

    TEvKqp::TEvScriptResponse::TPtr RunQueryInDb(const TString& query = "SELECT 42", TDuration resultsTtl = TestResultsTtl, const std::vector<NKikimrKqp::TScriptExecutionRetryState::TMapping> retryMapping = {}) {
        auto ev = MakeHolder<TEvKqp::TEvScriptRequest>();
        ev->Record = GetQueryRequest(query);
        ev->ResultsTtl = resultsTtl;
        ev->RetryMapping = retryMapping;

        const auto edgeActor = GetRuntime()->AllocateEdgeActor();
        GetRuntime()->Send(new IEventHandle(MakeKqpProxyID(GetRuntime()->GetNodeId()), edgeActor, ev.Release()));

        const auto reply = GetRuntime()->GrabEdgeEvent<TEvKqp::TEvScriptResponse>(edgeActor, TestTimeout);
        UNIT_ASSERT_C(reply, "CreateScript response is empty");

        return reply;
    }

    TString CheckRunQueryInDb(const TString& query = "SELECT 42", TDuration resultsTtl = TestResultsTtl, const std::vector<NKikimrKqp::TScriptExecutionRetryState::TMapping> retryMapping = {}) {
        const auto result = RunQueryInDb(query, resultsTtl, retryMapping);
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, Ydb::StatusIds::SUCCESS, result->Get()->Issues.ToString());
        UNIT_ASSERT(result->Get()->ExecutionId);

        return result->Get()->ExecutionId;
    }

    // Creates query in db. Returns execution id
    TString CreateQueryInDb(const TString& query = "SELECT 42", TDuration leaseDuration = TestLeaseDuration, TDuration operationTtl = TestOperationTtl, TDuration resultsTtl = TestResultsTtl) {
        const TString executionId = CreateGuidAsString();

        NKikimrKqp::TScriptExecutionOperationMeta meta;
        SetDuration(leaseDuration, *meta.MutableLeaseDuration());
        SetDuration(operationTtl, *meta.MutableOperationTtl());
        SetDuration(resultsTtl, *meta.MutableResultsTtl());

        const auto edgeActor = GetRuntime()->AllocateEdgeActor();
        GetRuntime()->Register(NPrivate::CreateCreateScriptOperationQueryActor(executionId, NActors::TActorId(), GetQueryRequest(query), meta), 0, 0, TMailboxType::Simple, 0, edgeActor);

        const auto reply = GetRuntime()->GrabEdgeEvent<NPrivate::TEvPrivate::TEvCreateScriptOperationResponse>(edgeActor, TestTimeout);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(executionId, reply->Get()->ExecutionId);
        return reply->Get()->ExecutionId;
    }

    void CreateTableInDbSync(TVector<NKikimrSchemeOp::TColumnDescription> columns = DEFAULT_COLUMNS, i32 numberOfRequests = 1, TVector<TString> pathComponents = TEST_TABLE_PATH, TVector<TString> keyColumns = TEST_KEY_COLUMNS,
                             TMaybe<NKikimrSchemeOp::TTTLSettings> ttlSettings = Nothing()) {

        TVector<TActorId> edgeActors;
        for (i32 i = 0; i < numberOfRequests; ++i) {
            edgeActors.push_back(CreateTableInDbAsync(columns, pathComponents, keyColumns, ttlSettings));
        }
        WaitTableCreation(std::move(edgeActors));
    }

    TActorId CreateTableInDbAsync(TVector<NKikimrSchemeOp::TColumnDescription> columns = DEFAULT_COLUMNS, TVector<TString> pathComponents = TEST_TABLE_PATH, TVector<TString> keyColumns = TEST_KEY_COLUMNS,
                             TMaybe<NKikimrSchemeOp::TTTLSettings> ttlSettings = Nothing()) {
        const ui32 node = 0;
        TActorId edgeActor = GetRuntime()->AllocateEdgeActor(node);
        GetRuntime()->Register(CreateTableCreator(std::move(pathComponents), std::move(columns), std::move(keyColumns),
            NKikimrServices::KQP_PROXY, std::move(ttlSettings)), 0, 0, TMailboxType::Simple, 0, edgeActor);
        return edgeActor;
    }

    void WaitTableCreation(TVector<TActorId> edgeActors) {
        for (const auto& actor: edgeActors) {
            GetRuntime()->GrabEdgeEvent<TEvTableCreator::TEvCreateTableResponse>(actor, TestTimeout);
        }
    }

    void VerifyColumnsList( TVector<TString> pathComponents = TEST_TABLE_PATH, TVector<NKikimrSchemeOp::TColumnDescription> columns = DEFAULT_COLUMNS) {
        TStringBuilder path;
        path << "/dc-1/";
        for (size_t i = 0; i < pathComponents.size() - 1; ++i) {
            path << pathComponents[i] << "/";
        }
        path << pathComponents.back();

        auto result = TableClientSession->DescribeTable(path).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto&  existingColumns = result.GetTableDescription().GetColumns();
        UNIT_ASSERT_C(existingColumns.size() == columns.size(), "Expected size: " << columns.size() << ", actual size: " << existingColumns.size());

        THashSet<TString> existingNames;
        for (const auto& col: existingColumns) {
            existingNames.emplace(col.Name);
        }

        for (const auto& col: columns) {
            UNIT_ASSERT_C(existingNames.contains(col.GetName()), "Column \"" << col.GetName() << "\" is not present" );
        }
    }

    NPrivate::TEvPrivate::TEvLeaseCheckResult::TPtr CheckLeaseStatus(const TString& executionId) {
        const ui32 node = 0;
        TActorId edgeActor = GetRuntime()->AllocateEdgeActor(node);
        GetRuntime()->Register(NPrivate::CreateCheckLeaseStatusActor(edgeActor, TestDatabase, executionId));

        auto reply = GetRuntime()->GrabEdgeEvent<NPrivate::TEvPrivate::TEvLeaseCheckResult>(edgeActor, TestTimeout);
        UNIT_ASSERT(reply->Get()->Status == Ydb::StatusIds::SUCCESS);
        return reply;
    }

    void CheckLeaseExistence(const TString& executionId, bool expectedExistence, std::optional<i32> expectedOperationStatus, i64 leaseGeneration = 1, NPrivate::ELeaseState leaseStatus = NPrivate::ELeaseState::ScriptRunning) {
        const TString sql = R"(
            DECLARE $database As Utf8;
            DECLARE $execution_id As Utf8;

            SELECT
                COUNT(*) AS number_leases,
                SOME(lease_generation) AS lease_generation,
                SOME(lease_state) AS lease_state
            FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id;

            SELECT
                operation_status
            FROM `.metadata/script_executions`
            WHERE database = $database AND execution_id = $execution_id;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(TestDatabase)
                .Build()
            .AddParam("$execution_id")
                .Utf8(executionId)
                .Build();

        const auto result = TableClientSession->ExecuteDataQuery(sql, NYdb::NTable::TTxControl::BeginTx().CommitTx(), params.Build()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        {   // Lease info
            NYdb::TResultSetParser rs = result.GetResultSetParser(0);
            UNIT_ASSERT(rs.TryNextRow());

            const auto count = rs.ColumnParser("number_leases").GetUint64();
            UNIT_ASSERT_VALUES_EQUAL(count, expectedExistence ? 1 : 0);

            if (expectedExistence) {
                const auto leaseGenerationInDatabase = rs.ColumnParser("lease_generation").GetOptionalInt64();
                UNIT_ASSERT(leaseGenerationInDatabase);
                UNIT_ASSERT_VALUES_EQUAL(*leaseGenerationInDatabase, leaseGeneration);

                const auto leaseStatusInDatabase = rs.ColumnParser("lease_state").GetOptionalInt32();
                UNIT_ASSERT(leaseStatusInDatabase);
                UNIT_ASSERT_VALUES_EQUAL(*leaseStatusInDatabase, static_cast<i32>(leaseStatus));
            }
        }

        {   // Execution info
            NYdb::TResultSetParser rs = result.GetResultSetParser(1);
            UNIT_ASSERT(rs.TryNextRow());

            UNIT_ASSERT(rs.ColumnParser("operation_status").GetOptionalInt32() == expectedOperationStatus);
        }
    }

    TEvScriptLeaseUpdateResponse::TPtr UpdateLease(const TString& executionId, TDuration leaseDuration, i64 leaseGeneration = 1) {
        const auto edgeActor = GetRuntime()->AllocateEdgeActor();
        GetRuntime()->Register(CreateScriptLeaseUpdateActor(edgeActor, TestDatabase, executionId, leaseDuration, leaseGeneration, nullptr));

        auto reply = GetRuntime()->GrabEdgeEvent<TEvScriptLeaseUpdateResponse>(edgeActor, TestTimeout);
        UNIT_ASSERT_C(reply, "ScriptLeaseUpdate response is empty");

        return reply;
    }

    TEvGetScriptExecutionOperationResponse::TPtr GetScriptExecutionOperation(const TString& executionId) {
        const auto edgeActor = GetRuntime()->AllocateEdgeActor();
        GetRuntime()->Send(
            MakeKqpProxyID(GetRuntime()->GetFirstNodeId()),
            edgeActor,
            new TEvGetScriptExecutionOperation(
                TestDatabase,
                NOperationId::TOperationId(ScriptExecutionOperationFromExecutionId(executionId))
            )
        );

        auto reply = GetRuntime()->GrabEdgeEvent<TEvGetScriptExecutionOperationResponse>(edgeActor, TestTimeout);
        UNIT_ASSERT_C(reply, "GetScriptExecutionOperation response is empty");

        return reply;
    }

    TEvFetchScriptResultsResponse::TPtr FetchScriptResults(const TString& executionId, i32 resultSetId) {
        const auto edgeActor = GetRuntime()->AllocateEdgeActor();
        GetRuntime()->Register(NKikimr::NKqp::CreateGetScriptExecutionResultActor(edgeActor, TestDatabase, executionId, resultSetId, 0, 0, 0, TInstant::Max()));

        auto reply = GetRuntime()->GrabEdgeEvent<TEvFetchScriptResultsResponse>(edgeActor, TestTimeout);
        UNIT_ASSERT_C(reply, "FetchScriptResults response is empty");

        return reply;
    }

    void WaitQueryFinish(const TString& executionId, TDuration timeoutDuration = TestTimeout) {
        const auto timeout = TInstant::Now() + timeoutDuration;
        while (true) {
            const auto getOperation = GetScriptExecutionOperation(executionId);
            const auto& ev = *getOperation->Get();

            UNIT_ASSERT_VALUES_EQUAL_C(ev.Status, Ydb::StatusIds::SUCCESS, ev.Issues.ToString());
            UNIT_ASSERT_C(ev.Metadata, "Expected not empty metadata for success get operation");

            Ydb::Query::ExecuteScriptMetadata deserializedMeta;
            ev.Metadata->UnpackTo(&deserializedMeta);
            UNIT_ASSERT_VALUES_EQUAL(deserializedMeta.execution_id(), executionId);
            UNIT_ASSERT_C(deserializedMeta.exec_mode() == Ydb::Query::EXEC_MODE_EXECUTE, Ydb::Query::ExecMode_Name(deserializedMeta.exec_mode()));

            const auto execStatus = deserializedMeta.exec_status();
            if (ev.Ready) {
                UNIT_ASSERT_C(execStatus == Ydb::Query::EXEC_STATUS_COMPLETED, Ydb::Query::ExecStatus_Name(execStatus));
                break;
            }

            UNIT_ASSERT_C(IsIn({Ydb::Query::EXEC_STATUS_STARTING, Ydb::Query::EXEC_STATUS_RUNNING}, execStatus), Ydb::Query::ExecStatus_Name(execStatus));

            if (TInstant::Now() > timeout) {
                UNIT_FAIL("Failed to wait for query to finish: " << executionId);
            }

            Sleep(TDuration::MilliSeconds(100));
        }
    }

    void WaitOperationStatus(const TString& executionId, i32 expectedOperationStatus, TDuration timeoutDuration = TestTimeout) {
        Ydb::StatusIds::StatusCode lastOperationStatus = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        const auto timeout = TInstant::Now() + timeoutDuration;
        while (true) {
            const TString sql = R"(
                DECLARE $database As Utf8;
                DECLARE $execution_id As Utf8;

                SELECT
                    operation_status
                FROM `.metadata/script_executions`
                WHERE database = $database AND execution_id = $execution_id;
            )";

            NYdb::TParamsBuilder params;
            params
                .AddParam("$database")
                    .Utf8(TestDatabase)
                    .Build()
                .AddParam("$execution_id")
                    .Utf8(executionId)
                    .Build();

            const auto result = TableClientSession->ExecuteDataQuery(sql, NYdb::NTable::TTxControl::BeginTx().CommitTx(), params.Build()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            NYdb::TResultSetParser rs = result.GetResultSetParser(0);
            UNIT_ASSERT(rs.TryNextRow());

            if (const auto operationStatus = rs.ColumnParser("operation_status").GetOptionalInt32()) {
                if (operationStatus == expectedOperationStatus) {
                    return;
                }
                lastOperationStatus = static_cast<Ydb::StatusIds::StatusCode>(*operationStatus);
            }

            if (TInstant::Now() > timeout) {
                UNIT_FAIL("Failed to wait for operation status, last status: " << lastOperationStatus << ", execution id: " << executionId);
            }

            Sleep(TDuration::MilliSeconds(100));
        }
    }

private:
    NKikimrKqp::TEvQueryRequest GetQueryRequest(const TString& query) {
        NKikimrKqp::TEvQueryRequest queryProto;
        queryProto.SetUserToken(NACLib::TUserToken(BUILTIN_ACL_ROOT, TVector<NACLib::TSID>{GetRuntime()->GetAppData().AllAuthenticatedUsers}).SerializeAsString());

        auto& req = *queryProto.MutableRequest();
        req.SetDatabase(TestDatabase);
        req.SetQuery(query);
        req.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        req.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT);

        return queryProto;
    }

public:
    TPortManager PortManager;
    ui16 MsgBusPort = 0;
    ui16 GrpcPort = 0;
    THolder<Tests::TServerSettings> ServerSettings;
    THolder<Tests::TServer> Server;
    THolder<Tests::TClient> Client;
    THolder<NYdb::TDriver> YdbDriver;
    THolder<NYdb::NTable::TTableClient> TableClient;
    THolder<NYdb::NTable::TSession> TableClientSession;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(ScriptExecutionsTest) {
    Y_UNIT_TEST(RunCheckLeaseStatus) {
        TScriptExecutionsYdbSetup ydb;

        const TString executionId = ydb.CreateQueryInDb();
        UNIT_ASSERT(executionId);

        const TInstant startLeaseTime = TInstant::Now();
        ydb.CheckLeaseExistence(executionId, true, std::nullopt);

        if (const auto checkResult = ydb.CheckLeaseStatus(executionId); TInstant::Now() - startLeaseTime < TestLeaseDuration) {
            UNIT_ASSERT_VALUES_EQUAL(checkResult->Get()->OperationStatus, Nothing());
            ydb.CheckLeaseExistence(executionId, true, std::nullopt);
            SleepUntil(startLeaseTime + TestLeaseDuration);
        }

        const auto checkResult = ydb.CheckLeaseStatus(executionId);
        UNIT_ASSERT_VALUES_EQUAL(checkResult->Get()->OperationStatus, Ydb::StatusIds::UNAVAILABLE);
        ydb.CheckLeaseExistence(executionId, false, Ydb::StatusIds::UNAVAILABLE);
    }

    Y_UNIT_TEST(UpdatesLeaseAfterExpiring) {
        TScriptExecutionsYdbSetup ydb;

        const TString executionId = ydb.CreateQueryInDb();
        UNIT_ASSERT(executionId);

        TInstant startLeaseTime = TInstant::Now();

        ydb.CheckLeaseExistence(executionId, true, std::nullopt);
        SleepUntil(startLeaseTime + TestLeaseDuration);

        startLeaseTime = TInstant::Now();
        TDuration leaseDuration = TDuration::Seconds(10);
        const auto updateResponse = ydb.UpdateLease(executionId, leaseDuration);
        UNIT_ASSERT_VALUES_EQUAL_C(updateResponse->Get()->Status, Ydb::StatusIds::SUCCESS, updateResponse->Get()->Issues.ToString());
        UNIT_ASSERT(updateResponse->Get()->ExecutionEntryExists);

        ydb.CheckLeaseExistence(executionId, true, std::nullopt);
        auto checkResult = ydb.CheckLeaseStatus(executionId);

        if (TInstant::Now() - startLeaseTime < leaseDuration) {
            UNIT_ASSERT_VALUES_EQUAL(checkResult->Get()->OperationStatus, Nothing());
        }
    }

    Y_UNIT_TEST(AttemptToUpdateDeletedLease) {
        TScriptExecutionsYdbSetup ydb;

        const TString executionId = ydb.CreateQueryInDb();
        UNIT_ASSERT(executionId);
        ydb.CheckLeaseExistence(executionId, true, std::nullopt);

        Sleep(TestLeaseDuration);

        auto checkResult = ydb.CheckLeaseStatus(executionId);
        UNIT_ASSERT_VALUES_EQUAL(checkResult->Get()->OperationStatus, Ydb::StatusIds::UNAVAILABLE);
        ydb.CheckLeaseExistence(executionId, false, Ydb::StatusIds::UNAVAILABLE);

        const auto updateResponse = ydb.UpdateLease(executionId, TestLeaseDuration);
        UNIT_ASSERT(!updateResponse->Get()->ExecutionEntryExists);
    }

    TString ExecuteQueryToRetry(TScriptExecutionsYdbSetup& ydb, TDuration backoffDuration) {
        NKikimrKqp::TScriptExecutionRetryState::TMapping retryMapping;
        retryMapping.AddStatusCode(Ydb::StatusIds::SCHEME_ERROR);
        auto& policy = *retryMapping.MutableBackoffPolicy();
        policy.SetRetryPeriodMs(backoffDuration.MilliSeconds());
        policy.SetBackoffPeriodMs(backoffDuration.MilliSeconds());
        policy.SetRetryRateLimit(1);

        constexpr char TABLE_NAME[] = "test_table";
        const auto executionId = ydb.CheckRunQueryInDb(
            TStringBuilder() << "SELECT * FROM " << TABLE_NAME,
            TestResultsTtl,
            {retryMapping}
        );

        const auto timeout = TInstant::Now() + TestTimeout;
        while (true) {
            const auto getOperation = ydb.GetScriptExecutionOperation(executionId);
            const auto& ev = *getOperation->Get();

            UNIT_ASSERT_VALUES_EQUAL_C(ev.Status, Ydb::StatusIds::SUCCESS, ev.Issues.ToString());
            UNIT_ASSERT_C(!ev.Ready, "Operation unexpectedly finished");
            UNIT_ASSERT_C(ev.Metadata, "Expected not empty metadata for success get operation");

            Ydb::Query::ExecuteScriptMetadata deserializedMeta;
            ev.Metadata->UnpackTo(&deserializedMeta);
            UNIT_ASSERT_VALUES_EQUAL(deserializedMeta.execution_id(), executionId);
            UNIT_ASSERT_C(deserializedMeta.exec_mode() == Ydb::Query::EXEC_MODE_EXECUTE, Ydb::Query::ExecMode_Name(deserializedMeta.exec_mode()));

            const auto execStatus = deserializedMeta.exec_status();
            if (execStatus == Ydb::Query::EXEC_STATUS_FAILED) {
                UNIT_ASSERT_STRING_CONTAINS(ev.Issues.ToString(), "Script execution operation failed with code SCHEME_ERROR and will be restarted");
                ydb.CheckLeaseExistence(executionId, true, Ydb::StatusIds::SCHEME_ERROR, 1, NPrivate::ELeaseState::WaitRetry);
                break;
            }

            UNIT_ASSERT_C(IsIn({Ydb::Query::EXEC_STATUS_STARTING, Ydb::Query::EXEC_STATUS_RUNNING}, execStatus), Ydb::Query::ExecStatus_Name(execStatus));

            if (TInstant::Now() > timeout) {
                UNIT_FAIL("Failed to wait for query to finish (before retry)");
            }

            Sleep(TDuration::MilliSeconds(100));
        }

        ydb.WaitQueryFinish(ydb.CheckRunQueryInDb(TStringBuilder() << R"(
            CREATE TABLE )" << TABLE_NAME << R"( (
                PRIMARY KEY (Key)
            ) AS
                SELECT 42 AS Key, "Some-Val" AS Value
        )"));

        return executionId;
    }

    void CheckQueryResults(TScriptExecutionsYdbSetup& ydb, const TString& executionId) {
        const auto scriptResults = ydb.FetchScriptResults(executionId, 0);
        const auto& ev = *scriptResults->Get();
        UNIT_ASSERT_VALUES_EQUAL_C(ev.Status, Ydb::StatusIds::SUCCESS, ev.Issues.ToString());
        UNIT_ASSERT(!ev.HasMoreResults);
        UNIT_ASSERT(ev.ResultSet);

        const auto& resultSet = *ev.ResultSet;
        UNIT_ASSERT_VALUES_EQUAL(resultSet.rows_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.columns_size(), 2);

        NYdb::TResultSetParser parser(resultSet);
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Key").GetInt32(), 42);
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Value").GetString(), "Some-Val");
    }

    Y_UNIT_TEST(RestartQueryWithGetOperation) {
        constexpr TDuration BACKOFF_DURATION = TDuration::Seconds(5);

        TScriptExecutionsYdbSetup ydb;

        const auto executionId = ExecuteQueryToRetry(ydb, BACKOFF_DURATION);

        Sleep(BACKOFF_DURATION);
        ydb.WaitQueryFinish(executionId);

        CheckQueryResults(ydb, executionId);
    }

    Y_UNIT_TEST(BackgroundOperationRestart) {
        constexpr TDuration BACKOFF_DURATION = TDuration::Seconds(5);

        TScriptExecutionsYdbSetup ydb(/* enableScriptExecutionBackgroundChecks */ true);

        const auto executionId = ExecuteQueryToRetry(ydb, BACKOFF_DURATION);

        // Wait background retry
        Sleep(BACKOFF_DURATION);
        ydb.WaitOperationStatus(executionId, Ydb::StatusIds::SUCCESS);

        ydb.CheckLeaseExistence(executionId, false, Ydb::StatusIds::SUCCESS);
        CheckQueryResults(ydb, executionId);
    }

    Y_UNIT_TEST(BackgroundOperationFinalization) {
        TScriptExecutionsYdbSetup ydb(/* enableScriptExecutionBackgroundChecks */ true);

        const TString executionId = ydb.CreateQueryInDb();
        UNIT_ASSERT(executionId);

        ydb.CheckLeaseExistence(executionId, true, std::nullopt);

        // Wait background finalization
        Sleep(TestLeaseDuration);
        ydb.WaitOperationStatus(executionId, Ydb::StatusIds::UNAVAILABLE);

        ydb.CheckLeaseExistence(executionId, false, Ydb::StatusIds::UNAVAILABLE);
    }

    Y_UNIT_TEST(BackgroundChecksStartAfterRestart) {
        TScriptExecutionsYdbSetup ydb(/* enableScriptExecutionBackgroundChecks */ false);

        const TString executionId = ydb.CreateQueryInDb();
        UNIT_ASSERT(executionId);

        ydb.CheckLeaseExistence(executionId, true, std::nullopt);

        // Wait background finalization
        Sleep(TestLeaseDuration);
        ydb.GetRuntime()->Register(CreateKqpFinalizeScriptService({}, nullptr, nullptr, true));
        ydb.WaitOperationStatus(executionId, Ydb::StatusIds::UNAVAILABLE);

        ydb.CheckLeaseExistence(executionId, false, Ydb::StatusIds::UNAVAILABLE);
    }
}

Y_UNIT_TEST_SUITE(TestScriptExecutionsUtils) {
    Y_UNIT_TEST(TestRetryPolicyItem) {
        NKikimrKqp::TScriptExecutionRetryState retryState;

        {
            auto& mapping = *retryState.AddRetryPolicyMapping();
            mapping.AddStatusCode(Ydb::StatusIds::SCHEME_ERROR);
            mapping.MutableBackoffPolicy()->SetRetryRateLimit(42);
        }

        {
            auto& mapping = *retryState.AddRetryPolicyMapping();
            mapping.AddStatusCode(Ydb::StatusIds::UNAVAILABLE);
            mapping.AddStatusCode(Ydb::StatusIds::INTERNAL_ERROR);
            mapping.MutableBackoffPolicy()->SetRetryRateLimit(84);
        }

        const auto checkStatus = [&](Ydb::StatusIds::StatusCode status, ui64 expectedRateLimit) {
            const auto policy = TRetryPolicyItem::FromProto(status, retryState);
            UNIT_ASSERT_VALUES_EQUAL(policy.RetryCount, expectedRateLimit);
        };

        checkStatus(Ydb::StatusIds::SCHEME_ERROR, 42);
        checkStatus(Ydb::StatusIds::UNAVAILABLE, 84);
        checkStatus(Ydb::StatusIds::INTERNAL_ERROR, 84);
        checkStatus(Ydb::StatusIds::BAD_REQUEST, 0);
    }

    Y_UNIT_TEST(TestRetryLimiter) {
        constexpr ui64 RETRY_COUNT = 10;
        const TInstant now = TInstant::Now();

        TRetryLimiter limiter;
        limiter.Assign(RETRY_COUNT, now, 0.0);
        UNIT_ASSERT_VALUES_EQUAL(limiter.RetryCount, RETRY_COUNT);
        UNIT_ASSERT_VALUES_EQUAL(limiter.RetryCounterUpdatedAt, now);
        UNIT_ASSERT_VALUES_EQUAL(limiter.RetryRate, 0.0);

        constexpr TDuration RETRY_PERIOD = TDuration::Seconds(1);
        constexpr TDuration BACKOFF_DURATION = TDuration::Seconds(1);

        {   // Retry rate limit
            TRetryPolicyItem policy(RETRY_COUNT, 0, RETRY_PERIOD, BACKOFF_DURATION);

            for (ui64 i = 0; i <= 2 * RETRY_COUNT; ++i) {
                Sleep(RETRY_PERIOD / (2 * RETRY_COUNT));
                if (i < 2 * RETRY_COUNT) {
                    UNIT_ASSERT_C(limiter.UpdateOnRetry(TInstant::Now(), policy), i << ": " << limiter.RetryRate << ", error=" << limiter.LastError);
                    UNIT_ASSERT_DOUBLES_EQUAL(limiter.Backoff.SecondsFloat(), (1.0 + 0.5 * static_cast<double>(i + 1)) * BACKOFF_DURATION.SecondsFloat(), 0.1);
                } else {
                    UNIT_ASSERT_C(!limiter.UpdateOnRetry(TInstant::Now(), policy), limiter.RetryRate);
                    UNIT_ASSERT_STRING_CONTAINS(limiter.LastError, TStringBuilder() << "failure rate " << limiter.RetryRate << " exceeds limit of "  << RETRY_COUNT);
                }
            }
        }

        {   // Retry count limit
            TRetryPolicyItem policy(8 * RETRY_COUNT, 4 * RETRY_COUNT, RETRY_PERIOD, BACKOFF_DURATION);

            for (ui64 i = 0; i <= RETRY_COUNT; ++i) {
                if (i < RETRY_COUNT) {
                    UNIT_ASSERT_C(limiter.UpdateOnRetry(TInstant::Now(), policy), i << ": rate=" << limiter.RetryRate << ", count=" << limiter.RetryCount << ", error=" << limiter.LastError);
                } else {
                    UNIT_ASSERT_C(!limiter.UpdateOnRetry(TInstant::Now(), policy), limiter.RetryCount);
                    UNIT_ASSERT_STRING_CONTAINS(limiter.LastError, TStringBuilder() << "retry count reached limit of " << 4 * RETRY_COUNT);
                }
            }
        }
    }
}

Y_UNIT_TEST_SUITE(TableCreation) {

    Y_UNIT_TEST(SimpleTableCreation) {
        TScriptExecutionsYdbSetup ydb;

        ydb.CreateTableInDbSync();
        ydb.VerifyColumnsList();
    }

    Y_UNIT_TEST(ConcurrentTableCreation) {
        TScriptExecutionsYdbSetup ydb;

        constexpr i32 requests = 20;

        ydb.CreateTableInDbSync(DEFAULT_COLUMNS, requests);
        ydb.VerifyColumnsList();
    }

    Y_UNIT_TEST(MultipleTablesCreation) {
        TScriptExecutionsYdbSetup ydb;

        constexpr i32 requests = 2;

        auto uniqueTablePath = TEST_TABLE_PATH;
        TVector<TActorId> edgeActors;
        for (i32 i = 0; i < requests; ++i) {
            uniqueTablePath.back() = TEST_TABLE_PATH.back() + ToString(i);
            edgeActors.push_back(ydb.CreateTableInDbAsync(DEFAULT_COLUMNS, uniqueTablePath));
        }

        ydb.WaitTableCreation(std::move(edgeActors));

        for(i32 i = 0; i < requests; i++) {
            uniqueTablePath.back() = TEST_TABLE_PATH.back() + ToString(i);
            ydb.VerifyColumnsList(uniqueTablePath);
        }
    }

    Y_UNIT_TEST(ConcurrentMultipleTablesCreation) {
        TScriptExecutionsYdbSetup ydb;

        constexpr i32 tables = 2;
        constexpr i32 requests = 20;

        auto uniqueTablePath = TEST_TABLE_PATH;
        TVector<TActorId> edgeActors;
        for (i32 i = 0; i < tables; ++i) {
            uniqueTablePath.back() = TEST_TABLE_PATH.back() + ToString(i);
            for (i32 j = 0; j < requests; ++j) {
                edgeActors.push_back(ydb.CreateTableInDbAsync(DEFAULT_COLUMNS, uniqueTablePath));
            }
        }

        ydb.WaitTableCreation(std::move(edgeActors));

        for(i32 i = 0; i < tables; i++) {
            uniqueTablePath.back() = TEST_TABLE_PATH.back() + ToString(i);
            ydb.VerifyColumnsList(uniqueTablePath);
        }
    }

    Y_UNIT_TEST(ConcurrentTableCreationWithDifferentVersions) {
        TScriptExecutionsYdbSetup ydb;

        constexpr i32 requests = 10;

        TVector<TActorId> edgeActors;
        for (i32 i = 0; i < requests; ++i) {
            edgeActors.push_back(ydb.CreateTableInDbAsync(i % 2 ? EXTENDED_COLUMNS : DEFAULT_COLUMNS));

        }

        ydb.WaitTableCreation(edgeActors);
        ydb.VerifyColumnsList(TEST_TABLE_PATH, EXTENDED_COLUMNS);
    }

    Y_UNIT_TEST(SimpleUpdateTable) {
        TScriptExecutionsYdbSetup ydb;

        ydb.CreateTableInDbSync(DEFAULT_COLUMNS);
        ydb.CreateTableInDbSync(EXTENDED_COLUMNS);
        ydb.VerifyColumnsList(TEST_TABLE_PATH, EXTENDED_COLUMNS);
    }

    Y_UNIT_TEST(ConcurrentUpdateTable) {
        TScriptExecutionsYdbSetup ydb;

        constexpr i32 requests = 10;

        ydb.CreateTableInDbSync(DEFAULT_COLUMNS);
        ydb.CreateTableInDbSync(EXTENDED_COLUMNS, requests);

        ydb.VerifyColumnsList(TEST_TABLE_PATH, EXTENDED_COLUMNS);
    }

    Y_UNIT_TEST(CreateOldTable) {
        TScriptExecutionsYdbSetup ydb;

        ydb.CreateTableInDbSync(EXTENDED_COLUMNS);
        ydb.CreateTableInDbSync(DEFAULT_COLUMNS);
        ydb.VerifyColumnsList(TEST_TABLE_PATH, EXTENDED_COLUMNS);
    }

}

} // namespace NKikimr::NKqp
