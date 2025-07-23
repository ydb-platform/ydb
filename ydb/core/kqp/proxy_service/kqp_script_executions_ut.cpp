#include "kqp_script_executions.h"
#include "kqp_script_executions_impl.h"

#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/util/proto_duration.h>
#include <ydb/library/table_creator/table_creator.h>
#include <ydb/services/ydb/ydb_common_ut.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/library/actors/interconnect/interconnect_impl.h>

namespace NKikimr::NKqp {

using namespace Tests;
using namespace NSchemeShard;

namespace  {

constexpr TDuration TestLeaseDuration = TDuration::Seconds(1);
constexpr TDuration TestTimeout = TDuration::Seconds(10);
constexpr TDuration TestOperationTtl = TDuration::Minutes(1);
constexpr TDuration TestResultsTtl = TDuration::Minutes(1);
const TString TestDatabase = "test_db";

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
    TScriptExecutionsYdbSetup() {
        Init();
    }

    void Init() {
        MsgBusPort = PortManager.GetPort(2134);
        GrpcPort = PortManager.GetPort(2135);
        ServerSettings = MakeHolder<Tests::TServerSettings>(MsgBusPort);
        ServerSettings->SetEnableScriptExecutionOperations(true);
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

    bool RunSelect42Script(ui32 node = 0) {
        TActorId edgeActor = GetRuntime()->AllocateEdgeActor(node);

        TActorId kqpProxy = MakeKqpProxyID(GetRuntime()->GetNodeId(node));

        auto ev = MakeHolder<TEvKqp::TEvScriptRequest>();
        auto& req = *ev->Record.MutableRequest();
        req.SetQuery("SELECT 42");
        req.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT);
        req.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        req.SetDatabase(ServerSettings->DomainName);

        GetRuntime()->Send(new IEventHandle(kqpProxy, edgeActor, ev.Release()), node);

        auto reply = GetRuntime()->GrabEdgeEvent<TEvKqp::TEvScriptResponse>(edgeActor, TestTimeout);
        Ydb::StatusIds::StatusCode status = reply->Get()->Status;
        return status == Ydb::StatusIds::SUCCESS;
    }

    // Creates query in db. Returns execution id
    TString CreateQueryInDb(const TString& query = "SELECT 42", TDuration leaseDuration = TestLeaseDuration, TDuration operationTtl = TestOperationTtl, TDuration resultsTtl = TestResultsTtl) {
        TString executionId = CreateGuidAsString();

        NKikimrKqp::TEvQueryRequest req;
        req.MutableRequest()->SetDatabase(TestDatabase);
        req.MutableRequest()->SetQuery(query);
        req.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);

        NKikimrKqp::TScriptExecutionOperationMeta meta;
        SetDuration(leaseDuration, *meta.MutableLeaseDuration());
        SetDuration(operationTtl, *meta.MutableOperationTtl());
        SetDuration(resultsTtl, *meta.MutableResultsTtl());

        TActorId edgeActor = GetRuntime()->AllocateEdgeActor(0);
        GetRuntime()->Register(NPrivate::CreateCreateScriptOperationQueryActor(executionId, NActors::TActorId(), req, meta), 0, 0, TMailboxType::Simple, 0, edgeActor);

        auto reply = GetRuntime()->GrabEdgeEvent<NPrivate::TEvPrivate::TEvCreateScriptOperationResponse>(edgeActor, TestTimeout);
        UNIT_ASSERT(reply->Get()->Status == Ydb::StatusIds::SUCCESS);
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

    void CheckLeaseExistence(const TString& executionId, bool expectedExistence, std::optional<i32> expectedStatus) {
        TString sql = R"(
            DECLARE $database As Utf8;
            DECLARE $execution_id As Utf8;

            SELECT COUNT(*)
            FROM `.metadata/script_execution_leases`
            WHERE database = $database AND execution_id = $execution_id;

            SELECT operation_status
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

        auto result = TableClientSession->ExecuteDataQuery(sql, NYdb::NTable::TTxControl::BeginTx().CommitTx(), params.Build()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        NYdb::TResultSetParser rs1 = result.GetResultSetParser(0);
        UNIT_ASSERT(rs1.TryNextRow());

        auto count = rs1.ColumnParser(0).GetUint64();
        UNIT_ASSERT_VALUES_EQUAL(count, expectedExistence ? 1 : 0);

        NYdb::TResultSetParser rs2 = result.GetResultSetParser(1);
        UNIT_ASSERT(rs2.TryNextRow());

        UNIT_ASSERT(rs2.ColumnParser("operation_status").GetOptionalInt32() == expectedStatus);
    }

    TEvScriptLeaseUpdateResponse::TPtr UpdateLease(const TString& executionId, TDuration leaseDuration, i64 leaseGeneration = 1) {
        const auto edgeActor = GetRuntime()->AllocateEdgeActor();
        GetRuntime()->Register(CreateScriptLeaseUpdateActor(edgeActor, TestDatabase, executionId, leaseDuration, leaseGeneration, nullptr));

        auto reply = GetRuntime()->GrabEdgeEvent<TEvScriptLeaseUpdateResponse>(edgeActor, TestTimeout);
        UNIT_ASSERT(reply);

        return reply;
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

}

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

        auto checkResult2 = ydb.CheckLeaseStatus(executionId);
        UNIT_ASSERT_VALUES_EQUAL(checkResult2->Get()->OperationStatus, Ydb::StatusIds::UNAVAILABLE);
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
