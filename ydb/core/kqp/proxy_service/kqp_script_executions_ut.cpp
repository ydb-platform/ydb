#include "kqp_script_executions.h"
#include "kqp_script_executions_impl.h"

#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/services/ydb/ydb_common_ut.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/actors/interconnect/interconnect_impl.h>

namespace NKikimr::NKqp {

using namespace Tests;
using namespace NSchemeShard;

namespace  {

constexpr TDuration TestLeaseDuration = TDuration::Seconds(1);
const TString TestDatabase = "test_db";

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

        // Logging
        GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
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
        while (!RunSelect42Script()) {
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

        auto reply = GetRuntime()->GrabEdgeEvent<TEvKqp::TEvScriptResponse>(edgeActor);
        Ydb::StatusIds::StatusCode status = reply->Get()->Status;
        return status == Ydb::StatusIds::SUCCESS;
    }

    // Creates query in db. Returns execution id
    TString CreateQueryInDb(const TString& query = "SELECT 42", TDuration leaseDuration = TestLeaseDuration) {
        TString executionId = CreateGuidAsString();
        NKikimrKqp::TEvQueryRequest req;
        req.MutableRequest()->SetDatabase(TestDatabase);
        req.MutableRequest()->SetQuery(query);
        req.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        const ui32 node = 0;
        TActorId edgeActor = GetRuntime()->AllocateEdgeActor(node);
        GetRuntime()->Register(NPrivate::CreateCreateScriptOperationQueryActor(executionId, NActors::TActorId(), req, leaseDuration), 0, 0, TMailboxType::Simple, 0, edgeActor);

        auto reply = GetRuntime()->GrabEdgeEvent<NPrivate::TEvPrivate::TEvCreateScriptOperationResponse>(edgeActor);
        UNIT_ASSERT(reply->Get()->Status == Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(executionId, reply->Get()->ExecutionId);
        return reply->Get()->ExecutionId;
    }

    NPrivate::TEvPrivate::TEvLeaseCheckResult::TPtr CheckLeaseStatus(const TString& executionId) {
        const ui32 node = 0;
        TActorId edgeActor = GetRuntime()->AllocateEdgeActor(node);
        GetRuntime()->Register(NPrivate::CreateCheckLeaseStatusActor(TestDatabase, executionId), 0, 0, TMailboxType::Simple, 0, edgeActor);

        auto reply = GetRuntime()->GrabEdgeEvent<NPrivate::TEvPrivate::TEvLeaseCheckResult>(edgeActor);
        UNIT_ASSERT(reply->Get()->Status == Ydb::StatusIds::SUCCESS);
        return reply;
    }

    void CheckLeaseExistance(const TString& executionId, bool expectedExistance, TMaybe<i32> expectedStatus) {
        TStringBuilder sql;
            sql <<
                R"(
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
            UNIT_ASSERT_VALUES_EQUAL(count, expectedExistance ? 1 : 0);

            NYdb::TResultSetParser rs2 = result.GetResultSetParser(1);
            UNIT_ASSERT(rs2.TryNextRow());

            UNIT_ASSERT_VALUES_EQUAL(rs2.ColumnParser("operation_status").GetOptionalInt32(), expectedStatus);
    }

    THolder<TEvScriptLeaseUpdateResponse> UpdateLease(const TString& executionId, TDuration leaseDuration) {
        GetRuntime()->Register(CreateScriptLeaseUpdateActor(GetRuntime()->AllocateEdgeActor(), TestDatabase, executionId, leaseDuration));
        auto reply = GetRuntime()->GrabEdgeEvent<TEvScriptLeaseUpdateResponse>();
        
        UNIT_ASSERT(reply != nullptr);
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
        ydb.CheckLeaseExistance(executionId, true, Nothing());
        auto checkResult1 = ydb.CheckLeaseStatus(executionId);
        const TDuration checkTime = TInstant::Now() - startLeaseTime;
        if (checkTime < TestLeaseDuration) {
            UNIT_ASSERT_VALUES_EQUAL(checkResult1->Get()->OperationStatus, Nothing());
            ydb.CheckLeaseExistance(executionId, true, Nothing());
            SleepUntil(startLeaseTime + TestLeaseDuration);
        }

        auto checkResult2 = ydb.CheckLeaseStatus(executionId);
        UNIT_ASSERT_VALUES_EQUAL(checkResult2->Get()->OperationStatus, Ydb::StatusIds::ABORTED);
        ydb.CheckLeaseExistance(executionId, false, Ydb::StatusIds::ABORTED);
    }
    
    Y_UNIT_TEST(UpdatesLeaseAfterExpiring) {
        TScriptExecutionsYdbSetup ydb;

        const TString executionId = ydb.CreateQueryInDb();
        UNIT_ASSERT(executionId);

        TInstant startLeaseTime = TInstant::Now();

        ydb.CheckLeaseExistance(executionId, true, Nothing());
        SleepUntil(startLeaseTime + TestLeaseDuration);

        startLeaseTime = TInstant::Now();
        TDuration leaseDuration = TDuration::Seconds(10);
        auto updateResponse = ydb.UpdateLease(executionId, leaseDuration);
        UNIT_ASSERT_C(updateResponse->Status == Ydb::StatusIds::SUCCESS, updateResponse->Issues.ToString());
        UNIT_ASSERT(updateResponse->ExecutionEntryExists);
        
        ydb.CheckLeaseExistance(executionId, true, Nothing());
        auto checkResult = ydb.CheckLeaseStatus(executionId);

        if (TInstant::Now() - startLeaseTime < leaseDuration) {
            UNIT_ASSERT_VALUES_EQUAL(checkResult->Get()->OperationStatus, Nothing());
        }
    }

    Y_UNIT_TEST(AttemptToUpdateDeletedLease) {
        TScriptExecutionsYdbSetup ydb;

        const TString executionId = ydb.CreateQueryInDb();
        UNIT_ASSERT(executionId);
        ydb.CheckLeaseExistance(executionId, true, Nothing());

        Sleep(TestLeaseDuration);

        auto checkResult = ydb.CheckLeaseStatus(executionId);
        UNIT_ASSERT_VALUES_EQUAL(checkResult->Get()->OperationStatus, Ydb::StatusIds::ABORTED);
        ydb.CheckLeaseExistance(executionId, false, Ydb::StatusIds::ABORTED);

        auto updateResponse = ydb.UpdateLease(executionId, TestLeaseDuration);
        UNIT_ASSERT(!updateResponse->ExecutionEntryExists);
    }
}
} // namespace NKikimr::NKqp
