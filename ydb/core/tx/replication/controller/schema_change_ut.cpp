#include "controller_impl.h"

#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/replication/ut_helpers/mock_service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>

namespace NKikimr::NReplication::NController {

namespace {

NKikimrReplication::TSchemaChange MakeSchemaChange(
        ui64 step = 100, ui64 txId = 10, ui64 sourceSchemaVersion = 2,
        bool withExtraColumn = true) {
    NKikimrReplication::TSchemaChange schema;
    schema.MutableVersion()->SetStep(step);
    schema.MutableVersion()->SetTxId(txId);
    schema.SetSourceSchemaVersion(sourceSchemaVersion);

    auto* key = schema.AddColumns();
    key->SetName("key");
    key->SetType("Uint32");

    auto* value = schema.AddColumns();
    value->SetName("value");
    value->SetType("Utf8");

    if (withExtraColumn) {
        auto* extra = schema.AddColumns();
        extra->SetName("extra");
        extra->SetType("Uint64");
    }

    schema.AddPrimaryKeyColumnNames("key");
    return schema;
}

TEvService::TEvSchemaChangeReport* MakeSchemaChangeReport(
        const TWorkerId& id, const NKikimrReplication::TSchemaChange& schema,
        bool applied = false, bool completed = false) {
    auto* event = new TEvService::TEvSchemaChangeReport();
    id.Serialize(*event->Record.MutableWorker());
    event->Record.MutableSchema()->CopyFrom(schema);
    event->Record.SetApplied(applied);
    event->Record.SetCompleted(completed);
    return event;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(SchemaChangeBarrier) {
    using namespace NTestHelpers;

    Y_UNIT_TEST(WaitsForCompleteMembershipAndReleasesDuplicateReport) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
        }));

        const auto service = env.GetRuntime().Register(CreateReplicationMockService(env.GetSender()));
        env.GetRuntime().RegisterService(MakeReplicationServiceId(env.GetRuntime().GetNodeId(0)), service);

        NYdb::NTable::TTableClient client(env.GetDriver(), NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(env.GetEndpoint())
            .Database(env.GetDatabase()));
        auto session = client.CreateSession().GetValueSync().GetSession();
        const auto result = session.ExecuteSchemeQuery(Sprintf(R"(
            CREATE ASYNC REPLICATION `replication` FOR
                `/Root/table` AS `/Root/replica`
            WITH (CONNECTION_STRING = "grpc://%s/?database=/Root");
        )", env.GetEndpoint().c_str())).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        const auto controllerId = env.GetDescription("/Root/replication")
            .GetPathDescription().GetReplicationDescription().GetControllerId();

        // Consume the initial session handshake.
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(env.GetSender());

        // The production target registar supplies the first root-partition
        // worker.  Its command is also the authoritative replication/target
        // identity for the test.
        const auto firstRun = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto first = TWorkerId::Parse(firstRun->Get()->Record.GetWorker());
        const TWorkerId second(first.ReplicationId(), first.TargetId(), first.WorkerId() + 1);

        // Register a second root partition through the controller's normal
        // durable worker-registration transaction before either report.
        auto secondRun = MakeHolder<TEvService::TEvRunWorker>();
        second.Serialize(*secondRun->Record.MutableWorker());
        env.SendAsync(controllerId, secondRun.Release());

        // Give both workers a session so applied responses are observable via
        // the mock replication service.
        auto status = MakeHolder<TEvService::TEvStatus>();
        first.Serialize(*status->Record.AddWorkers());
        second.Serialize(*status->Record.AddWorkers());
        env.SendAsync(controllerId, status.Release());

        const auto schema = MakeSchemaChange();
        env.SendAsync(controllerId, MakeSchemaChangeReport(first, schema));

        // A single report must not begin DDL. The destination still lacks the
        // requested column until every worker in the durable roster reports.
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica")
            .GetPathDescription().GetTable().ColumnsSize(), 2);

        env.SendAsync(controllerId, MakeSchemaChangeReport(second, schema));

        const auto appliedFirst = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        const auto appliedFirstId = TWorkerId::Parse(appliedFirst->Get()->Record.GetWorker());
        UNIT_ASSERT(appliedFirstId == first || appliedFirstId == second);
        UNIT_ASSERT_VALUES_EQUAL(appliedFirst->Get()->Record.GetSchema().SerializeAsString(), schema.SerializeAsString());

        const auto appliedSecond = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        const auto appliedSecondId = TWorkerId::Parse(appliedSecond->Get()->Record.GetWorker());
        UNIT_ASSERT(appliedSecondId == first || appliedSecondId == second);
        UNIT_ASSERT_VALUES_UNEQUAL(appliedFirstId, appliedSecondId);

        const auto destinationDescription = env.GetDescription("/Root/replica");
        const auto& destination = destinationDescription.GetPathDescription().GetTable();
        UNIT_ASSERT_VALUES_EQUAL(destination.ColumnsSize(), 3);

        env.SendAsync(controllerId, MakeSchemaChangeReport(first, schema, true));
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());

        // A service that restarted with no workers subsequently boots this
        // worker and acknowledges STATUS_RUNNING. That acknowledgement must
        // receive the durable recovery release, not only the initial status
        // worker list.
        auto running = MakeHolder<TEvService::TEvWorkerStatus>(
            first, NKikimrReplication::TEvWorkerStatus::STATUS_RUNNING);
        env.SendAsync(controllerId, running.Release());
        const auto replayed = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(replayed->Get()->Record.GetApplied());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(replayed->Get()->Record.GetWorker()), first);
        UNIT_ASSERT_VALUES_EQUAL(replayed->Get()->Record.GetSchema().SerializeAsString(), schema.SerializeAsString());

        env.SendAsync(controllerId, MakeSchemaChangeReport(second, schema, true));
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());

        env.SendAsync(controllerId, MakeSchemaChangeReport(first, schema, false, true));
        auto completed = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(completed->Get()->Record.GetCompleted());
        env.SendAsync(controllerId, MakeSchemaChangeReport(second, schema, false, true));
        completed = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(completed->Get()->Record.GetCompleted());

        // A replay after APPLIED is an idempotent release: it must not start a
        // second DDL operation or change the durable schema.
        env.SendAsync(controllerId, MakeSchemaChangeReport(first, schema));
        const auto duplicate = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(duplicate->Get()->Record.GetWorker()), first);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica")
            .GetPathDescription().GetTable().ColumnsSize(), 3);
    }

    Y_UNIT_TEST(GlobalBackToBackSchemaChangesWaitForCompletionAndThenProgress) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);

        env.CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
            .Name = "table",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
            .ReplicationConfig = Nothing(),
        }));

        const auto service = env.GetRuntime().Register(CreateReplicationMockService(env.GetSender()));
        env.GetRuntime().RegisterService(MakeReplicationServiceId(env.GetRuntime().GetNodeId(0)), service);

        NYdb::NTable::TTableClient client(env.GetDriver(), NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(env.GetEndpoint())
            .Database(env.GetDatabase()));
        auto session = client.CreateSession().GetValueSync().GetSession();
        const auto create = session.ExecuteSchemeQuery(Sprintf(R"(
            CREATE ASYNC REPLICATION `replication` FOR
                `/Root/table` AS `/Root/replica`
            WITH (
                CONNECTION_STRING = "grpc://%s/?database=/Root",
                CONSISTENCY_LEVEL = "GLOBAL",
                COMMIT_INTERVAL = Interval("PT10S")
            );
        )", env.GetEndpoint().c_str())).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), NYdb::EStatus::SUCCESS, create.GetIssues().ToString());

        const auto controllerId = env.GetDescription("/Root/replication")
            .GetPathDescription().GetReplicationDescription().GetControllerId();
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(env.GetSender());

        const auto firstRun = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto first = TWorkerId::Parse(firstRun->Get()->Record.GetWorker());
        const TWorkerId second(first.ReplicationId(), first.TargetId(), first.WorkerId() + 1);

        auto secondRun = MakeHolder<TEvService::TEvRunWorker>();
        second.Serialize(*secondRun->Record.MutableWorker());
        env.SendAsync(controllerId, secondRun.Release());

        auto status = MakeHolder<TEvService::TEvStatus>();
        first.Serialize(*status->Record.AddWorkers());
        second.Serialize(*status->Record.AddWorkers());
        env.SendAsync(controllerId, status.Release());

        const auto txIdResult = env.Send<TEvService::TEvTxIdResult>(controllerId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(100, 0)}));
        UNIT_ASSERT_VALUES_EQUAL(txIdResult->Get()->Record.GetVersionTxIds(0).GetVersion().GetStep(), 10000);
        UNIT_ASSERT(txIdResult->Get()->Record.GetVersionTxIds(0).GetTxId());

        const auto addColumn = MakeSchemaChange();
        const auto dropColumn = MakeSchemaChange(200, 20, 3, false);

        env.SendAsync(controllerId, MakeSchemaChangeReport(first, addColumn));
        env.SendAsync(controllerId, MakeSchemaChangeReport(second, addColumn));

        // GLOBAL DDL completion leaves the first barrier in Verifying until a
        // post-schema quorum is observed. Both workers nevertheless may have
        // already reached the next CDC schema record. These reports must be
        // deferred, rather than turning the current barrier into an error.
        auto firstApplied = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(firstApplied->Get()->Record.GetSchema().SerializeAsString(), addColumn.SerializeAsString());
        firstApplied = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(firstApplied->Get()->Record.GetSchema().SerializeAsString(), addColumn.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica")
            .GetPathDescription().GetTable().ColumnsSize(), 3);

        env.SendAsync(controllerId, new TEvents::TEvPoisonPill());
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(env.GetSender());

        auto recoveredSecondRun = MakeHolder<TEvService::TEvRunWorker>();
        second.Serialize(*recoveredSecondRun->Record.MutableWorker());
        env.SendAsync(controllerId, recoveredSecondRun.Release());

        auto recoveredStatus = MakeHolder<TEvService::TEvStatus>();
        first.Serialize(*recoveredStatus->Record.AddWorkers());
        second.Serialize(*recoveredStatus->Record.AddWorkers());
        env.SendAsync(controllerId, recoveredStatus.Release());

        env.SendAsync(controllerId, MakeSchemaChangeReport(first, dropColumn));
        env.SendAsync(controllerId, MakeSchemaChangeReport(second, dropColumn));

        // Pump the deferred reports through the controller. They must not
        // execute the second DDL while the first barrier is still active.
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica")
            .GetPathDescription().GetTable().ColumnsSize(), 3);

        for (const auto& worker : {first, second}) {
            env.SendAsync(controllerId, MakeSchemaChangeReport(worker, addColumn, true));
            auto acknowledgement = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
            UNIT_ASSERT(acknowledgement->Get()->Record.GetApplied());

            env.SendAsync(controllerId, MakeSchemaChangeReport(worker, addColumn, false, true));
            acknowledgement = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
            UNIT_ASSERT(acknowledgement->Get()->Record.GetCompleted());
        }

        // The previous data-plane barrier is fully completed, but its write
        // transaction still waits for heartbeat boundary 10000. Retrying the
        // already observed next records must nevertheless install a new
        // barrier and perform DROP COLUMN.
        env.SendAsync(controllerId, MakeSchemaChangeReport(first, dropColumn));
        env.SendAsync(controllerId, MakeSchemaChangeReport(second, dropColumn));

        auto secondApplied = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(secondApplied->Get()->Record.GetSchema().SerializeAsString(), dropColumn.SerializeAsString());
        secondApplied = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(secondApplied->Get()->Record.GetSchema().SerializeAsString(), dropColumn.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica")
            .GetPathDescription().GetTable().ColumnsSize(), 2);
    }
}

} // NKikimr::NReplication::NController
