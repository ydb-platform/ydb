#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/protos/set_column_constraint.pb.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardCheckConstraintScan) {

    Y_UNIT_TEST(SimpleCheck) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "test", 1);
        auto shards = GetTableShards(server, sender, "/Root/test");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/test` (key, value) VALUES (1, 1), (2, 2);");

        auto snapshot = CreateVolatileSnapshot(server, { "/Root/test" });
        auto tableId = ResolveTableId(server, sender, "/Root/test");

        auto request = MakeHolder<TEvDataShard::TEvCheckConstraintRequest>();
        request->Record.SetId(100);
        request->Record.SetTabletId(shards[0]);
        request->Record.SetOwnerId(tableId.PathId.OwnerId);
        request->Record.SetPathId(tableId.PathId.LocalPathId);
        auto* col = request->Record.AddCheckingColumns();
        col->SetColumnName("value");
        request->Record.SetSnapshotStep(snapshot.Step);
        request->Record.SetSnapshotTxId(snapshot.TxId);

        runtime.SendToPipe(shards[0], sender, request.Release(), 0, GetPipeConfigWithRetries());

        {
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvCheckConstraintResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL((ui32)reply->Record.GetStatus(), (ui32)NKikimrSetColumnConstraint::ECheckStatus::ACCEPTED);
        }

        {
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvCheckConstraintResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL((ui32)reply->Record.GetStatus(), (ui32)NKikimrSetColumnConstraint::ECheckStatus::SUCCESS);
        }
    }
}

} // namespace NKikimr