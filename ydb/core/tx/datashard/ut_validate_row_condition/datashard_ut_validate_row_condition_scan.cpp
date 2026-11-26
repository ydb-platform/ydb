#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/protos/set_column_constraint.pb.h>
#include <ydb/core/tablet/resource_broker.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardCheckConstraintScan) {

    Y_UNIT_TEST_TWIN(SimpleCheck, ZeroMemoryLimit) {
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

        auto tableId = ResolveTableId(server, sender, "/Root/test");

        if (ZeroMemoryLimit) {
            using namespace NResourceBroker;
            auto configure = MakeHolder<TEvResourceBroker::TEvConfigure>();
            configure->Merge = true;
            auto* queue = configure->Record.AddQueues();
            queue->SetName("queue_build_index");
            queue->MutableLimit()->SetMemory(0);
            runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, configure.Release()));
            auto reply = runtime.GrabEdgeEventRethrow<TEvResourceBroker::TEvConfigureResult>(sender);
            UNIT_ASSERT(reply->Get()->Record.GetSuccess());
        }

        auto request = MakeHolder<TEvDataShard::TEvValidateRowConditionRequest>();
        request->Record.SetId(100);
        request->Record.SetTabletId(shards[0]);
        request->Record.SetOwnerId(tableId.PathId.OwnerId);
        request->Record.SetPathId(tableId.PathId.LocalPathId);
        request->Record.AddNotNullColumns("value");

        runtime.SendToPipe(shards[0], sender, request.Release(), 0, GetPipeConfigWithRetries());

        {
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvValidateRowConditionResponse>(handle);
            if (ZeroMemoryLimit) {
                UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrSetColumnConstraint::EValidateStatus::BAD_REQUEST);
                UNIT_ASSERT_VALUES_EQUAL(reply->Record.IssuesSize(), 1);
                UNIT_ASSERT_STRING_CONTAINS(reply->Record.GetIssues(0).message(), "scan memory reservation exceeds the current broker limit");
            } else {
                UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrSetColumnConstraint::EValidateStatus::DONE);
                UNIT_ASSERT(reply->Record.GetIsValid());
            }
        }
    }

    Y_UNIT_TEST(CheckWithNulls) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "test_nulls", 1);
        auto shards = GetTableShards(server, sender, "/Root/test_nulls");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/test_nulls` (key, value) VALUES (1, 1), (2, NULL);");

        auto tableId = ResolveTableId(server, sender, "/Root/test_nulls");

        auto request = MakeHolder<TEvDataShard::TEvValidateRowConditionRequest>();
        request->Record.SetId(101);
        request->Record.SetTabletId(shards[0]);
        request->Record.SetOwnerId(tableId.PathId.OwnerId);
        request->Record.SetPathId(tableId.PathId.LocalPathId);
        request->Record.AddNotNullColumns("value");

        runtime.SendToPipe(shards[0], sender, request.Release(), 0, GetPipeConfigWithRetries());

        {
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvValidateRowConditionResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrSetColumnConstraint::EValidateStatus::DONE);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetIsValid(), false);
        }
    }

    Y_UNIT_TEST(CheckSpecificColumn) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "test_cols",
            TShardedTableOptions().Columns({
                {"key", "Uint32", true, false},
                {"col2", "Uint32", false, false},
                {"col3", "Uint32", false, false}
            }));
        auto shards = GetTableShards(server, sender, "/Root/test_cols");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/test_cols` (key, col2, col3) VALUES (1, NULL, 10);");

        auto tableId = ResolveTableId(server, sender, "/Root/test_cols");

        auto request = MakeHolder<TEvDataShard::TEvValidateRowConditionRequest>();
        request->Record.SetId(102);
        request->Record.SetTabletId(shards[0]);
        request->Record.SetOwnerId(tableId.PathId.OwnerId);
        request->Record.SetPathId(tableId.PathId.LocalPathId);
        request->Record.AddNotNullColumns("col3");

        runtime.SendToPipe(shards[0], sender, request.Release(), 0, GetPipeConfigWithRetries());

        {
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvValidateRowConditionResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrSetColumnConstraint::EValidateStatus::DONE);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetIsValid(), true);
        }
    }

    Y_UNIT_TEST(CheckSpecificColumnWithNull) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "test_cols_null",
            TShardedTableOptions().Columns({
                {"key", "Uint32", true, false},
                {"col2", "Uint32", false, false},
                {"col3", "Uint32", false, false}
            }));
        auto shards = GetTableShards(server, sender, "/Root/test_cols_null");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/test_cols_null` (key, col2, col3) VALUES (1, NULL, 10);");

        auto tableId = ResolveTableId(server, sender, "/Root/test_cols_null");

        auto request = MakeHolder<TEvDataShard::TEvValidateRowConditionRequest>();
        request->Record.SetId(103);
        request->Record.SetTabletId(shards[0]);
        request->Record.SetOwnerId(tableId.PathId.OwnerId);
        request->Record.SetPathId(tableId.PathId.LocalPathId);
        request->Record.AddNotNullColumns("col2");

        runtime.SendToPipe(shards[0], sender, request.Release(), 0, GetPipeConfigWithRetries());

        {
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvValidateRowConditionResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), NKikimrSetColumnConstraint::EValidateStatus::DONE);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetIsValid(), false);
        }
    }
}

} // namespace NKikimr
