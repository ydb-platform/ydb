#include "datashard.h"
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include "datashard_ut_common_kqp.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardLockRows) {

    std::tuple<TTestActorRuntime&, Tests::TServer::TPtr, TActorId> TestCreateServer(TPortManager& pm, std::optional<TServerSettings> serverSettings = {}) {
        if (!serverSettings) {
            serverSettings.emplace(pm.GetPort(2134));
            serverSettings->SetDomainName("Root").SetUseRealThreads(false);
        }

        Tests::TServer::TPtr server = new TServer(serverSettings.value());
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        return {runtime, server, sender};
    }

    class TTestPipe {
    public:
        TTestPipe(TTestActorRuntime& runtime, ui64 tabletId, ui32 nodeIndex = 0)
            : Runtime(runtime)
            , TabletId(tabletId)
        {
            PipeActor = Runtime.ConnectToPipe(TabletId, TActorId(), nodeIndex, NTabletPipe::TClientConfig{});
        }

        ~TTestPipe() {
            ui32 nodeIndex = PipeActor.NodeId() - Runtime.GetNodeId(0);
            Runtime.ClosePipe(PipeActor, TActorId(), nodeIndex);
        }

        void Send(const TActorId& sender, IEventBase* payload, ui64 cookie = 0) {
            ui32 nodeIndex = PipeActor.NodeId() - Runtime.GetNodeId(0);
            Runtime.SendToPipe(PipeActor, sender, payload, nodeIndex, cookie);
        }

    private:
        TTestActorRuntime& Runtime;
        const ui64 TabletId;
        TActorId PipeActor;
    };

    Y_UNIT_TEST(Basics) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));

        NLongTxService::TLockHandle lock1(123, runtime.GetActorSystem(0));
        NLongTxService::TLockHandle lock2(234, runtime.GetActorSystem(0));

        // Lock key 1 by lock 1
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(1);
            req->Record.SetLockId(lock1.GetLockId());
            req->Record.SetLockNodeId(lock1.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 1, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }

        // Try locking key 1 by lock 2
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(2);
            req->Record.SetLockId(lock2.GetLockId());
            req->Record.SetLockNodeId(lock2.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 1, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        // Try locking key 1 by lock 1 again
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(3);
            req->Record.SetLockId(lock1.GetLockId());
            req->Record.SetLockNodeId(lock1.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 1, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }

        // Try waiting for lock 2 some more
        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        // Destroy lock 1 handle, it must rollback all changes
        lock1.Reset();

        // Try waiting for lock 2 now
        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }
    }

    Y_UNIT_TEST(RequestCancelledOnSplit) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));

        NLongTxService::TLockHandle lock1(123, runtime.GetActorSystem(0));
        NLongTxService::TLockHandle lock2(234, runtime.GetActorSystem(0));

        // Lock key 1 by lock 1
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(1);
            req->Record.SetLockId(lock1.GetLockId());
            req->Record.SetLockNodeId(lock1.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 1, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }

        // Try locking key 1 by lock 2
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(2);
            req->Record.SetLockId(lock2.GetLockId());
            req->Record.SetLockNodeId(lock2.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 1, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(&runtime, -1);

        auto makeSplitKey = [](i32 value) {
            NKikimrMiniKQL::TValue proto;
            proto.SetInt32(value);
            return proto;
        };

        auto senderSplit = runtime.AllocateEdgeActor();
        ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table", shards.at(0), makeSplitKey(3));
        WaitTxNotification(server, senderSplit, txId);

        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(ev, "Unexpected failure to receive TEvLockRowsResult");
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_OVERLOADED);
        }
    }

    Y_UNIT_TEST(RequestCancelledOnGracefulRestart) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));

        NLongTxService::TLockHandle lock1(123, runtime.GetActorSystem(0));
        NLongTxService::TLockHandle lock2(234, runtime.GetActorSystem(0));

        // Lock key 1 by lock 1
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(1);
            req->Record.SetLockId(lock1.GetLockId());
            req->Record.SetLockNodeId(lock1.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 1, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender);
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS);
        }

        // Try locking key 1 by lock 2
        {
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(2);
            req->Record.SetLockId(lock2.GetLockId());
            req->Record.SetLockNodeId(lock2.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            TVector<TCell> cells({
                TCell::Make(i32(1)),
            });
            req->SetCellMatrix(TSerializedCellMatrix::Serialize(cells, 1, 1));
            pipe.Send(sender, req.release());

            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult with status " << ev->Get()->Record.GetStatus());
        }

        GracefulRestartTablet(runtime, shards.at(0), sender);

        {
            auto ev = runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(ev, "Unexpected failure to receive TEvLockRowsResult");
            auto* res = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrDataEvents::TEvLockRowsResult::STATUS_OVERLOADED);
        }
    }

} // Y_UNIT_TEST_SUITE(DataShardLockRows)

} // namespace NKikimr
