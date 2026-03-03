#include "datashard.h"
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include "datashard_ut_common_kqp.h"

#include <util/digest/multi.h>

namespace {
    struct TRequestId {
        NActors::TActorId Sender;
        ui64 Id;

        friend bool operator==(const TRequestId&, const TRequestId&) = default;
    };
} // namespace

template<> struct THash<TRequestId> {
    size_t operator()(const TRequestId& value) const {
        return ::MultiHash(value.Sender, value.Id);
    }
};

template<> struct std::hash<TRequestId> {
    size_t operator()(const TRequestId& value) const {
        return ::THash<TRequestId>()(value);
    }
};

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NLongTxService;
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

        ui64 GetTabletId() const {
            return TabletId;
        }

        ui32 GetNodeIndex() const {
            return PipeActor.NodeId() - Runtime.GetNodeId(0);
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

    class TKeysBuilder {
    public:
        TKeysBuilder() = default;

        TKeysBuilder&& Add(i32 key) && {
            Cells.push_back(TCell::Make(key));
            ++Rows;
            return std::move(*this);
        }

        TSerializedCellMatrix Build() && {
            return TSerializedCellMatrix(TSerializedCellMatrix::Serialize(Cells, Rows, 1));
        }

    private:
        TVector<TCell> Cells;
        size_t Rows = 0;
    };

    class TLockRowsHelper {
    public:
        TLockRowsHelper(TTestActorRuntime& runtime, TTestPipe& pipe)
            : Runtime(runtime)
            , Pipe(pipe)
            , Sender(Runtime.AllocateEdgeActor(pipe.GetNodeIndex()))
        {}

        template<class TCallback>
        ui64 SendRequest(const TLockHandle& lock, const TTableId& tableId, TSerializedCellMatrix keys, const TCallback& callback) {
            ui64 requestId = NextRequestId++;
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(requestId);
            req->Record.SetLockId(lock.GetLockId());
            req->Record.SetLockNodeId(lock.GetLockNodeId());
            req->Record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            req->SetCellMatrix(keys.ReleaseBuffer());
            callback(req.get());
            Pipe.Send(Sender, req.release());
            return requestId;
        }

        ui64 SendRequest(const TLockHandle& lock, const TTableId& tableId, TSerializedCellMatrix keys) {
            return SendRequest(lock, tableId, std::move(keys), [](auto*){});
        }

        std::unique_ptr<NEvents::TDataEvents::TEvLockRowsResult> ExpectResult(ui64 requestId, NKikimrDataEvents::TEvLockRowsResult::EStatus status = NKikimrDataEvents::TEvLockRowsResult::STATUS_SUCCESS, TDuration simTimeout = TDuration::Seconds(1)) {
            auto ev = Runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(Sender, simTimeout);
            UNIT_ASSERT_C(ev, "Expected TEvLockRowsResult was not received after " << simTimeout);
            std::unique_ptr<NEvents::TDataEvents::TEvLockRowsResult> res(ev->Release().Release());
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRequestId(), requestId);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), status);
            return res;
        }

        void ExpectNoResult(TDuration simTimeout = TDuration::Seconds(1)) {
            auto ev = Runtime.GrabEdgeEventRethrow<NEvents::TDataEvents::TEvLockRowsResult>(Sender, simTimeout);
            UNIT_ASSERT_C(!ev, "Unexpected TEvLockRowsResult received: RequestId# " << ev->Get()->Record.GetRequestId() << " Status# " << ev->Get()->Record.GetStatus());
        }

    private:
        TTestActorRuntime& Runtime;
        TTestPipe& Pipe;
        const TActorId Sender;
        ui64 NextRequestId = 1;
    };

    struct TWaitGraphEdge {
        ui64 LockId;
        ui64 OtherLockId;

        friend std::strong_ordering operator<=>(const TWaitGraphEdge&, const TWaitGraphEdge&) = default;
    };

    class TWaitGraphInterceptor : public THashMap<TRequestId, TWaitGraphEdge> {
    public:
        TWaitGraphInterceptor(TTestActorRuntime& runtime)
            : Runtime(runtime)
            , AddObserver(Runtime.AddObserver<TEvLongTxService::TEvWaitingLockAdd>(
                [this](auto& ev) {
                    auto* msg = ev->Get();
                    (*this)[TRequestId{ ev->Sender, msg->RequestId }] = TWaitGraphEdge{ msg->Lock.LockId, msg->OtherLock.LockId };
                    // Prevent long tx service from processing this event
                    ev.Reset();
                }))
            , RemoveObserver(Runtime.AddObserver<TEvLongTxService::TEvWaitingLockRemove>(
                [this](auto& ev) {
                    auto* msg = ev->Get();
                    (*this).erase(TRequestId{ ev->Sender, msg->RequestId });
                    // Prevent long tx service from processing this event
                }))
        {}

        void SendDeadlock(ui64 lockId, ui64 otherLockId) const {
            for (const auto& pr : *this) {
                if (pr.second.LockId == lockId && pr.second.OtherLockId == otherLockId) {
                    ui32 nodeIndex = pr.first.Sender.NodeId() - Runtime.GetNodeId(0);
                    Runtime.Send(
                        new IEventHandle(
                            pr.first.Sender,
                            TActorId(),
                            new TEvLongTxService::TEvWaitingLockDeadlock(pr.first.Id)),
                        nodeIndex, /* viaActorSystem */ true);
                }
            }
        }

        TString ToString() const {
            TVector<TWaitGraphEdge> edges;
            for (auto& pr : *this) {
                edges.push_back(pr.second);
            }
            std::sort(edges.begin(), edges.end());
            TStringBuilder sb;
            for (auto& edge : edges) {
                sb << edge.LockId << " -> " << edge.OtherLockId << "\n";
            }
            return sb;
        }

    private:
        TTestActorRuntime& Runtime;
        TTestActorRuntime::TEventObserverHolder AddObserver;
        TTestActorRuntime::TEventObserverHolder RemoveObserver;
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

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockRowsHelper lockRows(runtime, pipe);

        // Lock key 1 by lock 1
        auto req1 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req1);

        // Try locking key 1 by lock 2
        auto req2 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        // Try locking key 1 by lock 1 again
        auto req3 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req3);

        // Try waiting for lock 2 some more
        lockRows.ExpectNoResult();

        // Destroy lock 1 handle, it must rollback all changes
        lock1.Reset();

        // Try waiting for lock 2 result now
        lockRows.ExpectResult(req2);
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

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockRowsHelper lockRows(runtime, pipe);

        // Lock key 1 by lock 1
        auto req1 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req1);

        // Try locking key 1 by lock 2
        auto req2 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

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

        lockRows.ExpectResult(req2, NKikimrDataEvents::TEvLockRowsResult::STATUS_OVERLOADED);
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

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockRowsHelper lockRows(runtime, pipe);

        // Lock key 1 by lock 1
        auto req1 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req1);

        // Try locking key 1 by lock 2
        auto req2 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        GracefulRestartTablet(runtime, shards.at(0), sender);

        lockRows.ExpectResult(req2, NKikimrDataEvents::TEvLockRowsResult::STATUS_OVERLOADED);
    }

    Y_UNIT_TEST(BrokenOwnerUnblocksAwaiter) {
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

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockRowsHelper lockRows(runtime, pipe);

        TWaitGraphInterceptor waitGraph(runtime);

        // Lock keys 1 and 2 by lock 1
        auto req1 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Add(2).Build());
        lockRows.ExpectResult(req1);

        // Lock keys 2 and 3 by lock 2
        auto req2 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(2).Add(3).Build());
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "234 -> 123\n");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (1, 101);
            )"),
            "<empty>");

        // Lock 2 must succeed when the current owner is broken
        lockRows.ExpectResult(req2);

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "");
    }

    Y_UNIT_TEST(BrokenAwaiterUnblocksAwait) {
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

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockRowsHelper lockRows(runtime, pipe);

        TWaitGraphInterceptor waitGraph(runtime);

        // Lock keys 1 and 2 by lock 1
        auto req1 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Add(2).Build());
        lockRows.ExpectResult(req1);

        // Lock keys 3 and 2 by lock 2
        auto req2 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(3).Add(2).Build());
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "234 -> 123\n");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (3, 101);
            )"),
            "<empty>");

        // Lock 2 must fail as soon as its lock is broken
        lockRows.ExpectResult(req2, NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN);

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "");
    }

    Y_UNIT_TEST(RollbackAwaiterUnblocksAwait) {
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

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockRowsHelper lockRows(runtime, pipe);

        TWaitGraphInterceptor waitGraph(runtime);

        // Lock keys 1 and 2 by lock 1
        auto req1 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Add(2).Build());
        lockRows.ExpectResult(req1);

        // Lock keys 3 and 2 by lock 2
        auto req2 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(3).Add(2).Build());
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "234 -> 123\n");

        // Break the second lock (it will notify the long tx service and rollback datashard changes)
        lock2.Reset();

        // Lock 2 must fail as soon as its lock is rolled back
        lockRows.ExpectResult(req2, NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN);

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "");
    }

    Y_UNIT_TEST(DeadlockDetectionUnblocksAwait) {
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

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockHandle lock3(345, runtime.GetActorSystem(0));
        TLockRowsHelper lockRows(runtime, pipe);

        TWaitGraphInterceptor waitGraph(runtime);

        // Lock keys 1 and 2 by lock 1
        auto req1 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Add(2).Build());
        lockRows.ExpectResult(req1);

        // Lock keys 2 and 3 by lock 2
        auto req2 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(2).Add(3).Build());
        lockRows.ExpectNoResult();

        // Lock keys 3 and 2 by lock 3
        auto req3 = lockRows.SendRequest(lock3, tableId, TKeysBuilder().Add(3).Add(2).Build());
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "234 -> 123\n"
            "345 -> 234\n");

        // Break the current owner
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (1, 101);
            )"),
            "<empty>");

        // Both operations should be blocked waiting for each other
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "234 -> 345\n"
            "345 -> 234\n");

        // Ask the third (yongest) operation to break the deadlock
        waitGraph.SendDeadlock(345, 234);

        // The third operation should reply with the STATUS_DEADLOCK
        lockRows.ExpectResult(req3, NKikimrDataEvents::TEvLockRowsResult::STATUS_DEADLOCK);

        // Perhaps unexpectedly the row 3 is still locked until the lock is broken or removed
        lockRows.ExpectNoResult();

        // Reset the third lock (it will notify the long tx service and rollback datashard changes)
        lock3.Reset();

        // Finally the second operation should successfully lock all rows
        lockRows.ExpectResult(req2);

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "");
    }

    Y_UNIT_TEST(LockOverUncommittedChangeThenCommitBreaksLock) {
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

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockRowsHelper lockRows(runtime, pipe);

        // Write an uncommitted row at key 1
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (1, 101);
                SELECT key, value FROM `/Root/table` WHERE key = 1;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 101 } }");

        // Lock key 1 by lock 1
        auto req1 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req1);

        // Commit the previous transaction
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, R"(SELECT 1)"),
            "{ items { int32_value: 1 } }");

        // Lock key 2 by lock 1 (lock must be broken now)
        auto req2 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(2).Build());
        lockRows.ExpectResult(req2, NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN);
    }

    Y_UNIT_TEST(UncommittedChangeOverLockThenCommitBreaksLock) {
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

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockRowsHelper lockRows(runtime, pipe);

        // Lock key 1 by lock 1
        auto req1 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req1);

        // Write an uncommitted row at key 1
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (1, 101);
                SELECT key, value FROM `/Root/table` WHERE key = 1;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 101 } }");

        // Lock key 2 by lock 1 (lock not broken yet)
        auto req2 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(2).Build());
        lockRows.ExpectResult(req2);

        // Commit the previous transaction
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, R"(SELECT 1)"),
            "{ items { int32_value: 1 } }");

        // Lock key 3 by lock 1 (lock must be broken now)
        auto req3 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(3).Build());
        lockRows.ExpectResult(req3, NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN);
    }

} // Y_UNIT_TEST_SUITE(DataShardLockRows)

} // namespace NKikimr
