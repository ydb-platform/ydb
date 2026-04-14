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

        friend std::strong_ordering operator<=>(const TRequestId&, const TRequestId&) = default;
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
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_TRACE);

        InitRoot(server, sender);

        return {runtime, server, sender};
    }

    class TTestPipe {
    public:
        TTestPipe(TTestActorRuntime& runtime, ui64 tabletId, ui32 nodeIndex = 0)
            : Runtime(runtime)
            , TabletId(tabletId)
            , NodeIndex(nodeIndex)
        {
            Reopen();
        }

        ~TTestPipe() {
            Close();
        }

        void Reopen() {
            Close();
            PipeActor = Runtime.ConnectToPipe(TabletId, TActorId(), NodeIndex, NTabletPipe::TClientConfig{});
        }

        void Close() {
            if (PipeActor) {
                Runtime.ClosePipe(PipeActor, TActorId(), NodeIndex);
                PipeActor = {};
            }
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

        auto GetOpenTxs(const TTableId& tableId) {
            TActorId sender = Runtime.AllocateEdgeActor(GetNodeIndex());
            Runtime.SendToPipe(PipeActor, sender, new TEvDataShard::TEvGetOpenTxs(tableId.PathId));
            auto ev = Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetOpenTxsResult>(sender);
            return std::move(ev->Get()->OpenTxs);
        }

    private:
        TTestActorRuntime& Runtime;
        const ui64 TabletId;
        const ui32 NodeIndex;
        TActorId PipeActor;
    };

    class TKeysBuilder {
    public:
        TKeysBuilder(uint32_t cols = 1): Cols(cols)
        { }

        TKeysBuilder&& Add(i32 key) && {
            Cells.push_back(TCell::Make(key));
            return std::move(*this);
        }

        TKeysBuilder&& AddNull() && {
            Cells.emplace_back();
            return std::move(*this);
        }

        TSerializedCellMatrix Build() && {
            return TSerializedCellMatrix(TSerializedCellMatrix::Serialize(Cells, Cells.size()/Cols, Cols));
        }

    private:
        TVector<TCell> Cells;
        const size_t Cols;
    };

    class TLockRowsHelper {
    public:
        TLockRowsHelper(TTestActorRuntime& runtime, TTestPipe& pipe)
            : Runtime(runtime)
            , Pipe(pipe)
            , Sender(Runtime.AllocateEdgeActor(pipe.GetNodeIndex()))
        {}

        using TRequestModifyCallback = std::function<void(NEvents::TDataEvents::TEvLockRows*)>;

        ui64 SendRequest(const TLockHandle& lock, NKikimrDataEvents::ELockMode lockMode, const TTableId& tableId, TSerializedCellMatrix keys, const TRequestModifyCallback& callback = {}) {
            ui64 requestId = NextRequestId++;
            auto req = std::make_unique<NEvents::TDataEvents::TEvLockRows>(requestId);
            req->Record.SetLockId(lock.GetLockId());
            req->Record.SetLockNodeId(lock.GetLockNodeId());
            req->Record.SetLockMode(lockMode);
            req->SetTableId(tableId);
            req->Record.AddColumnIds(1);
            req->Record.SetPayloadFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            req->SetCellMatrix(keys.ReleaseBuffer());
            if (callback) {
                callback(req.get());
            }
            Pipe.Send(Sender, req.release());
            return requestId;
        }

        ui64 SendRequest(const TLockHandle& lock, const TTableId& tableId, TSerializedCellMatrix keys, const TRequestModifyCallback& callback = {}) {
            return SendRequest(lock, NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE, tableId, keys, callback);
        }

        void SendCancel(ui64 requestId) {
            Pipe.Send(Sender, new NEvents::TDataEvents::TEvLockRowsCancel(requestId));
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
        TWaitGraphInterceptor(TTestActorRuntime& runtime, bool suppressEvents = true)
            : Runtime(runtime)
            , AddObserver(Runtime.AddObserver<TEvLongTxService::TEvWaitingLockAdd>(
                [this, suppressEvents](auto& ev) {
                    auto* msg = ev->Get();
                    (*this)[TRequestId{ ev->Sender, msg->RequestId }] = TWaitGraphEdge{ msg->Lock.LockId, msg->OtherLock.LockId };
                    if (suppressEvents) {
                        // Prevent long tx service from processing this event
                        ev.Reset();
                    }
                }))
            , RemoveObserver(Runtime.AddObserver<TEvLongTxService::TEvWaitingLockRemove>(
                [this, suppressEvents](auto& ev) {
                    auto* msg = ev->Get();
                    (*this).erase(TRequestId{ ev->Sender, msg->RequestId });
                    if (suppressEvents) {
                        // Prevent long tx service from processing this event
                        ev.Reset();
                    }
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

        TString ToString(bool withRequestIds = false) const {
            TVector<std::pair<TWaitGraphEdge, TRequestId>> edges;
            for (auto& pr : *this) {
                edges.emplace_back(pr.second, pr.first);
            }
            std::sort(edges.begin(), edges.end());
            TStringBuilder sb;
            for (auto& pr : edges) {
                sb << pr.first.LockId << " -> " << pr.first.OtherLockId;
                if (withRequestIds) {
                    sb << " (" << pr.second.Id << ")";
                }
                sb << "\n";
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

    Y_UNIT_TEST(ExplicitCancellation) {
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
        TLockHandle lock4(456, runtime.GetActorSystem(0));
        TLockHandle lock5(567, runtime.GetActorSystem(0));
        TLockRowsHelper lockRows(runtime, pipe);

        TWaitGraphInterceptor waitGraph(runtime);

        // Lock key 1 by lock 1
        auto req1 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req1);

        // Lock key 1 by lock 2
        auto req2 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        // Lock key 1 by lock 3
        auto req3 = lockRows.SendRequest(lock3, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        // Lock key 1 by lock 4
        auto req4 = lockRows.SendRequest(lock4, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        // Lock key 1 by lock 5
        auto req5 = lockRows.SendRequest(lock5, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        // We must have a chain of 4 awaiters now
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "234 -> 123\n"
            "345 -> 234\n"
            "456 -> 345\n"
            "567 -> 456\n");

        // Cancel the current runtime lock owner, the next one must become the head
        lockRows.SendCancel(req2);
        lockRows.ExpectNoResult();

        // We must have a chain of 3 awaiters now
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "345 -> 123\n"
            "456 -> 345\n"
            "567 -> 456\n");

        // Cancel the middle of the chain
        lockRows.SendCancel(req4);
        lockRows.ExpectNoResult();

        // We must have a chain of 2 awaiters now
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "345 -> 123\n"
            "567 -> 345\n");

        // Cancel the end of the chain
        lockRows.SendCancel(req5);
        lockRows.ExpectNoResult();

        // We must have a simple awaiter now
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "345 -> 123\n");

        // Drop the first lock, then lock 3 must succeed
        lock1.Reset();
        lockRows.ExpectResult(req3);
    }

    Y_UNIT_TEST(ImplicitCancellation) {
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
        TLockRowsHelper lockRows(runtime, pipe);

        TTestPipe pipe2(runtime, shards.at(0));
        TLockRowsHelper lockRows2(runtime, pipe2);

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockHandle lock3(345, runtime.GetActorSystem(0));
        TLockHandle lock4(456, runtime.GetActorSystem(0));
        TLockHandle lock5(567, runtime.GetActorSystem(0));

        TWaitGraphInterceptor waitGraph(runtime);

        // Lock key 1 by lock 1
        auto req1 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req1);

        // Lock key 1 by lock 2
        lockRows2.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        lockRows2.ExpectNoResult();

        // Lock key 1 by lock 3
        lockRows.SendRequest(lock3, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        // Lock key 1 by lock 4
        lockRows2.SendRequest(lock4, tableId, TKeysBuilder().Add(1).Build());
        lockRows2.ExpectNoResult();

        // Lock key 1 by lock 5
        lockRows.SendRequest(lock5, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        // We must have a chain of 4 awaiters now
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "234 -> 123\n"
            "345 -> 234\n"
            "456 -> 345\n"
            "567 -> 456\n");

        // Close the second pipe, it must cancel requests 2 and 4
        pipe2.Close();
        lockRows2.ExpectNoResult();

        // We must have 2 awaiters left
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "345 -> 123\n"
            "567 -> 345\n");

        // Reopen the first pipe, it must cancel requests 3 and 5
        pipe.Reopen();
        lockRows.ExpectNoResult();
        UNIT_ASSERT_VALUES_EQUAL(waitGraph.ToString(), "");

        // The key 1 must still be locked, and lock attempts must be retriable
        lockRows.SendRequest(lock3, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        // We must have an awaiter
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "345 -> 123\n");
    }

    Y_UNIT_TEST(NoSameKeyChainDeadlock) {
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
        TLockRowsHelper lockRows(runtime, pipe);

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockHandle lock3(345, runtime.GetActorSystem(0));

        TWaitGraphInterceptor waitGraph(runtime);

        // Lock key 1 by lock 1
        auto req1 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req1);

        // Lock key 1 by lock 2 (it will start waiting for lock 1)
        auto req2 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        // Lock key 1 by lock 3 (it will start waiting for lock 2)
        auto req3 = lockRows.SendRequest(lock3, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "234 -> 123\n"
            "345 -> 234\n");

        // Lock key 1 by lock 2 again (e.g. a concurrent request)
        // It must jump the queue at the currently waiting lock, and not
        // self-deadlock waiting for lock 3 which is waiting for lock 2.
        auto req4 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        // Lock key 1 by lock 3 again
        // This must be enqueued after req3
        auto req5 = lockRows.SendRequest(lock3, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "234 -> 123\n"
            "345 -> 234\n");

        // Cancel req3, the wait graph must still have the 345 -> 234 edge
        lockRows.SendCancel(req3);
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "234 -> 123\n"
            "345 -> 234\n");

        // Remove lock 1, requests 2 and 4 must succeed in that order
        lock1.Reset();
        lockRows.ExpectResult(req2);
        lockRows.ExpectResult(req4);

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "345 -> 234\n");

        // Finally remove lock 2, it must unblock req5
        lock2.Reset();
        lockRows.ExpectResult(req5);
    }

    Y_UNIT_TEST(RuntimeLockChainRemoval) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));
        TLockRowsHelper lockRows(runtime, pipe);
        TWaitGraphInterceptor waitGraph(runtime);

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockHandle lock3(345, runtime.GetActorSystem(0));

        // Lock key 1 by lock 1, this will be used as a blocker for requests
        auto req1 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req1);

        // Send multiple requests to lock key 1 by lock 2
        auto req2 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        auto req3 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        auto req4 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        // Waiting edge from 234 to 123 should have been added once
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(true),
            "234 -> 123 (2)\n");

        // Lock key 1 by lock 3, it will be further blocked
        auto req5 = lockRows.SendRequest(lock3, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(true),
            "234 -> 123 (2)\n"
            "345 -> 234 (3)\n");

        // Cancel req3 (middle of the chain)
        // We expect wait graph not to change in any way
        lockRows.SendCancel(req3);
        lockRows.ExpectNoResult();
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(true),
            "234 -> 123 (2)\n"
            "345 -> 234 (3)\n");

        // Add a new tail for lock 2 requests
        // We expect wait graph not to change in any way
        auto req6 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(true),
            "234 -> 123 (2)\n"
            "345 -> 234 (3)\n");

        // Cancel req2 (current head of the chain)
        // Since req4 has its predecessor changed it will wake up and add a replacement 234 -> 123 edge
        lockRows.SendCancel(req2);
        lockRows.ExpectNoResult();
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(true),
            "234 -> 123 (4)\n"
            "345 -> 234 (3)\n");

        // Cancel req6 (current tail of the chain)
        // We expect wait graph not to change since req5 predecessor didn't change
        lockRows.SendCancel(req6);
        lockRows.ExpectNoResult();
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(true),
            "234 -> 123 (4)\n"
            "345 -> 234 (3)\n");

        // Finally cancel req4
        // We expect req5 to wake up and change wait edge to 345 -> 123
        lockRows.SendCancel(req4);
        lockRows.ExpectNoResult();
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(true),
            "345 -> 123 (5)\n");

        Y_UNUSED(req5);
    }

    Y_UNIT_TEST(BadRequests) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));
        TLockRowsHelper lockRows(runtime, pipe);

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));

        TWaitGraphInterceptor waitGraph(runtime);

        // Lock key 1 by lock 1, this will be used as a blocker for requests
        auto req1 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Build());
        auto res1 = lockRows.ExpectResult(req1);

        // Lock key 1 by lock2, this will be enqueued as an awaiter
        auto req2 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "234 -> 123\n");

        // Try using duplicate request id, request must fail once, along with the original
        lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build(), [&](auto* req) {
            req->Record.SetRequestId(req2);
        });
        lockRows.ExpectResult(req2, NKikimrDataEvents::TEvLockRowsResult::STATUS_BAD_REQUEST);
        lockRows.ExpectNoResult();
        UNIT_ASSERT_VALUES_EQUAL(waitGraph.ToString(), "");

        // Using optimistic lock mode is unsupported
        ui64 req4 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build(), [&](auto* req) {
            req->Record.SetLockMode(NKikimrDataEvents::OPTIMISTIC);
        });
        lockRows.ExpectResult(req4, NKikimrDataEvents::TEvLockRowsResult::STATUS_BAD_REQUEST);

        // Alter table, adding a column
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                ALTER TABLE `/Root/table` ADD COLUMN value2 int;
            )"),
            "SUCCESS");

        // Using an outdated tableId should fail
        ui64 req5 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req5, NKikimrDataEvents::TEvLockRowsResult::STATUS_SCHEME_CHANGED);

        // Using an updated tableId should succeed
        // Note that alter has broken lock 1 and key will be locked by lock 2
        tableId = ResolveTableId(server, sender, "/Root/table");
        ui64 req6 = lockRows.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req6);
        UNIT_ASSERT_VALUES_EQUAL(waitGraph.ToString(), "");

        // Try locking key 1 by lock 1, specifying a previously returned existing locks
        ui64 req7 = lockRows.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Build(), [&](auto* req) {
            *req->Record.MutableExistingLocks() = res1->Record.GetLocks();
        });
        lockRows.ExpectResult(req7, NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN);
    }

    Y_UNIT_TEST(SharedLockThenUpgradeToExclusive) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));
        TLockRowsHelper lockRows(runtime, pipe);

        TLockHandle lock1(123, runtime.GetActorSystem(0));

        TWaitGraphInterceptor waitGraph(runtime);

        // Lock key 1 by lock 1 in shared mode
        auto req1 = lockRows.SendRequest(lock1, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req1);

        // Lock key 1 by lock 1 in exclusive mode
        auto req2 = lockRows.SendRequest(lock1, NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req2);
    }

    Y_UNIT_TEST(MultipleSharedLocksThenUpgradeToExclusive) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));
        TLockRowsHelper lockRows(runtime, pipe);

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockHandle lock3(345, runtime.GetActorSystem(0));

        TWaitGraphInterceptor waitGraph(runtime);

        // Lock key 1 by lock 1 in shared mode
        auto req1 = lockRows.SendRequest(lock1, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req1);

        // Lock key 1 by lock 2 in shared mode
        auto req2 = lockRows.SendRequest(lock2, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req2);

        // Lock key 1 by lock 3 in shared mode
        auto req3 = lockRows.SendRequest(lock3, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req3);

        // Lock key 1 by lock 1 in exclusive mode
        auto req4 = lockRows.SendRequest(lock1, NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "123 -> 345\n");

        // Remove lock 3, req4 should switch to waiting for lock 2
        lock3.Reset();
        lockRows.ExpectNoResult();
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "123 -> 234\n");

        // Remove lock 2, req4 should succeed and upgrade shared lock to exclusive
        lock2.Reset();
        lockRows.ExpectResult(req4);
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "");
    }

    Y_UNIT_TEST(MultipleLockModes) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));
        TLockRowsHelper lockRows(runtime, pipe);

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockHandle lock3(345, runtime.GetActorSystem(0));

        TWaitGraphInterceptor waitGraph(runtime);

        // Lock key 1 by lock 1 in key shared mode
        auto req1 = lockRows.SendRequest(lock1, NKikimrDataEvents::PESSIMISTIC_SHARED_KEY, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req1);

        // Lock key 1 by lock 2 in no key exclusive mode
        auto req2 = lockRows.SendRequest(lock2, NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE_NO_KEY, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req2);

        // Lock key 1 by lock 3 in exclusive mode
        auto req3 = lockRows.SendRequest(lock3, NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectNoResult();
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "345 -> 234\n");

        // Remove lock 2, req3 should switch to waiting for lock 1
        lock2.Reset();
        lockRows.ExpectNoResult();
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "345 -> 123\n");

        // Remove lock 1, req3 should successfully lock key 1
        lock1.Reset();
        lockRows.ExpectResult(req3);

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "");
    }

    Y_UNIT_TEST(DistributedDeadlock) {
        TPortManager pm;

        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false).SetNodeCount(4);
        auto [runtime, server, sender] = TestCreateServer(pm, serverSettings);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key))
                WITH (PARTITION_AT_KEYS = (10));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        THashSet<ui32> shardNodeIds;
        for (ui64 shard : shards) {
            ui32 nodeId = ResolveTablet(runtime, shard).NodeId();
            Cerr << "shard: " << shard << " nodeId: " << nodeId << Endl;
            shardNodeIds.insert(nodeId);
        }

        TTestPipe pipe1(runtime, shards.at(0));
        TLockRowsHelper lockRows1(runtime, pipe1);
        TTestPipe pipe2(runtime, shards.at(1));
        TLockRowsHelper lockRows2(runtime, pipe2);

        // Choose lock nodes that are distinct from nodes with table shards.
        TVector<size_t> lockNodeIdxs;
        for (size_t i = 0; i < runtime.GetNodeCount(); ++i) {
            if (!shardNodeIds.contains(runtime.GetNodeId(i))) {
                lockNodeIdxs.push_back(i);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(lockNodeIdxs.size(), 2u);

        TLockHandle lock1(123, runtime.GetActorSystem(lockNodeIdxs[0]), TInstant::Seconds(123456));
        TLockHandle lock2(234, runtime.GetActorSystem(lockNodeIdxs[1]), TInstant::Seconds(234567));

        TWaitGraphInterceptor waitGraph(runtime, /*suppressEvents=*/false);

        // Lock key 1 by lock 1
        auto req1 = lockRows1.SendRequest(lock1, tableId, TKeysBuilder().Add(1).Build());
        lockRows1.ExpectResult(req1);

        // Lock key 11 by lock 2
        auto req2 = lockRows2.SendRequest(lock2, tableId, TKeysBuilder().Add(11).Build());
        lockRows2.ExpectResult(req2);

        // Try locking key 1 by lock 2
        auto req3 = lockRows1.SendRequest(lock2, tableId, TKeysBuilder().Add(1).Build());
        lockRows1.ExpectNoResult();

        // Try locking key 11 by lock 1
        auto req4 = lockRows2.SendRequest(lock1, tableId, TKeysBuilder().Add(11).Build());
        lockRows2.ExpectNoResult();

        // LongTxService should break the deadlock, aborting the request belonging to the youngest lock.
        lockRows1.ExpectResult(req3, NKikimrDataEvents::TEvLockRowsResult::STATUS_DEADLOCK);
        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "123 -> 234\n");

        runtime.SimulateSleep(TDuration::Seconds(1));
        for (size_t nodeIdx = 0; nodeIdx < runtime.GetNodeCount(); ++nodeIdx) {
            // Check that the wait edges are instantiated in the LongTxService on all nodes.
            ui32 nodeId = runtime.GetNodeId(nodeIdx);
            auto sender = runtime.AllocateEdgeActor(nodeIdx);
            runtime.Send(
                MakeLongTxServiceID(nodeId), sender,
                new TEvLongTxService::TEvGetLockWaitGraph, nodeIdx, true);
            auto result = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvGetLockWaitGraphResult>(sender);

            TVector<std::pair<ui64, ui64>> edges;
            for (const auto& edge : result->Get()->WaitEdges) {
                edges.emplace_back(edge.Awaiter.LockId, edge.Blocker.LockId);
            }
            std::sort(edges.begin(), edges.end());
            TStringBuilder wgStr;
            for (auto& [a, b] : edges) {
                wgStr << a << " -> " << b << "\n";
            }
            UNIT_ASSERT_VALUES_EQUAL_C(
                wgStr,
                "123 -> 234\n",
                "with nodeIdx: " << nodeIdx);
        }

        // Perhaps unexpectedly row 11 is still locked until the lock is broken or removed
        lockRows2.ExpectNoResult();

        // Reset the second lock (it will notify the long tx service and rollback datashard changes)
        lock2.Reset();

        // Finally req4 should succeed
        lockRows2.ExpectResult(req4);

        UNIT_ASSERT_VALUES_EQUAL(
            waitGraph.ToString(),
            "");
    }

    Y_UNIT_TEST(MultipleSharedLocksCleanupBottomUp) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));
        TLockRowsHelper lockRows(runtime, pipe);

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockHandle lock3(345, runtime.GetActorSystem(0));

        TWaitGraphInterceptor waitGraph(runtime);

        // Lock key 1 by lock 1 in shared mode
        auto req1 = lockRows.SendRequest(lock1, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req1);

        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 1u);

        // Lock key 1 by lock 2 in shared mode
        auto req2 = lockRows.SendRequest(lock2, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req2);

        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 2u);

        // Lock key 1 by lock 3 in shared mode
        auto req3 = lockRows.SendRequest(lock3, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req3);

        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 3u);

        // Removing lock 1 should rollback its changes, making MultiTxId for locks [1..2] redundant which is also removed
        lock1.Reset();
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 1u);

        // Removing lock 2 should cause MultiTxId for locks [2..3] to become an alias for lock 3
        lock2.Reset();
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 1u);

        // Removing lock 3 should cause the last MultiTxId cleanup, leaving none
        lock3.Reset();
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 0u);
    }

    Y_UNIT_TEST(MultipleSharedLocksCleanupTopDown) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));
        TLockRowsHelper lockRows(runtime, pipe);

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockHandle lock3(345, runtime.GetActorSystem(0));
        TLockHandle lock4(456, runtime.GetActorSystem(0));

        TWaitGraphInterceptor waitGraph(runtime);

        // Lock key 1 by lock 1 in shared mode
        auto req1 = lockRows.SendRequest(lock1, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req1);

        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 1u);

        // Lock key 1 by lock 2 in shared mode
        auto req2 = lockRows.SendRequest(lock2, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req2);

        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 2u);

        // Lock key 1 by lock 3 in shared mode
        auto req3 = lockRows.SendRequest(lock3, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req3);

        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 3u);

        // Lock key 1 by lock 4 in shared mode
        auto req4 = lockRows.SendRequest(lock4, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(1).Build());
        lockRows.ExpectResult(req4);

        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 4u);

        // Removing lock 4 shouldn't change anything, because MultiTxId for locks [1..4] has non-zero locked rows
        lock4.Reset();
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 4u);

        // Removing lock 3 should cleanup MultiTxId for locks [1..3] and relink MultiTxId [1..4] to [1..2]
        lock3.Reset();
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 3u);

        // Removing lock 2 makes MultiTxId [1..2] to be a redundant alias for lock 1, which is removed
        lock2.Reset();
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 2u);

        // Finally removing lock 1 will cause a lock 1 rollback and removal of corresponding MultiTxId
        lock1.Reset();
        lockRows.ExpectNoResult();

        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 0u);
    }

    Y_UNIT_TEST(MultipleSharedLocksLayers) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        auto tableId = ResolveTableId(server, sender, "/Root/table");
        const auto shards = GetTableShards(server, sender, "/Root/table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));
        TLockRowsHelper lockRows(runtime, pipe);

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockHandle lock3(345, runtime.GetActorSystem(0));
        TLockHandle lock4(456, runtime.GetActorSystem(0));

        TWaitGraphInterceptor waitGraph(runtime);

        // Lock keys 1..4 by lock 1 in shared mode
        auto req1 = lockRows.SendRequest(lock1, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(1).Add(2).Add(3).Add(4).Build());
        lockRows.ExpectResult(req1);

        // Uncommitted:
        // * lock 1
        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 1u);

        // Lock keys 2..5 by lock 2 in shared mode
        auto req2 = lockRows.SendRequest(lock2, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(2).Add(3).Add(4).Add(5).Build());
        lockRows.ExpectResult(req2);

        // Uncommitted:
        // * lock 1
        // * lock 2
        // * multi[1, 2]
        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 3u);

        // Lock keys 3..6 by lock 3 in shared mode
        auto req3 = lockRows.SendRequest(lock3, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(3).Add(4).Add(5).Add(6).Build());
        lockRows.ExpectResult(req3);

        // Uncommitted:
        // * lock 1
        // * lock 2
        // * lock 3
        // * multi[1, 2]
        // * multi[multi[1, 2], 3]
        // * multi[2, 3]
        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 6u);

        // Lock keys 4..7 by lock 4 in shared mode
        auto req4 = lockRows.SendRequest(lock4, NKikimrDataEvents::PESSIMISTIC_SHARED, tableId, TKeysBuilder().Add(4).Add(5).Add(6).Add(7).Build());
        lockRows.ExpectResult(req4);

        // Uncommitted:
        // * lock 1
        // * lock 2
        // * lock 3
        // * lock 4
        // * multi[1, 2]
        // * multi[multi[1, 2], 3]
        // * multi[multi[multi[1, 2], 3], 4]
        // * multi[2, 3] (LockedRowsCount == 0)
        // * multi[multi[2, 3], 4]
        // * multi[3, 4]
        // Rows and corresponding locks:
        // * key1: lock 1
        // * key2: multi[1..2]
        // * key3: multi[1..3]
        // * key4: multi[1..4]
        // * key5: multi[2..4]
        // * key6: multi[3..4]
        // * key7: lock 4
        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 10u);

        // Removing lock 1 will not remove any MultiTxIds, because all of them are used by at least 1 row
        lock1.Reset();
        lockRows.ExpectNoResult();

        // Uncommitted:
        // * lock 2
        // * lock 3
        // * lock 4
        // * multi[2]
        // * multi[multi[2], 3]
        // * multi[multi[multi[2], 3], 4]
        // * multi[2, 3] (LockedRowsCount == 0)
        // * multi[multi[2, 3], 4]
        // * multi[3, 4]
        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 9u);

        // Removing lock 2 will also remove MultiTxId which used to combine lock 1 and 2 and collapse multi[2, 3]
        lock2.Reset();
        lockRows.ExpectNoResult();

        // Uncommitted:
        // * lock 3
        // * lock 4
        // * multi[3]
        // * multi[multi[3], 4]
        // * multi[3, 4]
        // * multi[3, 4]
        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 6u);

        // Removing lock 3 will also remove MultiTxIds which only had lock 3 left
        lock3.Reset();
        lockRows.ExpectNoResult();

        // Uncommitted:
        // * lock 4
        // * multi[4]
        // * multi[4]
        // * multi[4]
        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 4u);

        // Removing lock 4 will cleanup all remaining locks
        lock4.Reset();
        lockRows.ExpectNoResult();

        // Uncommitted: none left
        UNIT_ASSERT_VALUES_EQUAL(pipe.GetOpenTxs(tableId).size(), 0u);
    }

    Y_UNIT_TEST_TWIN(UniqueIndexBasic, OnAdd) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetNodeCount(1)
            .SetUseRealThreads(false);
        serverSettings.FeatureFlags.SetEnableAddUniqueIndex(true);
        serverSettings.FeatureFlags.SetEnableOnlineAddUniqueIndex(true);
        auto [runtime, server, sender] = TestCreateServer(pm, serverSettings);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_TRACE);

        if (OnAdd) {
            UNIT_ASSERT_VALUES_EQUAL(
                KqpSchemeExec(runtime, R"(
                    CREATE TABLE `/Root/table` (uniq int, pk int, PRIMARY KEY (uniq, pk));
                )"),
                "SUCCESS");
            UNIT_ASSERT_VALUES_EQUAL(
                KqpSchemeExec(runtime, R"(
                    ALTER TABLE `/Root/table` ADD INDEX idx GLOBAL UNIQUE ON (uniq);
                )", "/Root"),
                "SUCCESS");
        } else {
            UNIT_ASSERT_VALUES_EQUAL(
                KqpSchemeExec(runtime, R"(
                    CREATE TABLE `/Root/table` (uniq int, pk int, PRIMARY KEY (uniq, pk), INDEX idx GLOBAL UNIQUE ON (uniq));
                )"),
                "SUCCESS");
        }

        const auto tableId = ResolveTableId(server, sender, "/Root/table/idx/indexImplTable");
        const auto shards = GetTableShards(server, sender, "/Root/table/idx/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockRowsHelper lockRows(runtime, pipe);

        auto sendRequestWithUniq = [&](const TLockHandle& lock, TSerializedCellMatrix keys) {
            return lockRows.SendRequest(lock, tableId, keys, [&](auto* req) {
                req->Record.AddColumnIds(2);
            });
        };

        // Unique indexes should have UniqueIndexKeySize = number of unique key columns

        // Lock (1, 1) by lock 1
        auto req1 = sendRequestWithUniq(lock1, TKeysBuilder(2).Add(1).Add(1).Build());
        lockRows.ExpectResult(req1);

        // Try locking (1, 2) by lock 2 - should conflict
        auto req2 = sendRequestWithUniq(lock2, TKeysBuilder(2).Add(1).Add(2).Build());
        lockRows.ExpectNoResult();

        // Try locking (1, 2) by lock 1 - should be OK, the same lock is already taken
        auto req3 = sendRequestWithUniq(lock1, TKeysBuilder(2).Add(1).Add(1).Build());
        lockRows.ExpectResult(req3);

        // Try waiting for lock 2 some more
        lockRows.ExpectNoResult();

        // Destroy lock 1 handle, it must rollback all changes
        lock1.Reset();

        // Try waiting for lock 2 result now
        lockRows.ExpectResult(req2);
    }

    Y_UNIT_TEST(UniqueIndexNulls) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (uniq int, pk int, PRIMARY KEY (uniq, pk), INDEX idx GLOBAL UNIQUE ON (uniq));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table/idx/indexImplTable");
        const auto shards = GetTableShards(server, sender, "/Root/table/idx/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockHandle lock2(234, runtime.GetActorSystem(0));
        TLockRowsHelper lockRows(runtime, pipe);

        auto sendRequestWithUniq = [&](const TLockHandle& lock, TSerializedCellMatrix keys) {
            return lockRows.SendRequest(lock, tableId, keys, [&](auto* req) {
                req->Record.AddColumnIds(2);
            });
        };

        // Lock (null, 1) by lock 1
        auto req1 = sendRequestWithUniq(lock1, TKeysBuilder(2).AddNull().Add(1).Build());
        lockRows.ExpectResult(req1);

        // Try locking (null, 2) by lock 2 - should not conflict
        auto req2 = sendRequestWithUniq(lock2, TKeysBuilder(2).AddNull().Add(2).Build());
        lockRows.ExpectResult(req2);
    }

    Y_UNIT_TEST(UniqueIndexOverUncommittedThenCommit) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (uniq int, pk int, PRIMARY KEY (uniq, pk), INDEX idx GLOBAL UNIQUE ON (uniq));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table/idx/indexImplTable");
        const auto shards = GetTableShards(server, sender, "/Root/table/idx/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockRowsHelper lockRows(runtime, pipe);

        auto sendRequestWithUniq = [&](const TLockHandle& lock, TSerializedCellMatrix keys) {
            return lockRows.SendRequest(lock, tableId, keys, [&](auto* req) {
                req->Record.AddColumnIds(2);
            });
        };

        // Write an uncommitted row (1, 101)
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                UPSERT INTO `/Root/table` (uniq, pk) VALUES (1, 101);
                SELECT uniq, pk FROM `/Root/table` WHERE uniq = 1;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 101 } }");

        // Lock (1, 1) by lock 1
        auto req1 = sendRequestWithUniq(lock1, TKeysBuilder(2).Add(1).Add(1).Build());
        lockRows.ExpectResult(req1);

        // Commit the previous transaction
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, R"(SELECT 1)"),
            "{ items { int32_value: 1 } }");

        // Lock (2, 1) by lock 1 (lock must be broken now)
        auto req2 = sendRequestWithUniq(lock1, TKeysBuilder(2).Add(2).Add(1).Build());
        lockRows.ExpectResult(req2, NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN);
    }

    Y_UNIT_TEST(UniqueIndexUncommittedOverLock) {
        TPortManager pm;

        auto [runtime, server, sender] = TestCreateServer(pm);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (uniq int, pk int, PRIMARY KEY (uniq, pk), INDEX idx GLOBAL UNIQUE ON (uniq));
            )"),
            "SUCCESS");

        const auto tableId = ResolveTableId(server, sender, "/Root/table/idx/indexImplTable");
        const auto shards = GetTableShards(server, sender, "/Root/table/idx/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        TTestPipe pipe(runtime, shards.at(0));

        TLockHandle lock1(123, runtime.GetActorSystem(0));
        TLockRowsHelper lockRows(runtime, pipe);

        auto sendRequestWithUniq = [&](const TLockHandle& lock, TSerializedCellMatrix keys) {
            return lockRows.SendRequest(lock, tableId, keys, [&](auto* req) {
                req->Record.AddColumnIds(2);
            });
        };

        // Lock (1, 1) by lock 1
        auto req1 = sendRequestWithUniq(lock1, TKeysBuilder(2).Add(1).Add(1).Build());
        lockRows.ExpectResult(req1);

        // Write an uncommitted row (1, 101)
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                UPSERT INTO `/Root/table` (uniq, pk) VALUES (1, 101);
                SELECT uniq, pk FROM `/Root/table` WHERE uniq = 1;
            )"),
            "{ items { int32_value: 1 } items { int32_value: 101 } }");

        // Lock (2, 1) by lock 1 (lock not broken yet)
        auto req2 = sendRequestWithUniq(lock1, TKeysBuilder(2).Add(2).Add(1).Build());
        lockRows.ExpectResult(req2);

        // Commit the previous transaction
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, R"(SELECT 1)"),
            "{ items { int32_value: 1 } }");

        // Lock (3, 1) by lock 1 (lock must be broken now)
        auto req3 = sendRequestWithUniq(lock1, TKeysBuilder(2).Add(3).Add(1).Build());
        lockRows.ExpectResult(req3, NKikimrDataEvents::TEvLockRowsResult::STATUS_LOCKS_BROKEN);
    }

} // Y_UNIT_TEST_SUITE(DataShardLockRows)

} // namespace NKikimr
