#include "long_tx_service.h"

#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/tx/scheme_board/cache.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>

#include <ydb/library/actors/interconnect/interconnect_impl.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NLongTxService {

    namespace {

        TTenantTestConfig::TTenantPoolConfig MakeDefaultTenantPoolConfig() {
            TTenantTestConfig::TTenantPoolConfig res = {
                // Static slots {tenant, {cpu, memory, network}}
                {
                    { {DOMAIN1_NAME, {1, 1, 1}} },
                },
                // NodeType
                "storage"
            };
            return res;
        }

        TTenantTestConfig MakeTenantTestConfig(bool fakeSchemeShard, ui32 nodeCount = 2) {
            TTenantTestConfig res = {
                // Domains {name, schemeshard {{ subdomain_names }}}
                {{{DOMAIN1_NAME, SCHEME_SHARD1_ID, TVector<TString>()}}},
                // HiveId
                HIVE_ID,
                // FakeTenantSlotBroker
                true,
                // FakeSchemeShard
                fakeSchemeShard,
                // CreateConsole
                false,
                // Nodes {tenant_pool_config}
                {},
                // DataCenterCount
                1
            };
            for (ui32 i = 0; i < nodeCount; ++i) {
                res.Nodes.push_back({MakeDefaultTenantPoolConfig()});
            }
            return res;
        }

        void StartSchemeCache(TTestActorRuntime& runtime, const TString& root = DOMAIN1_NAME) {
            for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
                auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>();
                cacheConfig->Roots.emplace_back(1, SCHEME_SHARD1_ID, root);
                cacheConfig->Counters = new ::NMonitoring::TDynamicCounters();

                IActor* schemeCache = CreateSchemeBoardSchemeCache(cacheConfig.Get());
                TActorId schemeCacheId = runtime.Register(schemeCache, nodeIndex);
                runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId, nodeIndex);
            }
        }

        void SimulateSleep(TTestActorRuntime& runtime, TDuration duration) {
            auto sender = runtime.AllocateEdgeActor();
            runtime.Schedule(new IEventHandle(sender, sender, new TEvents::TEvWakeup()), duration);
            runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(sender);
        }

    }

Y_UNIT_TEST_SUITE(LongTxService) {

    Y_UNIT_TEST(BasicTransactions) {
        TTenantTestRuntime runtime(MakeTenantTestConfig(true));
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_DEBUG);

        auto sender1 = runtime.AllocateEdgeActor(0);
        auto service1 = MakeLongTxServiceID(runtime.GetNodeId(0));
        auto sender2 = runtime.AllocateEdgeActor(1);
        auto service2 = MakeLongTxServiceID(runtime.GetNodeId(1));

        TLongTxId txId;

        // Begin a new transaction at node 1
        {
            runtime.Send(
                new IEventHandle(service1, sender1,
                    new TEvLongTxService::TEvBeginTx("/dc-1",
                        NKikimrLongTxService::TEvBeginTx::MODE_WRITE_ONLY)),
                0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvBeginTxResult>(sender1);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLongTxId().GetNodeId(), runtime.GetNodeId(0));
            txId = msg->GetLongTxId();
        }

        // Issue an empty attach message at node 2
        {
            runtime.Send(
                new IEventHandle(service2, sender2,
                    new TEvLongTxService::TEvAttachColumnShardWrites(txId)),
                1, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvAttachColumnShardWritesResult>(sender2);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
        }

        // Commit this transaction at node 2
        {
            runtime.Send(
                new IEventHandle(service2, sender2,
                    new TEvLongTxService::TEvCommitTx(txId)),
                1, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvCommitTxResult>(sender2);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
        }

        // Rollback this transaction at node 2
        {
            runtime.Send(
                new IEventHandle(service2, sender2,
                    new TEvLongTxService::TEvRollbackTx(txId)),
                1, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvRollbackTxResult>(sender2);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), Ydb::StatusIds::BAD_SESSION);
        }

        auto observer = [&](auto& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvLongTxService::TEvRollbackTxResult::EventType: {
                    ui32 node1 = ev->Sender.NodeId();
                    ui32 node2 = ev->Recipient.NodeId();
                    if (node1 != node2) {
                        auto proxy = runtime.GetInterconnectProxy(0, 1);
                        runtime.Send(
                            new IEventHandle(proxy, {}, new TEvInterconnect::TEvDisconnect()),
                            0, true);
                        return TTestBasicRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestBasicRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observer);

        // Rollback this transaction at node 2
        {
            runtime.Send(
                new IEventHandle(service2, sender2,
                    new TEvLongTxService::TEvRollbackTx(txId)),
                1, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvRollbackTxResult>(sender2);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), Ydb::StatusIds::UNDETERMINED);
        }

        // Change txId to a non-existant node and try to commit
        {
            auto badTxId = txId;
            badTxId.NodeId = runtime.GetNodeId(1) + 1;
            runtime.Send(
                new IEventHandle(service2, sender2,
                    new TEvLongTxService::TEvCommitTx(badTxId)),
                1, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvCommitTxResult>(sender2);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), Ydb::StatusIds::UNAVAILABLE);
        }
    }

    Y_UNIT_TEST(AcquireSnapshot) {
        TTenantTestRuntime runtime(MakeTenantTestConfig(false));
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_DEBUG);
        StartSchemeCache(runtime);

        auto sender1 = runtime.AllocateEdgeActor(0);
        auto service1 = MakeLongTxServiceID(runtime.GetNodeId(0));

        // Sleep a little, so there's at least one plan step generated
        SimulateSleep(runtime, TDuration::Seconds(1));

        // Send an acquire read snapshot for node 1
        {
            runtime.Send(
                new IEventHandle(service1, sender1,
                    new TEvLongTxService::TEvAcquireReadSnapshot("/dc-1")),
                0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvAcquireReadSnapshotResult>(sender1);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
        }

        // Begin a new read-only transaction at node 1
        {
            runtime.Send(
                new IEventHandle(service1, sender1,
                    new TEvLongTxService::TEvBeginTx("/dc-1",
                        NKikimrLongTxService::TEvBeginTx::MODE_READ_ONLY)),
                0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvBeginTxResult>(sender1);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
            auto txId = msg->GetLongTxId();
            UNIT_ASSERT_VALUES_EQUAL(txId.NodeId, 0);
            UNIT_ASSERT_C(txId.Snapshot > TRowVersion::Min(), "Unexpected snapshot @ " << txId.Snapshot);
        }

        // Begin a new read-write transaction at node 1
        {
            runtime.Send(
                new IEventHandle(service1, sender1,
                    new TEvLongTxService::TEvBeginTx("/dc-1",
                        NKikimrLongTxService::TEvBeginTx::MODE_READ_WRITE)),
                0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvBeginTxResult>(sender1);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
            auto txId = msg->GetLongTxId();
            UNIT_ASSERT_VALUES_EQUAL(txId.NodeId, runtime.GetNodeId(0));
            UNIT_ASSERT_C(txId.Snapshot > TRowVersion::Min(), "Unexpected snapshot @ " << txId.Snapshot);
        }
    }

    Y_UNIT_TEST(LockSubscribe) {
        TTenantTestRuntime runtime(MakeTenantTestConfig(true));
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_DEBUG);

        TInstant lockTimestamp = TInstant::MicroSeconds(123456789);
        TLockHandle handle(123, runtime.GetActorSystem(0), lockTimestamp);

        auto node1 = runtime.GetNodeId(0);
        auto sender1 = runtime.AllocateEdgeActor(0);
        auto service1 = MakeLongTxServiceID(node1);
        auto node2 = runtime.GetNodeId(1);
        auto sender2 = runtime.AllocateEdgeActor(1);
        auto service2 = MakeLongTxServiceID(node2);

        {
            runtime.Send(
                new IEventHandle(service1, sender1,
                    new TEvLongTxService::TEvSubscribeLock(987, node1)),
                0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvLockStatus>(sender1);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLockId(), 987u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLockNode(), node1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND);
        }

        {
            runtime.Send(
                new IEventHandle(service2, sender2,
                    new TEvLongTxService::TEvSubscribeLock(987, node1)),
                1, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvLockStatus>(sender2);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLockId(), 987u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLockNode(),node1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND);
        }

        {
            runtime.Send(
                new IEventHandle(service1, sender1,
                    new TEvLongTxService::TEvSubscribeLock(123, node1)),
                0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvLockStatus>(sender1);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLockId(), 123u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLockNode(), node1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), NKikimrLongTxService::TEvLockStatus::STATUS_SUBSCRIBED);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetLockTimestamp(), lockTimestamp);
        }

        {
            runtime.Send(
                new IEventHandle(service2, sender2,
                    new TEvLongTxService::TEvSubscribeLock(123, node1)),
                1, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvLockStatus>(sender2);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLockId(), 123u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLockNode(), node1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), NKikimrLongTxService::TEvLockStatus::STATUS_SUBSCRIBED);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetLockTimestamp(), lockTimestamp);
        }

        {
            // move lock handle out, so it unregisters itself
            auto movedOut = std::move(handle);
        }

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvLockStatus>(sender1);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLockId(), 123u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLockNode(), node1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND);
        }

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvLockStatus>(sender2);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLockId(), 123u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLockNode(), node1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND);
        }

        // Block all cross-node TEvSubscribeLock messages and disconnect instead
        size_t disconnectCount = 0;
        auto observer = [&](auto& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvLongTxService::TEvSubscribeLock::EventType: {
                    ui32 node1 = ev->Sender.NodeId();
                    ui32 node2 = ev->Recipient.NodeId();
                    if (node1 != node2) {
                        ++disconnectCount;
                        auto proxy = runtime.GetInterconnectProxy(0, 1);
                        runtime.Send(
                            new IEventHandle(proxy, {}, new TEvInterconnect::TEvDisconnect()),
                            0, true);
                        // Advance time on each disconnect, so timeout happens faster
                        runtime.AdvanceCurrentTime(TDuration::Seconds(5));
                        return TTestBasicRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestBasicRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observer);

        // Try to subscribe to the lock of a disconnecting node
        // We should eventually get an unavailable result
        {
            runtime.Send(
                new IEventHandle(service2, sender2,
                    new TEvLongTxService::TEvSubscribeLock(234, node1)),
                1, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvLockStatus>(sender2);
            const auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLockId(), 234u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLockNode(), node1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), NKikimrLongTxService::TEvLockStatus::STATUS_UNAVAILABLE);
        }

        // We expect multiple disconnects before unavailable result is returned
        UNIT_ASSERT_GE(disconnectCount, 3);
    }

} // Y_UNIT_TEST_SUITE(LongTxService)

Y_UNIT_TEST_SUITE(LockWaitGraph) {

    namespace {

    struct TLockHolder {
        TLockHandle Handle;
        TLockInfo Info;

        explicit TLockHolder(TLockHandle handle)
            : Handle(std::move(handle))
            , Info(Handle.GetLockId(), Handle.GetLockNodeId())
        {}
    };

    TString GetWaitGraphString(TTestActorRuntime& runtime, size_t nodeIdx) {
        ui32 nodeId = runtime.GetNodeId(nodeIdx);
        auto sender = runtime.AllocateEdgeActor(nodeIdx);
        runtime.Send(
            MakeLongTxServiceID(nodeId),
            sender,
            new TEvLongTxService::TEvGetLockWaitGraph,
            nodeIdx);
        auto result = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvGetLockWaitGraphResult>(sender);
        TVector<std::pair<ui64, ui64>> edges;
        for (const auto& edge : result->Get()->WaitEdges) {
            edges.emplace_back(edge.Awaiter.LockId, edge.Blocker.LockId);
        }
        std::sort(edges.begin(), edges.end());

        TStringBuilder sb;
        for (auto& [a, b] : edges) {
            sb << a << " -> " << b << "\n";
        }
        return sb;
    }

    class TMockDatashard {
    public:
        TMockDatashard(TTestActorRuntime& runtime, size_t nodeIdx)
            : Runtime(runtime)
            , NodeIdx(nodeIdx)
            , ActorId(Runtime.AllocateEdgeActor(nodeIdx))
        {}

        void AddLock(const TLockInfo& lock) {
            const bool inserted = WaitGraph.try_emplace(lock).second;
            if (inserted) {
                SendToLongTx(new TEvLongTxService::TEvSubscribeLock(lock.LockId, lock.LockNodeId));

                auto responseEv = Runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvLockStatus>(ActorId);
                auto& response = responseEv->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(response.GetLockId(), lock.LockId);
                UNIT_ASSERT_VALUES_EQUAL(response.GetLockNode(), lock.LockNodeId);
                UNIT_ASSERT_VALUES_EQUAL(
                    response.GetStatus(), NKikimrLongTxService::TEvLockStatus::STATUS_SUBSCRIBED);
            }
        }

        // Returns request id.
        ui64 AddWait(const TLockInfo& awaiter, const TLockInfo& blocker) {
            Y_ENSURE(WaitGraph.contains(awaiter));
            Y_ENSURE(WaitGraph.contains(blocker));

            const ui64 reqId = ++LastRequestId;
            Y_ENSURE(WaitGraph[awaiter].emplace(reqId, blocker).second);
            SendToLongTx(new TEvLongTxService::TEvWaitingLockAdd(reqId, awaiter, blocker));
            return reqId;
        }

        void RemoveWait(const TLockInfo& awaiter, ui64 reqId) {
            Y_ENSURE(WaitGraph.contains(awaiter));
            auto& edges = WaitGraph[awaiter];
            Y_ENSURE(edges.contains(reqId));
            edges.erase(reqId);
            SendToLongTx(new TEvLongTxService::TEvWaitingLockRemove(reqId));
        }

        ~TMockDatashard() {
            for (const auto& [lock, edges] : WaitGraph) {
                for (const auto& [reqId, blocker] : edges) {
                    SendToLongTx(new TEvLongTxService::TEvWaitingLockRemove(reqId));
                }
            }
        }

    private:
        void SendToLongTx(TAutoPtr<IEventBase> ev) {
            Runtime.Send(MakeLongTxServiceID(Runtime.GetNodeId(NodeIdx)), ActorId, ev, NodeIdx, true);
        }

    private:
        struct TLockInfoHash {
            ui64 operator()(const TLockInfo& li) const {
                return CombineHashes(std::hash<ui64>{}(li.LockId), std::hash<ui32>{}(li.LockNodeId));
            }
        };

        TTestActorRuntime& Runtime;
        const size_t NodeIdx;
        TActorId ActorId;
        THashMap<TLockInfo, THashMap<ui64, TLockInfo>, TLockInfoHash> WaitGraph;
        ui64 LastRequestId = 0;
    };

    }

    // Test simple wait graph updates when locks and wait edges are added/removed.
    Y_UNIT_TEST(SimpleAddDelete) {
        TTenantTestRuntime runtime(MakeTenantTestConfig(true, 2));
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_DEBUG);

        // Add some locks and wait edges.

        TLockHolder lock1(TLockHandle(123, runtime.GetActorSystem(0)));
        TLockHolder lock2(TLockHandle(234, runtime.GetActorSystem(0)));
        TLockHolder lock3(TLockHandle(345, runtime.GetActorSystem(0)));
        TLockHolder lock4(TLockHandle(456, runtime.GetActorSystem(0)));

        TMockDatashard ds(runtime, /*nodeIdx=*/1);

        ds.AddLock(lock1.Info);
        ds.AddLock(lock2.Info);
        ds.AddLock(lock3.Info);
        ds.AddLock(lock4.Info);

        ds.AddWait(lock4.Info, lock1.Info);
        ds.AddWait(lock4.Info, lock2.Info);
        const auto reqId3 = ds.AddWait(lock4.Info, lock3.Info);
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(
            GetWaitGraphString(runtime, 0),
            "456 -> 123\n"
            "456 -> 234\n"
            "456 -> 345\n");

        // Delete a single edge.

        ds.RemoveWait(lock4.Info, reqId3);
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(
            GetWaitGraphString(runtime, 0),
            "456 -> 123\n"
            "456 -> 234\n");

        // Delete the awaiter. Edges should be cleaned up automatically.

        lock4.Handle.Reset();
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(
            GetWaitGraphString(runtime, 1),
            "");
    }

    // Test that wait edges are correctly propagated across a node chain.
    Y_UNIT_TEST(NodeChain) {
        TTenantTestRuntime runtime(MakeTenantTestConfig(true, 5));
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_DEBUG);

        TLockHolder lock1(TLockHandle(123, runtime.GetActorSystem(0)));
        TLockHolder lock2(TLockHandle(234, runtime.GetActorSystem(1)));
        TLockHolder lock3(TLockHandle(345, runtime.GetActorSystem(3)));
        TLockHolder lock4(TLockHandle(456, runtime.GetActorSystem(4)));

        TMockDatashard ds1(runtime, /*nodeIdx=*/0);
        TMockDatashard ds2(runtime, /*nodeIdx=*/2);
        TMockDatashard ds3(runtime, /*nodeIdx=*/4);

        ds1.AddLock(lock1.Info);
        ds1.AddLock(lock2.Info);
        ds1.AddWait(lock1.Info, lock2.Info);

        ds3.AddLock(lock3.Info);
        ds3.AddLock(lock4.Info);
        ds3.AddWait(lock3.Info, lock4.Info);

        ds2.AddLock(lock2.Info);
        ds2.AddLock(lock3.Info);
        ds2.AddWait(lock2.Info, lock3.Info);

        // Check that the wait chain is correctly propagated back to the first datashard.
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(
            GetWaitGraphString(runtime, /*nodeIdx=*/0),
            "123 -> 234\n"
            "234 -> 345\n"
            "345 -> 456\n");
    }

    // Test that a wait graph cycle is correctly propagated across a 4-node cluster.
    Y_UNIT_TEST(DistributedWaitCycle) {
        TTenantTestRuntime runtime(MakeTenantTestConfig(true, 4));
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_DEBUG);

        TLockHolder lock1(TLockHandle(123, runtime.GetActorSystem(0)));
        TLockHolder lock2(TLockHandle(234, runtime.GetActorSystem(1)));

        TMockDatashard ds1(runtime, /*nodeIdx=*/2);
        TMockDatashard ds2(runtime, /*nodeIdx=*/3);

        // Add a cycle to the wait graph with edges and nodes residing on different nodes.

        ds1.AddLock(lock1.Info);
        ds2.AddLock(lock2.Info);

        ds1.AddLock(lock2.Info);
        ds1.AddWait(lock2.Info, lock1.Info);

        ds2.AddLock(lock1.Info);
        const auto reqId = ds2.AddWait(lock1.Info, lock2.Info);

        // Check that the wait graph is correctly instantiated on all nodes.
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
        for (size_t nodeIdx = 0; nodeIdx < runtime.GetNodeCount(); ++nodeIdx) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                GetWaitGraphString(runtime, nodeIdx),
                "123 -> 234\n"
                "234 -> 123\n",
                "with nodeIdx: " << nodeIdx);
        }

        // Break the deadlock and check that the removal is correctly propagated.
        ds2.RemoveWait(lock1.Info, reqId);
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
        for (size_t nodeIdx = 0; nodeIdx < runtime.GetNodeCount(); ++nodeIdx) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                GetWaitGraphString(runtime, nodeIdx),
                "234 -> 123\n",
                "with nodeIdx: " << nodeIdx);
        }
    }
} // Y_UNIT_TEST_SUITE(LockWaitGraph)

} // namespace NLongTxService
} // namespace NKikimr
