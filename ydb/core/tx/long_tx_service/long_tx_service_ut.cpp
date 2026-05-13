#include "long_tx_service.h"

#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>
#include <ydb/core/tx/scheme_board/cache.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>

#include <ydb/library/actors/interconnect/interconnect_impl.h>
#include <ydb/library/testlib/helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <algorithm>

namespace NKikimr {
namespace NLongTxService {

Y_UNIT_TEST_SUITE(LongTxService) {

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

<<<<<<< HEAD
        TTenantTestConfig MakeTenantTestConfig(bool fakeSchemeShard) {
=======
        TTenantTestConfig MakeTenantTestConfig(bool fakeSchemeShard, size_t nodesCount = 2) {
            TVector<TTenantTestConfig::TNodeConfig> nodes(nodesCount, {MakeDefaultTenantPoolConfig()});
>>>>>>> 30e4a301764 (Snapshot Locking (#36668))
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
<<<<<<< HEAD
                {{
                    // Node0
                    {
                        MakeDefaultTenantPoolConfig()
                    },
                    // Node1
                    {
                        MakeDefaultTenantPoolConfig()
                    },
                }},
=======
                nodes,
>>>>>>> 30e4a301764 (Snapshot Locking (#36668))
                // DataCenterCount
                1
            };
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
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, Ydb::StatusIds::SUCCESS);
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

        TLockHandle handle(123, runtime.GetActorSystem(0));

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

    void RunSimpleSnapshotsTest(size_t nodesCount, bool hasTable, bool manySnapshots = false) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSnapshotsLocking(true);
        appConfig.MutableLongTxServiceConfig()->SetInsideDataCenterExchangeFanOut(2);
<<<<<<< HEAD
=======
        appConfig.MutableLongTxServiceConfig()->SetSnapshotsExchangeIntervalSeconds(1);
        appConfig.MutableLongTxServiceConfig()->SetSnapshotsRegistryUpdateIntervalSeconds(1);
        appConfig.MutableLongTxServiceConfig()->SetLocalSnapshotPromotionTimeSeconds(5);
        appConfig.MutableLongTxServiceConfig()->SetMaxRemoteSnapshots(10);
>>>>>>> fc4feee5b5a (Support the snapshot service in column tables (#39235))

        TTenantTestRuntime runtime(MakeTenantTestConfig(false, nodesCount), appConfig);
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_DEBUG);
        StartSchemeCache(runtime);

<<<<<<< HEAD
        auto sender1 = runtime.AllocateEdgeActor(0);
=======
        for (size_t node = 0; node < nodesCount; ++node) {
            UNIT_ASSERT(!runtime.GetAppData(node).SnapshotRegistryHolder->Get());
        }

>>>>>>> fc4feee5b5a (Support the snapshot service in column tables (#39235))
        auto service1 = MakeLongTxServiceID(runtime.GetNodeId(0));

        // Sleep a little, so there's at least one plan step generated
        SimulateSleep(runtime, TDuration::Seconds(1));

        for (size_t node = 0; node < nodesCount; ++node) {
            UNIT_ASSERT(!runtime.GetAppData(node).SnapshotRegistryHolder->Get());
        }

        const ::NKikimr::TTableId table(0, 1);
        const ::NKikimr::TTableId tableWithSchema(table.PathId, 100500);
        const ::NKikimr::TTableId tableWithSysView(table.PathId, "some_sys_view");
        const ::NKikimr::TTableId otherTable(0, 2);

        // Send acquire read snapshots for node 1
        TVector<TActorId> snapshotSenders;
        TVector<NKqp::TSnapshotHandle> handles;
        TVector<TRowVersion> snapshots;
        const ui32 snapshotsCount = manySnapshots ? 12 : 1;
        for (ui32 i = 0; i < snapshotsCount; ++i) {
            const TActorId sender = runtime.AllocateEdgeActor(0);
            snapshotSenders.emplace_back(sender);
            runtime.Send(
                new IEventHandle(service1, sender,
                    new TEvLongTxService::TEvAcquireReadSnapshot("/dc-1", hasTable ? TVector<::NKikimr::TTableId>{table} : TVector<::NKikimr::TTableId>{})),
                0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvAcquireReadSnapshotResult>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, Ydb::StatusIds::SUCCESS);
            handles.emplace_back(std::move(msg->SnapshotHandle));
            snapshots.emplace_back(msg->Snapshot);
        }

        for (size_t node = 0; node < nodesCount; ++node) {
            UNIT_ASSERT(!runtime.GetAppData(node).SnapshotRegistryHolder->Get());
        }

        SimulateSleep(runtime, TDuration::Minutes(1));

        // snapshots have not been promoted yet
        for (size_t node = 0; node < nodesCount; ++node) {
            const auto& registry = runtime.GetAppData(node).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry);
            UNIT_ASSERT_VALUES_EQUAL(registry->GetBorder(), TRowVersion::Max());
            UNIT_ASSERT(registry->GetActiveSnapshots(table).empty());
            UNIT_ASSERT(registry->GetActiveSnapshots(tableWithSchema).empty());
            UNIT_ASSERT(registry->GetActiveSnapshots(tableWithSysView).empty());
            UNIT_ASSERT(registry->GetActiveSnapshots(otherTable).empty());
            for (const auto& snapshot : snapshots) {
                UNIT_ASSERT(!registry->HasSnapshot(table, snapshot));
                UNIT_ASSERT(!registry->HasSnapshot(tableWithSchema, snapshot));
                UNIT_ASSERT(!registry->HasSnapshot(tableWithSysView, snapshot));
                UNIT_ASSERT(!registry->HasSnapshot(otherTable, snapshot));
            }
        }

        SimulateSleep(runtime, TDuration::Minutes(3));

        // snapshots have been promoted
        for (size_t node = 0; node < nodesCount; ++node) {
            const auto& registry = runtime.GetAppData(node).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry);
            if (manySnapshots) {
                const TRowVersion expectedBorder = *std::min_element(snapshots.begin(), snapshots.end());
                UNIT_ASSERT_VALUES_EQUAL(registry->GetBorder(), expectedBorder);
                UNIT_ASSERT(registry->GetActiveSnapshots(table).empty());
                UNIT_ASSERT(registry->GetActiveSnapshots(tableWithSchema).empty());
                UNIT_ASSERT(registry->GetActiveSnapshots(tableWithSysView).empty());
                UNIT_ASSERT(registry->GetActiveSnapshots(otherTable).empty());
                for (const auto& snapshot : snapshots) {
                    UNIT_ASSERT(registry->HasSnapshot(table, snapshot));
                    UNIT_ASSERT(registry->HasSnapshot(tableWithSchema, snapshot));
                    UNIT_ASSERT(registry->HasSnapshot(tableWithSysView, snapshot));
                    UNIT_ASSERT(registry->HasSnapshot(otherTable, snapshot));
                }
            } else {
                UNIT_ASSERT_VALUES_EQUAL(registry->GetBorder(), TRowVersion::Max());
                UNIT_ASSERT(registry->HasSnapshot(table, snapshots.front()));
                UNIT_ASSERT(registry->HasSnapshot(tableWithSchema, snapshots.front()));
                UNIT_ASSERT(registry->HasSnapshot(tableWithSysView, snapshots.front()));
                UNIT_ASSERT(registry->HasSnapshot(otherTable, snapshots.front()) == !hasTable);
                UNIT_ASSERT(registry->GetActiveSnapshots(table).contains(snapshots.front()));
                UNIT_ASSERT(registry->GetActiveSnapshots(tableWithSchema).contains(snapshots.front()));
                UNIT_ASSERT(registry->GetActiveSnapshots(tableWithSysView).contains(snapshots.front()));
                UNIT_ASSERT_VALUES_EQUAL(registry->GetActiveSnapshots(otherTable).contains(snapshots.front()), !hasTable);
            }
        }

<<<<<<< HEAD
        handle.Reset();

        SimulateSleep(runtime, TDuration::Minutes(3));
=======
        handles.clear();
        SimulateSleep(runtime, TDuration::Seconds(10));
>>>>>>> fc4feee5b5a (Support the snapshot service in column tables (#39235))

        // snapshots have been deleted and the information about that is promoted already
        for (size_t node = 0; node < nodesCount; ++node) {
            const auto& registry = runtime.GetAppData(node).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry);
            UNIT_ASSERT_VALUES_EQUAL(registry->GetBorder(), TRowVersion::Max());
            UNIT_ASSERT(registry->GetActiveSnapshots(table).empty());
            UNIT_ASSERT(registry->GetActiveSnapshots(tableWithSchema).empty());
            UNIT_ASSERT(registry->GetActiveSnapshots(tableWithSysView).empty());
            UNIT_ASSERT(registry->GetActiveSnapshots(otherTable).empty());
            for (const auto& snapshot : snapshots) {
                UNIT_ASSERT(!registry->HasSnapshot(table, snapshot));
                UNIT_ASSERT(!registry->HasSnapshot(tableWithSchema, snapshot));
                UNIT_ASSERT(!registry->HasSnapshot(tableWithSysView, snapshot));
                UNIT_ASSERT(!registry->HasSnapshot(otherTable, snapshot));
            }
        }
    }

    Y_UNIT_TEST_TWIN(SimpleSnapshotsSingleNode, HasTable) {
        RunSimpleSnapshotsTest(1, HasTable);
    }

    Y_UNIT_TEST_TWIN(SimpleSnapshotsManyNodes, HasTable) {
        RunSimpleSnapshotsTest(4, HasTable);
    }

    Y_UNIT_TEST_TWIN(SimpleSnapshotsSingleNodeManySnapshots, HasTable) {
        RunSimpleSnapshotsTest(1, HasTable, true);
    }

    Y_UNIT_TEST_TWIN(SimpleSnapshotsManyNodesManySnapshots, HasTable) {
        RunSimpleSnapshotsTest(4, HasTable, true);
    }

} // Y_UNIT_TEST_SUITE(LongTxService)

} // namespace NLongTxService
} // namespace NKikimr
