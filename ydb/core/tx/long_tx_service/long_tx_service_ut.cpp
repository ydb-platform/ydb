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

        TTenantTestConfig MakeTenantTestConfig(bool fakeSchemeShard, size_t nodesCount = 2) {
            TVector<TTenantTestConfig::TNodeConfig> nodes(nodesCount, {MakeDefaultTenantPoolConfig()});
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
                nodes,
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

    void RunSimpleSnapshotsTest(size_t nodesCount, bool hasTable, bool manySnapshots = false) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSnapshotsLocking(true);
        appConfig.MutableLongTxServiceConfig()->SetInsideDataCenterExchangeFanOut(2);
        appConfig.MutableLongTxServiceConfig()->SetSnapshotsExchangeIntervalSeconds(1);
        appConfig.MutableLongTxServiceConfig()->SetSnapshotsRegistryUpdateIntervalSeconds(1);
        appConfig.MutableLongTxServiceConfig()->SetLocalSnapshotPromotionTimeSeconds(5);
        appConfig.MutableLongTxServiceConfig()->SetMaxRemoteSnapshots(10);

        TTenantTestRuntime runtime(MakeTenantTestConfig(false, nodesCount), appConfig);
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_DEBUG);
        StartSchemeCache(runtime);

        for (size_t node = 0; node < nodesCount; ++node) {
            UNIT_ASSERT(!runtime.GetAppData(node).SnapshotRegistryHolder->Get());
        }

        auto service1 = MakeLongTxServiceID(runtime.GetNodeId(0));

        // Sleep a little, so there's at least one plan step generated
        SimulateSleep(runtime, TDuration::Seconds(1));

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

        SimulateSleep(runtime, TDuration::Seconds(2));

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

        SimulateSleep(runtime, TDuration::Seconds(10));

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

            UNIT_ASSERT(runtime.GetAppData(node).SnapshotRegistryHolder->Get()->GetBorder() == (manySnapshots ? snapshots[10] : TRowVersion::Max()));
        }

        handles.clear();
        SimulateSleep(runtime, TDuration::Seconds(10));

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

            UNIT_ASSERT(runtime.GetAppData(node).SnapshotRegistryHolder->Get()->GetBorder() == TRowVersion::Max());
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

    Y_UNIT_TEST(SnapshotsLockingNetworkPartition) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSnapshotsLocking(true);
        appConfig.MutableLongTxServiceConfig()->SetInsideDataCenterExchangeFanOut(3);
        appConfig.MutableLongTxServiceConfig()->SetSnapshotsExchangeIntervalSeconds(1);
        appConfig.MutableLongTxServiceConfig()->SetSnapshotsRegistryUpdateIntervalSeconds(1);
        appConfig.MutableLongTxServiceConfig()->SetLocalSnapshotPromotionTimeSeconds(1);
        appConfig.MutableLongTxServiceConfig()->SetPrefillTimeoutSeconds(10);

        const size_t nodeCount = 10;

        TTenantTestRuntime runtime(MakeTenantTestConfig(false, nodeCount), appConfig);
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_DEBUG);
        StartSchemeCache(runtime);

        THashMap<ui32, ui32> indexByNodeId;
        for (size_t i = 0; i < nodeCount; ++i) {
            indexByNodeId[runtime.GetNodeId(i)] = i;
            UNIT_ASSERT(runtime.GetNodeId(i) != 0);
        }

        // Sleep to allow nodes to discover each other
        SimulateSleep(runtime, TDuration::Seconds(2));

        const ::NKikimr::TTableId table(0, 1);

        ui32 rootNode = 0; 
        size_t dropCounter = 0;
        bool splitNetwork = false;
        auto grabFirst = [&](TAutoPtr<IEventHandle> &ev) -> auto {
            if (ev->GetTypeRewrite() == NKikimr::NLongTxService::TEvLongTxService::TEvCollectSnapshots::EventType
                || ev->GetTypeRewrite() == NKikimr::NLongTxService::TEvLongTxService::TEvPropagateSnapshots::EventType) {

                if (rootNode == 0) {
                    rootNode = ev->Sender.NodeId();
                }

                if (dropCounter == 0 && ev->Sender.NodeId() == rootNode && ev->Recipient.NodeId() != ev->Sender.NodeId()) {
                    ++dropCounter;
                    auto disconnectEv = new TEvInterconnect::TEvNodeDisconnected(ev->Recipient.NodeId());
                    runtime.Send(new IEventHandle(ev->Sender, TActorId(), disconnectEv), indexByNodeId.at(ev->Sender.NodeId()), true);
                    return TTestActorRuntime::EEventAction::DROP;
                }

                if (splitNetwork && ev->Recipient.NodeId() != ev->Sender.NodeId()) {
                    if ((indexByNodeId.at(ev->Recipient.NodeId()) < (nodeCount / 2)) != (indexByNodeId.at(ev->Sender.NodeId()) < (nodeCount / 2))) {
                        ++dropCounter;
                        auto disconnectEv = new TEvInterconnect::TEvNodeDisconnected(ev->Recipient.NodeId());
                        runtime.Send(new IEventHandle(ev->Sender, TActorId(), disconnectEv), indexByNodeId.at(ev->Sender.NodeId()), true);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        auto saveObserver = runtime.SetObserverFunc(grabFirst);
        Y_DEFER {
            runtime.SetObserverFunc(saveObserver);
        };

        // Create a snapshot on node 1
        NKqp::TSnapshotHandle handle1;
        TRowVersion snapshot1;
        {
            auto sender1 = runtime.AllocateEdgeActor(0);
            auto service1 = MakeLongTxServiceID(runtime.GetNodeId(0));
            runtime.Send(
                new IEventHandle(service1, sender1,
                    new TEvLongTxService::TEvAcquireReadSnapshot("/dc-1", {table})),
                0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvAcquireReadSnapshotResult>(sender1);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, Ydb::StatusIds::SUCCESS);
            handle1 = std::move(msg->SnapshotHandle);
            snapshot1 = msg->Snapshot;
        }

        SimulateSleep(runtime, TDuration::Seconds(5));

        UNIT_ASSERT(dropCounter == 1); // retry
        for (size_t i = 0; i < nodeCount; ++i) {
            UNIT_ASSERT(runtime.GetAppData(i).SnapshotRegistryHolder->Get()->HasSnapshot(table, snapshot1));
        }

        splitNetwork = true;

        // Create a snapshot at rootNode 
        NKqp::TSnapshotHandle handle2;
        TRowVersion snapshot2;
        {
            auto sender2 = runtime.AllocateEdgeActor(indexByNodeId.at(rootNode));
            auto service2 = MakeLongTxServiceID(rootNode);
            runtime.Send(
                new IEventHandle(service2, sender2,
                    new TEvLongTxService::TEvAcquireReadSnapshot("/dc-1", {table})),
                indexByNodeId.at(rootNode), true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvAcquireReadSnapshotResult>(sender2);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, Ydb::StatusIds::SUCCESS);
            handle2 = std::move(msg->SnapshotHandle);
            snapshot2 = msg->Snapshot;
        }

        SimulateSleep(runtime, TDuration::Seconds(5));

        UNIT_ASSERT(dropCounter > 1);
        for (size_t i = 0; i < nodeCount / 2; ++i) {
            UNIT_ASSERT(runtime.GetAppData(i).SnapshotRegistryHolder->Get()->HasSnapshot(table, snapshot2)
                    == runtime.GetAppData(0).SnapshotRegistryHolder->Get()->HasSnapshot(table, snapshot2));
        }
        for (size_t i = nodeCount / 2; i < nodeCount; ++i) {
            UNIT_ASSERT(runtime.GetAppData(i).SnapshotRegistryHolder->Get()->HasSnapshot(table, snapshot2)
                    != runtime.GetAppData(0).SnapshotRegistryHolder->Get()->HasSnapshot(table, snapshot2));
        }
    }

    Y_UNIT_TEST(SnapshotsLockingOverflow) {
        const ui32 nodesCount = 2;
        const ::NKikimr::TTableId table(0, 1);
        const ::NKikimr::TTableId otherTable(0, 2);

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSnapshotsLocking(true);
        appConfig.MutableLongTxServiceConfig()->SetInsideDataCenterExchangeFanOut(2);
        appConfig.MutableLongTxServiceConfig()->SetSnapshotsExchangeIntervalSeconds(1);
        appConfig.MutableLongTxServiceConfig()->SetSnapshotsRegistryUpdateIntervalSeconds(1);
        appConfig.MutableLongTxServiceConfig()->SetMaxRemoteSnapshots(3);
        appConfig.MutableLongTxServiceConfig()->SetLocalSnapshotPromotionTimeSeconds(1);

        TTenantTestRuntime runtime(MakeTenantTestConfig(false, nodesCount), appConfig);
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_DEBUG);
        StartSchemeCache(runtime);

        auto sender1 = runtime.AllocateEdgeActor(0);
        auto service1 = MakeLongTxServiceID(runtime.GetNodeId(0));

        SimulateSleep(runtime, TDuration::Seconds(1));

        std::vector<NKqp::TSnapshotHandle> handles;
        std::vector<TRowVersion> snapshots;
        
        for (size_t i = 0; i < 10; ++i) {
            runtime.Send(
                new IEventHandle(service1, sender1,
                    new TEvLongTxService::TEvAcquireReadSnapshot("/dc-1", {table})),
                0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvAcquireReadSnapshotResult>(sender1);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, Ydb::StatusIds::SUCCESS);

            UNIT_ASSERT(snapshots.empty() || msg->Snapshot != snapshots.back());

            handles.emplace_back(std::move(msg->SnapshotHandle));
            snapshots.emplace_back(msg->Snapshot);

            SimulateSleep(runtime, TDuration::Seconds(1));
        }

        SimulateSleep(runtime, TDuration::Seconds(3));


        for (size_t node = 0; node < nodesCount; ++node) {
            for (size_t index = 0; index < snapshots.size(); ++index) {
                const auto& snapshot = snapshots[index];
                UNIT_ASSERT(runtime.GetAppData(node).SnapshotRegistryHolder->Get()->HasSnapshot(table, snapshot));
                UNIT_ASSERT(runtime.GetAppData(node).SnapshotRegistryHolder->Get()->HasSnapshot(otherTable, snapshot) == (index >= 3));
            }

            UNIT_ASSERT(runtime.GetAppData(node).SnapshotRegistryHolder->Get()->GetBorder() == snapshots[3]);

            UNIT_ASSERT(runtime.GetAppData(node).SnapshotRegistryHolder->Get()->GetActiveSnapshots(table).size() == 3);
            UNIT_ASSERT(runtime.GetAppData(node).SnapshotRegistryHolder->Get()->GetActiveSnapshots(otherTable).size() == 0);
        }
    }

    Y_UNIT_TEST(SnapshotsLockingFeatureFlag) {
        const ui32 nodesCount = 2;
        const ::NKikimr::TTableId table(0, 1);

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSnapshotsLocking(false);
        appConfig.MutableLongTxServiceConfig()->SetInsideDataCenterExchangeFanOut(2);
        appConfig.MutableLongTxServiceConfig()->SetSnapshotsExchangeIntervalSeconds(1);
        appConfig.MutableLongTxServiceConfig()->SetSnapshotsRegistryUpdateIntervalSeconds(1);
        appConfig.MutableLongTxServiceConfig()->SetMaxRemoteSnapshots(3);
        appConfig.MutableLongTxServiceConfig()->SetLocalSnapshotPromotionTimeSeconds(1);

        TTenantTestRuntime runtime(MakeTenantTestConfig(false, nodesCount), appConfig);
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_DEBUG);
        StartSchemeCache(runtime);

        SimulateSleep(runtime, TDuration::Seconds(1));

        auto sender0 = runtime.AllocateEdgeActor(0);
        auto service0 = MakeLongTxServiceID(runtime.GetNodeId(0));

        NKqp::TSnapshotHandle handle1;
        TRowVersion snapshot1;
        {
            runtime.Send(
                new IEventHandle(service0, sender0,
                    new TEvLongTxService::TEvAcquireReadSnapshot("/dc-1", {table})),
                0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvAcquireReadSnapshotResult>(sender0);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, Ydb::StatusIds::SUCCESS);
            handle1 = std::move(msg->SnapshotHandle);
            snapshot1 = msg->Snapshot;
        }

        SimulateSleep(runtime, TDuration::Seconds(3));

        {
            UNIT_ASSERT(runtime.GetAppData(0).SnapshotRegistryHolder->Get().get() == nullptr);
            UNIT_ASSERT(runtime.GetAppData(1).SnapshotRegistryHolder->Get().get() == nullptr);
        }
        runtime.GetAppData(0).FeatureFlags.SetEnableSnapshotsLocking(true);
        runtime.GetAppData(1).FeatureFlags.SetEnableSnapshotsLocking(true);

        SimulateSleep(runtime, TDuration::Seconds(2));

        NKqp::TSnapshotHandle handle2;
        TRowVersion snapshot2;
        {
            runtime.Send(
                new IEventHandle(service0, sender0,
                    new TEvLongTxService::TEvAcquireReadSnapshot("/dc-1", {table})),
                0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvAcquireReadSnapshotResult>(sender0);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, Ydb::StatusIds::SUCCESS);
            handle2 = std::move(msg->SnapshotHandle);
            snapshot2 = msg->Snapshot;
        }

        SimulateSleep(runtime, TDuration::Seconds(3));

        {
            const auto& registry = runtime.GetAppData(0).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry.get());
            UNIT_ASSERT(!registry->HasSnapshot(table, snapshot1));
            UNIT_ASSERT(registry->HasSnapshot(table, snapshot2));
        }
        {
            const auto& registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry.get());
            UNIT_ASSERT(!registry->HasSnapshot(table, snapshot1));
            UNIT_ASSERT(registry->HasSnapshot(table, snapshot2));
        }

        runtime.GetAppData(0).FeatureFlags.SetEnableSnapshotsLocking(false);
        runtime.GetAppData(1).FeatureFlags.SetEnableSnapshotsLocking(false);

        SimulateSleep(runtime, TDuration::Seconds(2));

        {
            UNIT_ASSERT(runtime.GetAppData(0).SnapshotRegistryHolder->Get().get() == nullptr);
            UNIT_ASSERT(runtime.GetAppData(1).SnapshotRegistryHolder->Get().get() == nullptr);
        }
    }

    Y_UNIT_TEST(SnapshotsLockingPrefill) {
        const ui32 nodesCount = 2;
        const ::NKikimr::TTableId table(0, 1);

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSnapshotsLocking(false);
        appConfig.MutableLongTxServiceConfig()->SetInsideDataCenterExchangeFanOut(2);
        appConfig.MutableLongTxServiceConfig()->SetSnapshotsExchangeIntervalSeconds(1);
        appConfig.MutableLongTxServiceConfig()->SetSnapshotsRegistryUpdateIntervalSeconds(1);
        appConfig.MutableLongTxServiceConfig()->SetMaxRemoteSnapshots(3);
        appConfig.MutableLongTxServiceConfig()->SetLocalSnapshotPromotionTimeSeconds(1);

        TTenantTestRuntime runtime(MakeTenantTestConfig(false, nodesCount), appConfig);
        runtime.SetLogPriority(NKikimrServices::LONG_TX_SERVICE, NLog::PRI_DEBUG);
        StartSchemeCache(runtime);

        SimulateSleep(runtime, TDuration::Seconds(1));

        bool prefillNotEmpty = false;
        bool blockPropagation = true;
        std::vector<TAutoPtr<IEventHandle>> captured;

        auto observer = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvLongTxService::TEvCollectSnapshots::EventType:
                case TEvLongTxService::TEvPropagateSnapshots::EventType:
                    if (blockPropagation && ev->Sender.NodeId() != ev->Recipient.NodeId()) {
                        captured.push_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    } else {
                        return TTestActorRuntime::EEventAction::PROCESS;
                    }
                case TEvLongTxService::TEvRemoteSnapshotsPrefillResult::EventType: {
                    auto& record = ev->Get<TEvLongTxService::TEvRemoteSnapshotsPrefillResult>()->Record;
                    if (record.GetSnapshots().SnapshotsSize() > 0) {
                        prefillNotEmpty = true;
                    }
                    break;
                }                
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto saveObserver = runtime.SetObserverFunc(observer);
        Y_DEFER {
            runtime.SetObserverFunc(saveObserver);
        };

        {
            UNIT_ASSERT(runtime.GetAppData(0).SnapshotRegistryHolder->Get().get() == nullptr);
            UNIT_ASSERT(runtime.GetAppData(1).SnapshotRegistryHolder->Get().get() == nullptr);
        }
        runtime.GetAppData(0).FeatureFlags.SetEnableSnapshotsLocking(true);

        SimulateSleep(runtime, TDuration::Seconds(2));

        auto sender0 = runtime.AllocateEdgeActor(0);
        auto service0 = MakeLongTxServiceID(runtime.GetNodeId(0));

        NKqp::TSnapshotHandle handle2;
        TRowVersion snapshot2;
        {
            runtime.Send(
                new IEventHandle(service0, sender0,
                    new TEvLongTxService::TEvAcquireReadSnapshot("/dc-1", {table})),
                0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvLongTxService::TEvAcquireReadSnapshotResult>(sender0);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, Ydb::StatusIds::SUCCESS);
            handle2 = std::move(msg->SnapshotHandle);
            snapshot2 = msg->Snapshot;
        }

        SimulateSleep(runtime, TDuration::Seconds(3));

        {
            const auto& registry = runtime.GetAppData(0).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry.get());
            UNIT_ASSERT(registry->HasSnapshot(table, snapshot2));
        }
        {
            const auto& registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(!registry.get());
        }

        UNIT_ASSERT(!prefillNotEmpty);

        runtime.GetAppData(1).FeatureFlags.SetEnableSnapshotsLocking(true);

        SimulateSleep(runtime, TDuration::Seconds(2));

        UNIT_ASSERT(prefillNotEmpty);
        
        {
            const auto& registry = runtime.GetAppData(0).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry.get());
            UNIT_ASSERT(registry->HasSnapshot(table, snapshot2));
        }
        {
            const auto& registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry.get());
            UNIT_ASSERT(registry->HasSnapshot(table, snapshot2));
        }

        handle2.Reset();

        SimulateSleep(runtime, TDuration::Seconds(2));

        {
            const auto& registry = runtime.GetAppData(0).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry.get());
            UNIT_ASSERT(!registry->HasSnapshot(table, snapshot2));
        }
        {
            const auto& registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry.get());
            UNIT_ASSERT(registry->HasSnapshot(table, snapshot2));
        }

        blockPropagation = false;
        for (auto& ev : captured) {
            runtime.Send(ev.Release());
        }

        SimulateSleep(runtime, TDuration::Seconds(2));

        {
            const auto& registry = runtime.GetAppData(0).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry.get());
            UNIT_ASSERT(!registry->HasSnapshot(table, snapshot2));
        }
        {
            const auto& registry = runtime.GetAppData(1).SnapshotRegistryHolder->Get();
            UNIT_ASSERT(registry.get());
            UNIT_ASSERT(!registry->HasSnapshot(table, snapshot2));
        }
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

        template<class TEvent>
        typename TEvent::TPtr GrabEvent(TDuration simTimeout = TDuration::Max()) {
            return Runtime.GrabEdgeEventRethrow<TEvent>(ActorId, simTimeout);
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

        TLockHolder lock1(TLockHandle(123, runtime.GetActorSystem(0), TInstant::Seconds(123456)));
        TLockHolder lock2(TLockHandle(234, runtime.GetActorSystem(1), TInstant::Seconds(234567)));

        TMockDatashard ds1(runtime, /*nodeIdx=*/2);
        TMockDatashard ds2(runtime, /*nodeIdx=*/3);

        // Add a cycle to the wait graph with edges and nodes residing on different nodes.

        ds1.AddLock(lock1.Info);
        ds2.AddLock(lock2.Info);

        ds1.AddLock(lock2.Info);
        const auto reqId = ds1.AddWait(lock2.Info, lock1.Info);

        ds2.AddLock(lock1.Info);
        ds2.AddWait(lock1.Info, lock2.Info);

        // Check that the wait graph is correctly instantiated on all nodes.
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
        for (size_t nodeIdx = 0; nodeIdx < runtime.GetNodeCount(); ++nodeIdx) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                GetWaitGraphString(runtime, nodeIdx),
                "123 -> 234\n"
                "234 -> 123\n",
                "with nodeIdx: " << nodeIdx);
        }

        auto deadlockEv = ds1.GrabEvent<TEvLongTxService::TEvWaitingLockDeadlock>();
        UNIT_ASSERT_VALUES_EQUAL(deadlockEv->Get()->RequestId, reqId);

        // Break the deadlock and check that the removal is correctly propagated.
        ds1.RemoveWait(lock2.Info, reqId);
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
        for (size_t nodeIdx = 0; nodeIdx < runtime.GetNodeCount(); ++nodeIdx) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                GetWaitGraphString(runtime, nodeIdx),
                "123 -> 234\n",
                "with nodeIdx: " << nodeIdx);
        }
    }
} // Y_UNIT_TEST_SUITE(LockWaitGraph)

} // namespace NLongTxService
} // namespace NKikimr
