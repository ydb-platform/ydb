#include "long_tx_service.h"

#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/scheme_board/cache.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>

#include <library/cpp/actors/interconnect/interconnect_impl.h>
#include <library/cpp/testing/unittest/registar.h>

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
                // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                {},
                // NodeType
                "storage"
            };
            return res;
        }

        TTenantTestConfig MakeTenantTestConfig(bool fakeSchemeShard) {
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
                // DataCenterCount
                1
            };
            return res;
        }

        void StartSchemeCache(TTestActorRuntime& runtime, const TString& root = DOMAIN1_NAME) {
            for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
                auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>();
                cacheConfig->Roots.emplace_back(1, SCHEME_SHARD1_ID, root);
                cacheConfig->Counters = new NMonitoring::TDynamicCounters();

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

        auto observer = [&](auto& runtime, auto& ev) {
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
            badTxId.NodeId = 3;
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

} // Y_UNIT_TEST_SUITE(LongTxService)

} // namespace NLongTxService
} // namespace NKikimr
