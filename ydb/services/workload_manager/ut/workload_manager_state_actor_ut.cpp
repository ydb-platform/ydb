#include <ydb/core/base/appdata.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/ds_table/config.h>
#include <ydb/services/metadata/ds_table/service.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/workload_manager/actors/workload_manager_state_actor.h>
#include <ydb/services/workload_manager/events.h>
#include <ydb/services/workload_manager/gateway.h>
#include <ydb/services/workload_manager/gateway_internal.h>
#include <ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/snapshot.h>
#include <ydb/services/workload_manager/service/service.h>
#include <ydb/services/workload_manager/ut/common/query_classifier_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NWorkloadManager {

namespace {

constexpr TDuration WAIT_TIMEOUT = TDuration::Seconds(10);

struct TFixture {
    TTestBasicRuntime Runtime{ 1 };
    std::shared_ptr<NPrivate::TWorkloadManagerGateway> Gateway;
    TActorId ServicesEdge;
    TActorId MetadataEdge;
    TActorId Sender;
    TActorId StateActor;
    ui32 NodeId = 0;

    void Init(bool enableResourcePools = true) {
        TAppPrepare app;
        app.SetEnableResourcePools(enableResourcePools);
        Runtime.Initialize(app.Unwrap());
        NodeId = Runtime.GetNodeId(0);

        Gateway = std::make_shared<NPrivate::TWorkloadManagerGateway>();
        Runtime.GetAppData().WorkloadManagerGateway = Gateway;

        ServicesEdge = Runtime.AllocateEdgeActor();
        Sender = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(NKqp::MakeKqpSchedulerServiceId(NodeId), ServicesEdge);
        Runtime.RegisterService(MakeServiceId(NodeId), ServicesEdge);

        const std::unique_ptr<IActor> metadataService(NMetadata::NProvider::CreateService(NMetadata::NProvider::TConfig()));
        MetadataEdge = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(NMetadata::NProvider::MakeServiceId(NodeId), MetadataEdge);

        StateActor = Runtime.Register(CreateWorkloadManagerStateActor(Gateway));

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        Runtime.DispatchEvents(options);
    }

    std::shared_ptr<IQueryClassifier> TryCreateQueryClassifier(const TString& databaseId, const TString& poolId = {}) {
        return Runtime.RunCall([databaseId, poolId] {
            return AppData()->WorkloadManagerGateway->TryCreateQueryClassifier(databaseId, TClassifyContext{ .PoolId = poolId });
        });
    }

    TString ClassifyPoolId(const TString& databaseId, const TString& poolId = {}) {
        auto classifier = TryCreateQueryClassifier(databaseId, poolId);
        UNIT_ASSERT(classifier);
        const auto result = classifier->PreCompileClassify(NKqp::TUserRequestContext{});
        const auto* resolved = std::get_if<IQueryClassifier::TResolvedPoolId>(&result);
        UNIT_ASSERT(resolved);
        return resolved->PoolId;
    }

    void WaitAddPool(const TString& poolId) {
        while (true) {
            auto ev = Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvAddPool>(ServicesEdge, WAIT_TIMEOUT);
            UNIT_ASSERT(ev);
            if (ev->Get()->PoolId == poolId) {
                return;
            }
        }
    }

    void SetResourcePoolsEnabled(bool enabled) {
        auto ev = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        ev->Record.MutableConfig()->MutableFeatureFlags()->SetEnableResourcePools(enabled);
        Runtime.Send(new IEventHandle(StateActor, Sender, ev.release()));
        UNIT_ASSERT(Runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(Sender, WAIT_TIMEOUT));
    }

    void SetResourcePoolsOnServerlessEnabled(bool enabled) {
        auto ev = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        auto* featureFlags = ev->Record.MutableConfig()->MutableFeatureFlags();
        featureFlags->SetEnableResourcePools(true);
        featureFlags->SetEnableResourcePoolsOnServerless(enabled);
        Runtime.Send(new IEventHandle(StateActor, Sender, ev.release()));
        UNIT_ASSERT(Runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(Sender, WAIT_TIMEOUT));
    }

    void InjectClassifiers(const TString& poolId) {
        Runtime.Send(new IEventHandle(StateActor, Sender,
            new NMetadata::NProvider::TEvRefreshSubscriberData(MakeClassifierSnapshot({
                MakeClassifierConfig(TEST_DB, "c1", /*rank=*/1, poolId),
            }))));
    }

    void InjectPoolInfo(const TString& poolId) {
        NResourcePool::TPoolSettings poolSettings;
        poolSettings.ConcurrentQueryLimit = 10;
        Runtime.Send(new IEventHandle(StateActor, Sender, new TEvUpdatePoolInfo(TEST_DB, poolId, poolSettings, std::nullopt)));
    }

    void Sync(const TString& probePoolId) {
        Runtime.Send(new IEventHandle(StateActor, Sender, new TEvEnsurePoolSubscribed(TEST_DB, probePoolId)));
        WaitAddPool(probePoolId);
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(WorkloadManagerStateActor) {
    // Resource pools disabled: TryCreateQueryClassifier returns nullptr.
    Y_UNIT_TEST(TestNoClassifierWhenPoolsDisabled) {
        TFixture fx;
        fx.Init(/*enableResourcePools=*/false);

        UNIT_ASSERT(!fx.TryCreateQueryClassifier(TEST_DB));
    }

    // Classifier metadata not received yet. The state actor:
    // - lets the gateway create a classifier at once, the query goes to the explicit or default pool,
    // - applies classifiers once the metadata snapshot arrives.
    Y_UNIT_TEST(TestClassifyWithoutMetadataSnapshot) {
        TFixture fx;
        fx.Init();

        UNIT_ASSERT(fx.Runtime.GrabEdgeEvent<NMetadata::NProvider::TEvSubscribeExternal>(fx.MetadataEdge, WAIT_TIMEOUT));

        UNIT_ASSERT_VALUES_EQUAL(fx.ClassifyPoolId(TEST_DB), NResourcePool::DEFAULT_POOL_ID);
        UNIT_ASSERT_VALUES_EQUAL(fx.ClassifyPoolId(TEST_DB, "poolB"), "poolB");

        fx.InjectClassifiers("poolA");
        fx.WaitAddPool("poolA");
        fx.InjectPoolInfo("poolA");
        fx.Sync("probe");

        UNIT_ASSERT_VALUES_EQUAL(fx.ClassifyPoolId(TEST_DB), "poolA");
    }

    // Serverless gate by database id. The gateway:
    // - creates no classifier for a serverless id while resource pools on serverless are off,
    // - creates a classifier for a dedicated id,
    // - creates a classifier for a serverless id once resource pools on serverless are on.
    Y_UNIT_TEST(TestServerlessGateByDatabaseId) {
        TFixture fx;
        fx.Init();

        const TString serverlessId = "72075186224037888:5:/Root/serverless";
        UNIT_ASSERT(!fx.TryCreateQueryClassifier(serverlessId));
        UNIT_ASSERT(fx.TryCreateQueryClassifier("/Root/dedicated"));

        fx.SetResourcePoolsOnServerlessEnabled(true);
        UNIT_ASSERT(fx.TryCreateQueryClassifier(serverlessId));
    }

    // Resource pools switched off and on at runtime. The state actor:
    // - drops classifiers on switch off, so none are applied after switch on until metadata re-delivers them,
    // - ignores a classifiers snapshot delivered after unsubscribe.
    Y_UNIT_TEST(TestPoolsReenabledDropsStaleClassifiers) {
        TFixture fx;
        fx.Init();

        UNIT_ASSERT(fx.Runtime.GrabEdgeEvent<NMetadata::NProvider::TEvSubscribeExternal>(fx.MetadataEdge, WAIT_TIMEOUT));
        fx.InjectClassifiers("poolA");
        fx.WaitAddPool("poolA");
        fx.InjectPoolInfo("poolA");
        fx.Sync("probe1");
        UNIT_ASSERT_VALUES_EQUAL(fx.ClassifyPoolId(TEST_DB), "poolA");

        fx.SetResourcePoolsEnabled(false);
        UNIT_ASSERT(!fx.TryCreateQueryClassifier(TEST_DB));
        UNIT_ASSERT(fx.Runtime.GrabEdgeEvent<NMetadata::NProvider::TEvUnsubscribeExternal>(fx.MetadataEdge, WAIT_TIMEOUT));

        fx.InjectClassifiers("poolA");
        fx.Sync("probe2");

        fx.SetResourcePoolsEnabled(true);
        UNIT_ASSERT(fx.Runtime.GrabEdgeEvent<NMetadata::NProvider::TEvSubscribeExternal>(fx.MetadataEdge, WAIT_TIMEOUT));
        UNIT_ASSERT_VALUES_EQUAL(fx.ClassifyPoolId(TEST_DB), NResourcePool::DEFAULT_POOL_ID);
    }

    // State actor stopped: the gateway is cleared, TryCreateQueryClassifier returns nullptr.
    Y_UNIT_TEST(TestPassAwayClearsGateway) {
        TFixture fx;
        fx.Init();

        UNIT_ASSERT(fx.TryCreateQueryClassifier(TEST_DB));

        fx.Runtime.Send(new IEventHandle(fx.StateActor, fx.Sender, new TEvents::TEvPoison()));
        UNIT_ASSERT(fx.Runtime.GrabEdgeEvent<NMetadata::NProvider::TEvUnsubscribeExternal>(fx.MetadataEdge, WAIT_TIMEOUT));

        UNIT_ASSERT(!fx.TryCreateQueryClassifier(TEST_DB));
    }

    // Pool subscription requests. The state actor:
    // - sends TEvAddPool + TEvSubscribeOnPoolChanges for the first request,
    // - sends nothing for a duplicate while in flight,
    // - subscribes a different pool.
    Y_UNIT_TEST(TestEnsurePoolSubscribedDedupsWhileInFlight) {
        TFixture fx;
        fx.Init();

        // First EnsurePoolSubscribed fires TEvAddPool + TEvSubscribeOnPoolChanges.
        fx.Runtime.Send(new IEventHandle(fx.StateActor, fx.Sender, new TEvEnsurePoolSubscribed("/Root/db1", "poolA")));
        auto firstAdd = fx.Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvAddPool>(fx.ServicesEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(firstAdd->Get()->PoolId, "poolA");
        auto firstSub = fx.Runtime.GrabEdgeEvent<TEvSubscribeOnPoolChanges>(fx.ServicesEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(firstSub->Get()->PoolId, "poolA");

        // Duplicate EnsurePoolSubscribed for the same key should be deduped;
        // a subscribe for a different pool must still proceed.
        fx.Runtime.Send(new IEventHandle(fx.StateActor, fx.Sender, new TEvEnsurePoolSubscribed("/Root/db1", "poolA")));
        fx.Runtime.Send(new IEventHandle(fx.StateActor, fx.Sender, new TEvEnsurePoolSubscribed("/Root/db1", "poolB")));

        auto nextAdd = fx.Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvAddPool>(fx.ServicesEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(nextAdd->Get()->PoolId, "poolB", "Duplicate EnsurePoolSubscribed must not fire TEvAddPool");
        auto nextSub = fx.Runtime.GrabEdgeEvent<TEvSubscribeOnPoolChanges>(fx.ServicesEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(nextSub->Get()->PoolId, "poolB", "Duplicate EnsurePoolSubscribed must not fire TEvSubscribeOnPoolChanges");
    }

    // Pool dropped. The state actor:
    // - resubscribes once on the first nullopt update,
    // - sends no duplicate resubscribe on the next nullopt.
    Y_UNIT_TEST(TestExpiredPoolDedupsResubscribe) {
        TFixture fx;
        fx.Init();

        // Populate cache: subscribe, then feed the config update.
        fx.Runtime.Send(new IEventHandle(fx.StateActor, fx.Sender, new TEvEnsurePoolSubscribed("/Root/db1", "poolA")));
        fx.Runtime.GrabEdgeEvent<TEvSubscribeOnPoolChanges>(fx.ServicesEdge, WAIT_TIMEOUT);

        fx.Runtime.Send(new IEventHandle(
            fx.StateActor, fx.Sender, new TEvUpdatePoolInfo("/Root/db1", "poolA", NResourcePool::TPoolSettings{}, std::nullopt)));

        // First deletion signal marks expired and re-subscribes (in-flight was empty).
        fx.Runtime.Send(new IEventHandle(fx.StateActor, fx.Sender, new TEvUpdatePoolInfo("/Root/db1", "poolA", std::nullopt, std::nullopt)));
        auto resub = fx.Runtime.GrabEdgeEvent<TEvSubscribeOnPoolChanges>(fx.ServicesEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(resub->Get()->PoolId, "poolA");

        // Second deletion signal (or duplicate expiration) should not fire another
        // subscribe. Probe with a different pool to force a next event.
        fx.Runtime.Send(new IEventHandle(fx.StateActor, fx.Sender, new TEvUpdatePoolInfo("/Root/db1", "poolA", std::nullopt, std::nullopt)));
        fx.Runtime.Send(new IEventHandle(fx.StateActor, fx.Sender, new TEvEnsurePoolSubscribed("/Root/db1", "poolB")));

        auto next = fx.Runtime.GrabEdgeEvent<TEvSubscribeOnPoolChanges>(fx.ServicesEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(next->Get()->PoolId, "poolB", "Repeated deletion signals must not fire duplicate resubscribes");
    }
}

}   // namespace NKikimr::NWorkloadManager
