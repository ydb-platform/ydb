#include <library/cpp/testing/unittest/registar.h>

#include <ydb/services/workload_manager/actors/workload_manager_state_actor.h>
#include <ydb/services/workload_manager/events.h>
#include <ydb/services/workload_manager/gateway.h>
#include <ydb/services/workload_manager/gateway_internal.h>
#include <ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/snapshot.h>
#include <ydb/services/workload_manager/service/service.h>
#include <ydb/services/workload_manager/ut/common/query_classifier_ut_common.h>

#include <ydb/services/metadata/abstract/common.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>


namespace NKikimr::NWorkloadManager {

namespace {

constexpr TDuration WAIT_TIMEOUT = TDuration::Seconds(10);

struct TFixture {
    TTestBasicRuntime Runtime{1};
    std::shared_ptr<NPrivate::TWorkloadManagerGateway> Gateway;
    TActorId SchemeCacheEdge;
    TActorId ServicesEdge;
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

        SchemeCacheEdge = Runtime.AllocateEdgeActor();
        ServicesEdge = Runtime.AllocateEdgeActor();
        Sender = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(MakeSchemeCacheID(), SchemeCacheEdge);
        Runtime.RegisterService(NKqp::MakeKqpSchedulerServiceId(NodeId), ServicesEdge);
        Runtime.RegisterService(MakeServiceId(NodeId), ServicesEdge);

        StateActor = Runtime.Register(CreateWorkloadManagerStateActor(Gateway));
        Runtime.EnableScheduleForActor(StateActor);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        Runtime.DispatchEvents(options);

        // Force classifier metadata Ready regardless of whether the process-wide
        // NMetadata::NProvider::TServiceOperator singleton was flipped by a prior test.
        Runtime.Send(new IEventHandle(
            StateActor, Sender,
            new NMetadata::NProvider::TEvRefreshSubscriberData(MakeClassifierSnapshot({}))));
    }

    void Warmup(const TString& databasePath) {
        Runtime.RunCall([databasePath] {
            AppData()->WorkloadManagerGateway->Warmup(databasePath);
            return 0;
        });
    }

    TReadyInfo EnsureReady(const TString& databaseId) {
        return Runtime.RunCall([databaseId] {
            return AppData()->WorkloadManagerGateway->EnsureReady(databaseId);
        });
    }

    void SubscribeOnReady(const TString& databaseId, const TActorId& subscriber, ui64 cookie) {
        Runtime.RunCall([&, databaseId, cookie] {
            AppData()->WorkloadManagerGateway->SubscribeOnReady(databaseId, subscriber, cookie);
            return 0;
        });
    }

    void SetResourcePoolsEnabled(bool enabled) {
        auto ev = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        ev->Record.MutableConfig()->MutableFeatureFlags()->SetEnableResourcePools(enabled);
        Runtime.Send(new IEventHandle(StateActor, Sender, ev.release()));
    }

    void InjectFetchResponse(const TString& databasePath, const TString& databaseId,
                             bool serverless, TPathId pathId,
                             Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
                             NYql::TIssues issues = {}) {
        Runtime.Send(new IEventHandle(
            StateActor, Sender,
            new TEvFetchDatabaseResponse(status, databasePath, databaseId, serverless, pathId, std::move(issues))));
    }
};

TString GetNavigatePath(const TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
    UNIT_ASSERT(!ev->Get()->Request->ResultSet.empty());
    return CanonizePath(JoinPath(ev->Get()->Request->ResultSet[0].Path));
}

}

Y_UNIT_TEST_SUITE(WorkloadManagerStateActor) {

    // Resource pools disabled: EnsureReady returns Disabled for any database.
    Y_UNIT_TEST(TestEnsureWhenPoolsDisabled) {
        TFixture fx;
        fx.Init(/*enableResourcePools=*/false);

        auto info = fx.EnsureReady(TEST_DB);
        UNIT_ASSERT(info.State == EReadyState::Disabled);
    }

    // Resource pools enabled, database info fetched: EnsureReady returns Ready.
    Y_UNIT_TEST(TestEnsureWhenPoolsEnabled) {
        TFixture fx;
        fx.Init();

        fx.InjectFetchResponse("/Root/db1", "/Root/db1", /*serverless=*/false, TPathId(1, 1));
        // Successful fetch response makes the state actor subscribe to a scheme cache watch —
        // use that as the sync point.
        auto watch = fx.Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvWatchPathId>(fx.SchemeCacheEdge, WAIT_TIMEOUT);
        UNIT_ASSERT(watch);

        auto info = fx.EnsureReady("/Root/db1");
        UNIT_ASSERT(info.State == EReadyState::Ready);
    }

    // Warmup for an unknown database: the state actor starts a database info fetch (scheme cache navigate) for the path.
    Y_UNIT_TEST(TestWarmupSpawnsFetcher) {
        TFixture fx;
        fx.Init();

        fx.Warmup("/Root/db1");

        auto ev = fx.Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>(fx.SchemeCacheEdge, WAIT_TIMEOUT);
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(GetNavigatePath(ev), "/Root/db1");
    }

    // Repeated warmups. The state actor:
    // - does not start a second fetch for a path already in flight,
    // - starts a fetch for a different path.
    Y_UNIT_TEST(TestWarmupDedupsByPath) {
        TFixture fx;
        fx.Init();

        fx.Warmup("/Root/dbA");
        auto first = fx.Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>(fx.SchemeCacheEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(GetNavigatePath(first), "/Root/dbA");

        // Second warmup for the same path should be deduped; a warmup for a different path proceeds.
        fx.Warmup("/Root/dbA");
        fx.Warmup("/Root/dbB");

        auto next = fx.Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>(fx.SchemeCacheEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(GetNavigatePath(next), "/Root/dbB",
                                    "Second Warmup for the same path should not spawn a fetcher");
    }

    // Warmup with an empty path: the gateway sends nothing, the next valid warmup still fetches.
    Y_UNIT_TEST(TestWarmupIgnoresEmptyPath) {
        TFixture fx;
        fx.Init();

        fx.Warmup("");
        fx.Warmup("/Root/db1");

        auto ev = fx.Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>(fx.SchemeCacheEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(GetNavigatePath(ev), "/Root/db1",
                                    "Empty-path Warmup should not spawn a fetcher");
    }

    // Subscribe for a database already fetched: the state actor replies SUCCESS at once with the subscriber cookie.
    Y_UNIT_TEST(TestSubscribeWhenAlreadyKnown) {
        TFixture fx;
        fx.Init();

        fx.InjectFetchResponse("/Root/db1", "/Root/db1", /*serverless=*/false, TPathId(1, 1));
        auto watch = fx.Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvWatchPathId>(fx.SchemeCacheEdge, WAIT_TIMEOUT);
        UNIT_ASSERT(watch);

        const TActorId subscriber = fx.Runtime.AllocateEdgeActor();
        fx.SubscribeOnReady("/Root/db1", subscriber, /*cookie=*/42);

        auto ev = fx.Runtime.GrabEdgeEvent<TEvWorkloadManagerReady>(subscriber, WAIT_TIMEOUT);
        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Cookie, 42u);
        UNIT_ASSERT_EQUAL(ev->Get()->Status, Ydb::StatusIds::SUCCESS);
    }

    // Subscribe for an unknown database. The state actor:
    // - starts a database info fetch,
    // - replies SUCCESS with the subscriber cookie once the fetch succeeds.
    Y_UNIT_TEST(TestSubscribeWhenNotKnown) {
        TFixture fx;
        fx.Init();

        const TActorId subscriber = fx.Runtime.AllocateEdgeActor();
        fx.SubscribeOnReady("/Root/db1", subscriber, /*cookie=*/7);

        auto navigate = fx.Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>(fx.SchemeCacheEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_C(navigate, "SubscribeOnReady should trigger a fetch when info is missing");

        fx.InjectFetchResponse("/Root/db1", "/Root/db1", /*serverless=*/false, TPathId(1, 1));

        auto ready = fx.Runtime.GrabEdgeEvent<TEvWorkloadManagerReady>(subscriber, WAIT_TIMEOUT);
        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(ready->Get()->Cookie, 7u);
        UNIT_ASSERT_EQUAL(ready->Get()->Status, Ydb::StatusIds::SUCCESS);
    }

    // Subscribe, database info fetch fails. The state actor:
    // - replies with the fetch status and message,
    // - does not cache the error: EnsureReady returns Pending and the next subscriber starts a new fetch.
    Y_UNIT_TEST(TestSubscribeWhenError) {
        TFixture fx;
        fx.Init();

        const TActorId subscriber = fx.Runtime.AllocateEdgeActor();
        fx.SubscribeOnReady("/Root/db1", subscriber, /*cookie=*/9);

        fx.Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>(fx.SchemeCacheEdge, WAIT_TIMEOUT);

        NYql::TIssues issues;
        issues.AddIssue(NYql::TIssue("scheme cache miss"));
        fx.InjectFetchResponse("/Root/db1", "/Root/db1", /*serverless=*/false, TPathId(), Ydb::StatusIds::NOT_FOUND, issues);

        auto ready = fx.Runtime.GrabEdgeEvent<TEvWorkloadManagerReady>(subscriber, WAIT_TIMEOUT);
        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(ready->Get()->Cookie, 9u);
        UNIT_ASSERT_EQUAL(ready->Get()->Status, Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_STRING_CONTAINS(ready->Get()->Message, "scheme cache miss");

        auto info = fx.EnsureReady("/Root/db1");
        UNIT_ASSERT(info.State == EReadyState::Pending);

        fx.SubscribeOnReady("/Root/db1", subscriber, /*cookie=*/10);
        auto navigate = fx.Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>(fx.SchemeCacheEdge, WAIT_TIMEOUT);
        UNIT_ASSERT(navigate);
        UNIT_ASSERT_VALUES_EQUAL(GetNavigatePath(navigate), "/Root/db1");
    }

    // Pool subscription requests. The state actor:
    // - sends TEvAddPool + TEvSubscribeOnPoolChanges for the first request,
    // - sends nothing for a duplicate while in flight,
    // - subscribes a different pool.
    Y_UNIT_TEST(TestEnsurePoolSubscribedDedupsWhileInFlight) {
        TFixture fx;
        fx.Init();

        // First EnsurePoolSubscribed fires TEvAddPool + TEvSubscribeOnPoolChanges.
        fx.Runtime.Send(new IEventHandle(
            fx.StateActor, fx.Sender,
            new TEvEnsurePoolSubscribed("/Root/db1", "poolA")));
        auto firstAdd = fx.Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvAddPool>(fx.ServicesEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(firstAdd->Get()->PoolId, "poolA");
        auto firstSub = fx.Runtime.GrabEdgeEvent<TEvSubscribeOnPoolChanges>(fx.ServicesEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(firstSub->Get()->PoolId, "poolA");

        // Duplicate EnsurePoolSubscribed for the same key should be deduped;
        // a subscribe for a different pool must still proceed.
        fx.Runtime.Send(new IEventHandle(
            fx.StateActor, fx.Sender,
            new TEvEnsurePoolSubscribed("/Root/db1", "poolA")));
        fx.Runtime.Send(new IEventHandle(
            fx.StateActor, fx.Sender,
            new TEvEnsurePoolSubscribed("/Root/db1", "poolB")));

        auto nextAdd = fx.Runtime.GrabEdgeEvent<NKqp::NScheduler::TEvAddPool>(fx.ServicesEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(nextAdd->Get()->PoolId, "poolB",
                                    "Duplicate EnsurePoolSubscribed must not fire TEvAddPool");
        auto nextSub = fx.Runtime.GrabEdgeEvent<TEvSubscribeOnPoolChanges>(fx.ServicesEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(nextSub->Get()->PoolId, "poolB",
                                    "Duplicate EnsurePoolSubscribed must not fire TEvSubscribeOnPoolChanges");
    }

    // Pool dropped. The state actor:
    // - resubscribes once on the first nullopt update,
    // - sends no duplicate resubscribe on the next nullopt.
    Y_UNIT_TEST(TestExpiredPoolDedupsResubscribe) {
        TFixture fx;
        fx.Init();

        // Populate cache: subscribe, then feed the config update.
        fx.Runtime.Send(new IEventHandle(
            fx.StateActor, fx.Sender,
            new TEvEnsurePoolSubscribed("/Root/db1", "poolA")));
        fx.Runtime.GrabEdgeEvent<TEvSubscribeOnPoolChanges>(fx.ServicesEdge, WAIT_TIMEOUT);

        fx.Runtime.Send(new IEventHandle(
            fx.StateActor, fx.Sender,
            new TEvUpdatePoolInfo("/Root/db1", "poolA", NResourcePool::TPoolSettings{}, std::nullopt)));

        // First deletion signal marks expired and re-subscribes (in-flight was empty).
        fx.Runtime.Send(new IEventHandle(
            fx.StateActor, fx.Sender,
            new TEvUpdatePoolInfo("/Root/db1", "poolA", std::nullopt, std::nullopt)));
        auto resub = fx.Runtime.GrabEdgeEvent<TEvSubscribeOnPoolChanges>(fx.ServicesEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(resub->Get()->PoolId, "poolA");

        // Second deletion signal (or duplicate expiration) should not fire another
        // subscribe. Probe with a different pool to force a next event.
        fx.Runtime.Send(new IEventHandle(
            fx.StateActor, fx.Sender,
            new TEvUpdatePoolInfo("/Root/db1", "poolA", std::nullopt, std::nullopt)));
        fx.Runtime.Send(new IEventHandle(
            fx.StateActor, fx.Sender,
            new TEvEnsurePoolSubscribed("/Root/db1", "poolB")));

        auto next = fx.Runtime.GrabEdgeEvent<TEvSubscribeOnPoolChanges>(fx.ServicesEdge, WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(next->Get()->PoolId, "poolB",
                                    "Repeated deletion signals must not fire duplicate resubscribes");
    }

    // Serverless database, resource pools disabled on serverless: EnsureReady returns Disabled.
    Y_UNIT_TEST(TestEnsureWhenPoolsDisabledOnServerless) {
        TFixture fx;
        fx.Init();

        fx.InjectFetchResponse("/Root/db1", "/Root/db1", /*serverless=*/true, TPathId(1, 1));
        auto watch = fx.Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvWatchPathId>(fx.SchemeCacheEdge, WAIT_TIMEOUT);
        UNIT_ASSERT(watch);

        auto info = fx.EnsureReady("/Root/db1");
        UNIT_ASSERT(info.State == EReadyState::Disabled);
    }

    // Database info fetch hangs. The state actor:
    // - releases the subscriber with retryable UNAVAILABLE once the request times out,
    // - does not cache the timeout: EnsureReady returns Pending.
    Y_UNIT_TEST(TestSubscribeTimesOutToUnavailable) {
        TFixture fx;
        fx.Init();

        const TActorId subscriber = fx.Runtime.AllocateEdgeActor();
        fx.SubscribeOnReady("/Root/db1", subscriber, /*cookie=*/11);
        auto navigate = fx.Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>(fx.SchemeCacheEdge, WAIT_TIMEOUT);
        UNIT_ASSERT(navigate);

        auto ready = fx.Runtime.GrabEdgeEvent<TEvWorkloadManagerReady>(subscriber, WAIT_TIMEOUT);
        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(ready->Get()->Cookie, 11u);
        UNIT_ASSERT_EQUAL(ready->Get()->Status, Ydb::StatusIds::UNAVAILABLE);

        auto info = fx.EnsureReady("/Root/db1");
        UNIT_ASSERT(info.State == EReadyState::Pending);
    }

    // State actor stopped while a subscriber waits. The state actor:
    // - releases the subscriber with SUCCESS,
    // - clears the gateway, so EnsureReady returns Disabled.
    Y_UNIT_TEST(TestPassAwayReleasesSubscribers) {
        TFixture fx;
        fx.Init();

        const TActorId subscriber = fx.Runtime.AllocateEdgeActor();
        fx.SubscribeOnReady("/Root/db1", subscriber, /*cookie=*/12);
        fx.Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>(fx.SchemeCacheEdge, WAIT_TIMEOUT);

        fx.Runtime.Send(new IEventHandle(fx.StateActor, fx.Sender, new TEvents::TEvPoison()));

        auto ready = fx.Runtime.GrabEdgeEvent<TEvWorkloadManagerReady>(subscriber, WAIT_TIMEOUT);
        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(ready->Get()->Cookie, 12u);
        UNIT_ASSERT_EQUAL(ready->Get()->Status, Ydb::StatusIds::SUCCESS);

        auto info = fx.EnsureReady("/Root/db1");
        UNIT_ASSERT(info.State == EReadyState::Disabled);
    }

    // Resource pools disabled: the state actor replies SUCCESS to a subscriber at once, without a fetch.
    Y_UNIT_TEST(TestSubscribeWhenPoolsDisabled) {
        TFixture fx;
        fx.Init(/*enableResourcePools=*/false);

        const TActorId subscriber = fx.Runtime.AllocateEdgeActor();
        fx.SubscribeOnReady("/Root/db1", subscriber, /*cookie=*/13);

        auto ready = fx.Runtime.GrabEdgeEvent<TEvWorkloadManagerReady>(subscriber, WAIT_TIMEOUT);
        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(ready->Get()->Cookie, 13u);
        UNIT_ASSERT_EQUAL(ready->Get()->Status, Ydb::StatusIds::SUCCESS);
    }

    // Resource pools switched off while a subscriber waits. The state actor:
    // - releases the subscriber with SUCCESS,
    // - publishes pools disabled before the reply, so EnsureReady returns Disabled.
    Y_UNIT_TEST(TestPoolsDisabledReleasesSubscribers) {
        TFixture fx;
        fx.Init();

        const TActorId subscriber = fx.Runtime.AllocateEdgeActor();
        fx.SubscribeOnReady("/Root/db1", subscriber, /*cookie=*/14);
        fx.Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>(fx.SchemeCacheEdge, WAIT_TIMEOUT);

        fx.SetResourcePoolsEnabled(false);

        auto ready = fx.Runtime.GrabEdgeEvent<TEvWorkloadManagerReady>(subscriber, WAIT_TIMEOUT);
        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(ready->Get()->Cookie, 14u);
        UNIT_ASSERT_EQUAL(ready->Get()->Status, Ydb::StatusIds::SUCCESS);

        auto info = fx.EnsureReady("/Root/db1");
        UNIT_ASSERT(info.State == EReadyState::Disabled);
    }
}

}
