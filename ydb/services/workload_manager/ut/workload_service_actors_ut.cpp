#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <ydb/services/workload_manager/actors/actors.h>
#include <ydb/services/workload_manager/service/service.h>
#include <ydb/services/workload_manager/service/workload_service_impl.h>
#include <ydb/services/workload_manager/ut/common/workload_service_ut_common.h>

namespace NKikimr::NWorkloadManager {

namespace {

using namespace NWorkloadManager;

TEvPrivate::TEvFetchPoolResponse::TPtr FetchPool(
    TIntrusivePtr<IYdbSetup> ydb, const TString& poolId = "", const TString& userSID = "user@" BUILTIN_SYSTEM_DOMAIN) {
    const auto& settings = ydb->GetSettings();
    auto runtime = ydb->GetRuntime();
    const auto& edgeActor = runtime->AllocateEdgeActor();

    auto userToken = MakeIntrusive<NACLib::TUserToken>(userSID, TVector<NACLib::TSID>{});
    userToken->SaveSerializationInfo();
    runtime->Register(CreatePoolFetcherActor(
        edgeActor, settings.DomainName_, poolId ? poolId : settings.PoolId_, userToken, NKikimrConfig::TWorkloadManagerConfig{}));
    return runtime->GrabEdgeEvent<TEvPrivate::TEvFetchPoolResponse>(edgeActor, FUTURE_WAIT_TIMEOUT);
}

TEvPrivate::TEvCpuLoadResponse::TPtr FetchCpuInfo(TIntrusivePtr<IYdbSetup> ydb) {
    auto runtime = ydb->GetRuntime();
    const auto& edgeActor = runtime->AllocateEdgeActor();

    runtime->Register(CreateCpuLoadFetcherActor(edgeActor));
    return runtime->GrabEdgeEvent<TEvPrivate::TEvCpuLoadResponse>(edgeActor, FUTURE_WAIT_TIMEOUT);
}

}   // anonymous namespace

Y_UNIT_TEST_SUITE(KqpWorkloadServiceActors) {
    Y_UNIT_TEST(TestPoolFetcher) {
        auto ydb = TYdbSetupSettings().ConcurrentQueryLimit(1).QueueSize(2).QueryCancelAfter(TDuration::Seconds(10)).Create();

        const auto& response = FetchPool(ydb);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Status, Ydb::StatusIds::SUCCESS, response->Get()->Issues.ToOneLineString());

        const auto& poolConfig = response->Get()->PoolConfig;
        const auto& settings = ydb->GetSettings();
        UNIT_ASSERT_VALUES_EQUAL(poolConfig.ConcurrentQueryLimit, settings.ConcurrentQueryLimit_);
        UNIT_ASSERT_VALUES_EQUAL(poolConfig.QueueSize, settings.QueueSize_);
        UNIT_ASSERT_VALUES_EQUAL(poolConfig.QueryCancelAfter, settings.QueryCancelAfter_);
    }

    Y_UNIT_TEST(TestPoolFetcherAclValidation) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& userSID = "user@test";
        TSampleQueries::CheckSuccess(ydb->ExecuteQuery(TStringBuilder() << R"(
            GRANT DESCRIBE SCHEMA ON `/Root/.metadata/workload_manager/pools/)"
                                                                        << ydb->GetSettings().PoolId_ << "` TO `" << userSID << "`;"));
        ydb->WaitPoolAccess(userSID, NACLib::EAccessRights::DescribeSchema);

        auto failedResponse = FetchPool(ydb, ydb->GetSettings().PoolId_, userSID);
        UNIT_ASSERT_VALUES_EQUAL_C(failedResponse->Get()->Status, Ydb::StatusIds::UNAUTHORIZED, failedResponse->Get()->Issues.ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(failedResponse->Get()->Issues.ToString(),
            TStringBuilder() << "You don't have access permissions for resource pool " << ydb->GetSettings().PoolId_);

        TSampleQueries::CheckSuccess(ydb->ExecuteQuery(TStringBuilder() << R"(
            GRANT SELECT ROW ON `/Root/.metadata/workload_manager/pools/)"
                                                                        << ydb->GetSettings().PoolId_ << "` TO `" << userSID << "`;"));
        ydb->WaitPoolAccess(userSID, NACLib::EAccessRights::SelectRow);

        auto successResponse = FetchPool(ydb, ydb->GetSettings().PoolId_, userSID);
        UNIT_ASSERT_VALUES_EQUAL_C(successResponse->Get()->Status, Ydb::StatusIds::SUCCESS, successResponse->Get()->Issues.ToOneLineString());
    }

    Y_UNIT_TEST(TestPoolFetcherNotExistingPool) {
        auto ydb = TYdbSetupSettings().Create();

        auto response = FetchPool(ydb, "another_pool_id");
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Status, Ydb::StatusIds::NOT_FOUND, response->Get()->Issues.ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(response->Get()->Issues.ToString(), TStringBuilder() << "Resource pool another_pool_id not found");
    }

    Y_UNIT_TEST(TestCreateDefaultPool) {
        auto ydb = TYdbSetupSettings().Create();

        const TString path = TStringBuilder() << ".metadata/workload_manager/pools/" << NResourcePool::DEFAULT_POOL_ID;
        auto response = ydb->Navigate(path, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        UNIT_ASSERT_VALUES_EQUAL(response->ErrorCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(response->ResultSet.at(0).Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown);

        // Create default pool
        TSampleQueries::TSelect42::CheckResult(
            ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID)));

        // Check that default pool created
        response = ydb->Navigate(path, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        UNIT_ASSERT_VALUES_EQUAL(response->ResultSet.at(0).Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindResourcePool);
    }

    Y_UNIT_TEST(TestDefaultPoolUsePermissions) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& userSID = "user@test";
        ydb->GetRuntime()->GetAppData().DefaultUserSIDs.emplace_back(userSID);

        // Create default pool
        auto settings = TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID);
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings));

        // Check default pool access
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings.UserSID(userSID)));
        TSampleQueries::TSelect42::CheckResult(
            ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings.UserSID(ydb->GetRuntime()->GetAppData().AllAuthenticatedUsers)));
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings.UserSID(BUILTIN_ACL_ROOT)));
    }

    Y_UNIT_TEST(TestDefaultPoolAdminPermissions) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& userSID = "user@test";
        ydb->GetRuntime()->GetAppData().AdministrationAllowedSIDs.emplace_back(userSID);

        // Create default pool
        auto settings = TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID);
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings));

        // Check default pool access
        settings.UserSID(userSID);
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings));

        // Check alter access
        TSampleQueries::CheckSuccess(ydb->ExecuteQuery(TStringBuilder() << R"(
            ALTER RESOURCE POOL )" << NResourcePool::DEFAULT_POOL_ID << R"( SET (
                TOTAL_CPU_LIMIT_PERCENT_PER_NODE=10
            );
        )", settings));

        // Check drop access
        TSampleQueries::CheckSuccess(ydb->ExecuteQuery(TStringBuilder() << R"(
            DROP RESOURCE POOL )" << NResourcePool::DEFAULT_POOL_ID << ";", settings));
    }

    Y_UNIT_TEST(TestCpuLoadActor) {
        const ui32 nodeCount = 5;
        auto ydb = TYdbSetupSettings().NodeCount(nodeCount).Create();

        auto response = FetchCpuInfo(ydb);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Status, Ydb::StatusIds::NOT_FOUND, response->Get()->Issues.ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(response->Get()->Issues.ToString(), "Cpu info not found");

        const double usage = 0.25;
        const ui32 threads = 2;
        for (size_t nodeIndex = 0; nodeIndex < nodeCount; ++nodeIndex) {
            ydb->UpdateNodeCpuInfo(usage, threads, nodeIndex);
        }

        response = FetchCpuInfo(ydb);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Status, Ydb::StatusIds::SUCCESS, response->Get()->Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->CpuNumber, threads * nodeCount);
        UNIT_ASSERT_DOUBLES_EQUAL(response->Get()->InstantLoad, usage, 0.01);
    }
}

Y_UNIT_TEST_SUITE(KqpWorkloadServiceSubscriptions) {
    struct TSubscriptionFixture {
        TTestBasicRuntime Runtime;
        bool EnabledResourcePoolsOnServerless = false;
        NKikimrConfig::TWorkloadManagerConfig WorkloadManagerConfig;
        TDatabaseState Database{
            .SelfId = {},
            .EnabledResourcePoolsOnServerless = EnabledResourcePoolsOnServerless,
            .WorkloadManagerConfig = WorkloadManagerConfig,
        };
        const TString DatabaseId = "/Root";
        const TString PoolId = "test_pool";
        TActorId PoolHandler;

        TSubscriptionFixture() {
            Runtime.Initialize(TAppPrepare().Unwrap());
            Database.SelfId = Runtime.AllocateEdgeActor();
            Runtime.EnableScheduleForActor(Database.SelfId);
            Runtime.RegisterService(MakeSchemeCacheID(), Runtime.AllocateEdgeActor());
            PoolHandler = Runtime.AllocateEdgeActor();
        }

        TActorId Subscribe() {
            const auto subscriber = Runtime.AllocateEdgeActor();
            Runtime.Send(Database.SelfId, subscriber, new TEvSubscribeOnPoolChanges(DatabaseId, PoolId));
            auto ev = Runtime.GrabEdgeEvent<TEvSubscribeOnPoolChanges>(Database.SelfId, FUTURE_WAIT_TIMEOUT);
            Runtime.RunCall([&] {
                Database.DoSubscribeRequest(std::move(ev));
                return true;
            });
            return subscriber;
        }

        TActorId Fetcher() const {
            return Database.PendingSubscriptions.at(PoolId).Fetcher;
        }

        void Reply(Ydb::StatusIds::StatusCode status, TActorId fetcher = {}) {
            Runtime.Send(Database.SelfId, fetcher ? fetcher : Fetcher(),
                new TEvPrivate::TEvFetchPoolResponse(status, DatabaseId, PoolId, {}, TPathId(1, 42), {}));
            auto ev = Runtime.GrabEdgeEvent<TEvPrivate::TEvFetchPoolResponse>(Database.SelfId, FUTURE_WAIT_TIMEOUT);
            Runtime.RunCall([&] {
                Database.UpdatePoolInfo(ev, status == Ydb::StatusIds::SUCCESS ? PoolHandler : TActorId{});
                return true;
            });
        }

        TEvPrivate::TEvRetryPoolSubscription::TPtr Retry() {
            auto ev = Runtime.GrabEdgeEvent<TEvPrivate::TEvRetryPoolSubscription>(Database.SelfId, FUTURE_WAIT_TIMEOUT);
            UNIT_ASSERT(ev);
            Runtime.RunCall([&] {
                Database.RetryPoolSubscription(*ev->Get());
                return true;
            });
            return ev;
        }
    };

    Y_UNIT_TEST(TestResourcePoolSubscriptionRetriesFetchErrors) {
        TSubscriptionFixture fixture;
        auto rejectRemoval = fixture.Runtime.AddObserver<TEvUpdatePoolInfo>([](auto&) {
            UNIT_FAIL("A fetch error must not notify subscribers that the pool was removed");
        });
        const auto firstSubscriber = fixture.Subscribe();
        const auto firstFetcher = fixture.Fetcher();
        fixture.Reply(Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Database.PendingSubscriptions.size(), 1);

        // A subscriber arriving during backoff joins the same fetch.
        const auto secondSubscriber = fixture.Subscribe();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Fetcher(), firstFetcher);
        fixture.Retry();
        UNIT_ASSERT(fixture.Fetcher() != firstFetcher);

        for (const auto status : {Ydb::StatusIds::INTERNAL_ERROR, Ydb::StatusIds::UNAUTHORIZED, Ydb::StatusIds::BAD_REQUEST}) {
            const auto fetcher = fixture.Fetcher();
            fixture.Reply(status);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Database.PendingSubscriptions.size(), 1);
            fixture.Retry();
            UNIT_ASSERT(fixture.Fetcher() != fetcher);
        }

        fixture.Reply(Ydb::StatusIds::SUCCESS);
        auto update = fixture.Runtime.GrabEdgeEvent<TEvPrivate::TEvUpdatePoolSubscription>(fixture.PoolHandler, FUTURE_WAIT_TIMEOUT);
        UNIT_ASSERT(update);
        UNIT_ASSERT_VALUES_EQUAL(update->Get()->PathId, TPathId(1, 42));
        UNIT_ASSERT_VALUES_EQUAL(update->Get()->Subscribers.size(), 2);
        UNIT_ASSERT(update->Get()->Subscribers.contains(firstSubscriber));
        UNIT_ASSERT(update->Get()->Subscribers.contains(secondSubscriber));
        UNIT_ASSERT(fixture.Database.PendingSubscriptions.empty());
    }

    Y_UNIT_TEST(TestResourcePoolSubscriptionNotFoundAfterFetchError) {
        TSubscriptionFixture fixture;
        const auto subscriber = fixture.Subscribe();
        fixture.Reply(Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Database.PendingSubscriptions.size(), 1);
        fixture.Retry();
        fixture.Reply(Ydb::StatusIds::NOT_FOUND);

        auto update = fixture.Runtime.GrabEdgeEvent<TEvUpdatePoolInfo>(subscriber, FUTURE_WAIT_TIMEOUT);
        UNIT_ASSERT(update);
        UNIT_ASSERT_VALUES_EQUAL(update->Get()->DatabaseId, fixture.DatabaseId);
        UNIT_ASSERT_VALUES_EQUAL(update->Get()->PoolId, fixture.PoolId);
        UNIT_ASSERT(!update->Get()->Config);
        UNIT_ASSERT(!update->Get()->SecurityObject);
        UNIT_ASSERT(fixture.Database.PendingSubscriptions.empty());
    }

    Y_UNIT_TEST(TestResourcePoolSubscriptionIgnoresStaleFetchAndRetry) {
        TSubscriptionFixture fixture;
        fixture.Subscribe();
        const auto oldFetcher = fixture.Fetcher();
        fixture.Reply(Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Database.PendingSubscriptions.size(), 1);
        auto retry = fixture.Retry();
        fixture.Reply(Ydb::StatusIds::NOT_FOUND);

        const auto subscriber = fixture.Subscribe();
        const auto newFetcher = fixture.Fetcher();
        fixture.Runtime.RunCall([&] {
            fixture.Database.RetryPoolSubscription(*retry->Get());
            return true;
        });
        fixture.Reply(Ydb::StatusIds::NOT_FOUND, oldFetcher);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Fetcher(), newFetcher);

        fixture.Reply(Ydb::StatusIds::SUCCESS);
        auto update = fixture.Runtime.GrabEdgeEvent<TEvPrivate::TEvUpdatePoolSubscription>(fixture.PoolHandler, FUTURE_WAIT_TIMEOUT);
        UNIT_ASSERT(update);
        UNIT_ASSERT_VALUES_EQUAL(update->Get()->Subscribers.size(), 1);
        UNIT_ASSERT(update->Get()->Subscribers.contains(subscriber));
    }

    TActorId SubscribeOnPool(TIntrusivePtr<IYdbSetup> ydb) {
        const auto& settings = ydb->GetSettings();
        auto& runtime = *ydb->GetRuntime();
        const auto& edgeActor = runtime.AllocateEdgeActor();

        runtime.Send(MakeServiceId(runtime.GetNodeId()), edgeActor, new TEvSubscribeOnPoolChanges(settings.DomainName_, settings.PoolId_));
        const auto& response = runtime.GrabEdgeEvent<TEvUpdatePoolInfo>(edgeActor, FUTURE_WAIT_TIMEOUT);
        UNIT_ASSERT_C(response, "Subscription update not found");

        const auto& config = response->Get()->Config;
        UNIT_ASSERT_C(config, "Pool config not found");
        UNIT_ASSERT_C(*config == settings.GetDefaultPoolSettings(), "Unexpected pool config");

        const auto& securityObject = response->Get()->SecurityObject;
        UNIT_ASSERT_C(securityObject, "Security object not found");
        UNIT_ASSERT_VALUES_EQUAL_C(securityObject->GetOwnerSID(), BUILTIN_ACL_ROOT, "Unexpected owner user SID");

        return edgeActor;
    }

    Y_UNIT_TEST(TestResourcePoolSubscription) {
        auto ydb = TYdbSetupSettings()
                       .QueueSize(10)
                       .ConcurrentQueryLimit(5)
                       .QueryCancelAfter(TDuration::Seconds(42))
                       .DatabaseLoadCpuThreshold(30.0)
                       .Create();

        SubscribeOnPool(ydb);
    }

    Y_UNIT_TEST(TestResourcePoolSubscriptionAfterAlter) {
        auto ydb = TYdbSetupSettings().Create();

        const auto& subscriber = SubscribeOnPool(ydb);

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            ALTER RESOURCE POOL )" << ydb->GetSettings().PoolId_
                                                 << R"( SET (
                CONCURRENT_QUERY_LIMIT=42
            );
        )");

        const auto& response = ydb->GetRuntime()->GrabEdgeEvent<TEvUpdatePoolInfo>(subscriber, FUTURE_WAIT_TIMEOUT);
        UNIT_ASSERT_C(response, "Subscription update not found");

        const auto& config = response->Get()->Config;
        UNIT_ASSERT_C(config, "Pool config not found");
        UNIT_ASSERT_VALUES_EQUAL(config->ConcurrentQueryLimit, 42);
    }

    Y_UNIT_TEST(TestResourcePoolSubscriptionAfterAclChange) {
        auto ydb = TYdbSetupSettings().Create();

        const auto& subscriber = SubscribeOnPool(ydb);

        const TString& userSID = "test@user";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            GRANT ALL ON `/Root/.metadata/workload_manager/pools/)"
                                                 << ydb->GetSettings().PoolId_ << R"(` TO `)" << userSID << R"(`;
        )");

        const auto& response = ydb->GetRuntime()->GrabEdgeEvent<TEvUpdatePoolInfo>(subscriber, FUTURE_WAIT_TIMEOUT);
        UNIT_ASSERT_C(response, "Subscription update not found");

        const auto& securityObject = response->Get()->SecurityObject;
        UNIT_ASSERT_C(securityObject, "Security object not found");

        NACLib::TUserToken token("", userSID, {});
        UNIT_ASSERT_C(securityObject->CheckAccess(NACLib::GenericFull, token),
            TStringBuilder() << "Unexpected pool access rights: " << securityObject->ToString());
    }

    Y_UNIT_TEST(TestResourcePoolSubscriptionAfterDrop) {
        auto ydb = TYdbSetupSettings().Create();

        const auto& subscriber = SubscribeOnPool(ydb);

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            DROP RESOURCE POOL )" << ydb->GetSettings().PoolId_
                                                 << R"(;
        )");

        const auto& response = ydb->GetRuntime()->GrabEdgeEvent<TEvUpdatePoolInfo>(subscriber, FUTURE_WAIT_TIMEOUT);
        UNIT_ASSERT_C(response, "Subscription update not found");
        UNIT_ASSERT_C(!response->Get()->Config, "Unexpected pool config");
        UNIT_ASSERT_C(!response->Get()->SecurityObject, "Unexpected security object");
    }

    // Subscribe on a missing pool: the workload service replies with TEvUpdatePoolInfo without config and security object.
    Y_UNIT_TEST(TestResourcePoolSubscriptionNotExistingPool) {
        auto ydb = TYdbSetupSettings().Create();
        auto& runtime = *ydb->GetRuntime();
        const auto& edgeActor = runtime.AllocateEdgeActor();

        runtime.Send(
            MakeServiceId(runtime.GetNodeId()), edgeActor, new TEvSubscribeOnPoolChanges(ydb->GetSettings().DomainName_, "another_pool_id"));

        const auto& response = runtime.GrabEdgeEvent<TEvUpdatePoolInfo>(edgeActor, FUTURE_WAIT_TIMEOUT);
        UNIT_ASSERT_C(response, "Subscription update not found");
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->PoolId, "another_pool_id");
        UNIT_ASSERT_C(!response->Get()->Config, "Unexpected pool config");
        UNIT_ASSERT_C(!response->Get()->SecurityObject, "Unexpected security object");
    }
}

}   // namespace NKikimr::NWorkloadManager
