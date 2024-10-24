#include <ydb/core/base/appdata_fwd.h>

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/workload_service/actors/actors.h>
#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>


namespace NKikimr::NKqp {

namespace {

using namespace NWorkload;


TEvPrivate::TEvFetchPoolResponse::TPtr FetchPool(TIntrusivePtr<IYdbSetup> ydb, const TString& poolId = "", const TString& userSID = "user@" BUILTIN_SYSTEM_DOMAIN) {
    const auto& settings = ydb->GetSettings();
    auto runtime = ydb->GetRuntime();
    const auto& edgeActor = runtime->AllocateEdgeActor();

    auto userToken = MakeIntrusive<NACLib::TUserToken>(userSID, TVector<NACLib::TSID>{});
    userToken->SaveSerializationInfo();
    runtime->Register(CreatePoolFetcherActor(edgeActor, settings.DomainName_, poolId ? poolId : settings.PoolId_, userToken));
    return runtime->GrabEdgeEvent<TEvPrivate::TEvFetchPoolResponse>(edgeActor, FUTURE_WAIT_TIMEOUT);
}

TEvPrivate::TEvCpuLoadResponse::TPtr FetchCpuInfo(TIntrusivePtr<IYdbSetup> ydb) {
    auto runtime = ydb->GetRuntime();
    const auto& edgeActor = runtime->AllocateEdgeActor();

    runtime->Register(CreateCpuLoadFetcherActor(edgeActor));
    return runtime->GrabEdgeEvent<TEvPrivate::TEvCpuLoadResponse>(edgeActor, FUTURE_WAIT_TIMEOUT);
}

}  // anonymous namespace

Y_UNIT_TEST_SUITE(KqpWorkloadServiceActors) {
    Y_UNIT_TEST(TestPoolFetcher) {
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(1)
            .QueueSize(2)
            .QueryCancelAfter(TDuration::Seconds(10))
            .QueryMemoryLimitPercentPerNode(15)
            .Create();

        const auto& response = FetchPool(ydb);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Status, Ydb::StatusIds::SUCCESS, response->Get()->Issues.ToOneLineString());

        const auto& poolConfig = response->Get()->PoolConfig;
        const auto& settings = ydb->GetSettings();
        UNIT_ASSERT_VALUES_EQUAL(poolConfig.ConcurrentQueryLimit, settings.ConcurrentQueryLimit_);
        UNIT_ASSERT_VALUES_EQUAL(poolConfig.QueueSize, settings.QueueSize_);
        UNIT_ASSERT_VALUES_EQUAL(poolConfig.QueryCancelAfter, settings.QueryCancelAfter_);
        UNIT_ASSERT_VALUES_EQUAL(poolConfig.QueryMemoryLimitPercentPerNode, settings.QueryMemoryLimitPercentPerNode_);
    }

    Y_UNIT_TEST(TestPoolFetcherAclValidation) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& userSID = "user@test";
        TSampleQueries::CheckSuccess(ydb->ExecuteQuery(TStringBuilder() << R"(
            GRANT DESCRIBE SCHEMA ON `/Root/.metadata/workload_manager/pools/)" << ydb->GetSettings().PoolId_ << "` TO `" << userSID << "`;"
        ));
        ydb->WaitPoolAccess(userSID, NACLib::EAccessRights::DescribeSchema);

        auto failedResponse = FetchPool(ydb, ydb->GetSettings().PoolId_, userSID);
        UNIT_ASSERT_VALUES_EQUAL_C(failedResponse->Get()->Status, Ydb::StatusIds::UNAUTHORIZED, failedResponse->Get()->Issues.ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(failedResponse->Get()->Issues.ToString(), TStringBuilder() << "You don't have access permissions for resource pool " << ydb->GetSettings().PoolId_);

        TSampleQueries::CheckSuccess(ydb->ExecuteQuery(TStringBuilder() << R"(
            GRANT SELECT ROW ON `/Root/.metadata/workload_manager/pools/)" << ydb->GetSettings().PoolId_ << "` TO `" << userSID << "`;"
        ));
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
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID)));

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
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings.UserSID(ydb->GetRuntime()->GetAppData().AllAuthenticatedUsers)));
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
                QUERY_MEMORY_LIMIT_PERCENT_PER_NODE=1
            );
        )", settings));

        // Check drop access
        TSampleQueries::CheckSuccess(ydb->ExecuteQuery(TStringBuilder() << R"(
            DROP RESOURCE POOL )" << NResourcePool::DEFAULT_POOL_ID << ";"
        , settings));
    }

    Y_UNIT_TEST(TestCpuLoadActor) {
        const ui32 nodeCount = 5;
        auto ydb = TYdbSetupSettings()
            .NodeCount(nodeCount)
            .Create();

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
    TActorId SubscribeOnPool(TIntrusivePtr<IYdbSetup> ydb) {
        const auto& settings = ydb->GetSettings();
        auto& runtime = *ydb->GetRuntime();
        const auto& edgeActor = runtime.AllocateEdgeActor();

        runtime.Send(MakeKqpWorkloadServiceId(runtime.GetNodeId()), edgeActor, new TEvSubscribeOnPoolChanges(settings.DomainName_, settings.PoolId_));
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
            .QueryMemoryLimitPercentPerNode(55.0)
            .DatabaseLoadCpuThreshold(30.0)
            .Create();

        SubscribeOnPool(ydb);
    }

    Y_UNIT_TEST(TestResourcePoolSubscriptionAfterAlter) {
        auto ydb = TYdbSetupSettings().Create();

        const auto& subscriber = SubscribeOnPool(ydb);

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            ALTER RESOURCE POOL )" << ydb->GetSettings().PoolId_ << R"( SET (
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
            GRANT ALL ON `/Root/.metadata/workload_manager/pools/)" << ydb->GetSettings().PoolId_ << R"(` TO `)" << userSID << R"(`;
        )");

        const auto& response = ydb->GetRuntime()->GrabEdgeEvent<TEvUpdatePoolInfo>(subscriber, FUTURE_WAIT_TIMEOUT);
        UNIT_ASSERT_C(response, "Subscription update not found");

        const auto& securityObject = response->Get()->SecurityObject;
        UNIT_ASSERT_C(securityObject, "Security object not found");

        NACLib::TUserToken token("", userSID, {});
        UNIT_ASSERT_C(securityObject->CheckAccess(NACLib::GenericFull, token), TStringBuilder() << "Unexpected pool access rights: " << securityObject->ToString());
    }

    Y_UNIT_TEST(TestResourcePoolSubscriptionAfterDrop) {
        auto ydb = TYdbSetupSettings().Create();

        const auto& subscriber = SubscribeOnPool(ydb);

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            DROP RESOURCE POOL )" << ydb->GetSettings().PoolId_ << R"(;
        )");

        const auto& response = ydb->GetRuntime()->GrabEdgeEvent<TEvUpdatePoolInfo>(subscriber, FUTURE_WAIT_TIMEOUT);
        UNIT_ASSERT_C(response, "Subscription update not found");
        UNIT_ASSERT_C(!response->Get()->Config, "Unexpected pool config");
        UNIT_ASSERT_C(!response->Get()->SecurityObject, "Unexpected security object");
    }
}

}  // namespace NKikimr::NKqp
