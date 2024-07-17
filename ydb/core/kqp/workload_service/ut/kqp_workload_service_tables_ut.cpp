#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/path.h>

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/workload_service/kqp_workload_service.h>
#include <ydb/core/kqp/workload_service/common/events.h>
#include <ydb/core/kqp/workload_service/tables/table_queries.h>
#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>


namespace NKikimr::NKqp {

namespace {

using namespace NActors;
using namespace NWorkload;


void CheckPoolDescription(TIntrusivePtr<IYdbSetup> ydb, ui64 expectedQueueSize, ui64 expectedInFlight, TDuration leaseDuration = FUTURE_WAIT_TIMEOUT) {
    const auto& description = ydb->GetPoolDescription(leaseDuration);
    UNIT_ASSERT_VALUES_EQUAL(description.DelayedRequests, expectedQueueSize);
    UNIT_ASSERT_VALUES_EQUAL(description.RunningRequests, expectedInFlight);
}

void DelayRequest(TIntrusivePtr<IYdbSetup> ydb, const TString& sessionId, TDuration leaseDuration = FUTURE_WAIT_TIMEOUT) {
    const auto& settings = ydb->GetSettings();
    auto runtime = ydb->GetRuntime();
    const auto& edgeActor = runtime->AllocateEdgeActor();

    runtime->Register(CreateDelayRequestActor(edgeActor, settings.DomainName_, settings.PoolId_, sessionId, TInstant::Now(), Nothing(), leaseDuration, runtime->GetAppData().Counters));
    auto response = runtime->GrabEdgeEvent<TEvPrivate::TEvDelayRequestResponse>(edgeActor, FUTURE_WAIT_TIMEOUT);
    UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Status, Ydb::StatusIds::SUCCESS, response->Get()->Issues.ToOneLineString());
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->SessionId, sessionId);
}

void StartRequest(TIntrusivePtr<IYdbSetup> ydb, const TString& sessionId, TDuration leaseDuration = FUTURE_WAIT_TIMEOUT) {
    const auto& settings = ydb->GetSettings();
    auto runtime = ydb->GetRuntime();
    const auto& edgeActor = runtime->AllocateEdgeActor();

    runtime->Register(CreateStartRequestActor(edgeActor, settings.DomainName_, settings.PoolId_, sessionId, leaseDuration, runtime->GetAppData().Counters));
    auto response = runtime->GrabEdgeEvent<TEvPrivate::TEvStartRequestResponse>(edgeActor, FUTURE_WAIT_TIMEOUT);
    UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Status, Ydb::StatusIds::SUCCESS, response->Get()->Issues.ToOneLineString());
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->SessionId, sessionId);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->NodeId, runtime->GetNodeId());
}

}  // anonymous namespace

Y_UNIT_TEST_SUITE(KqpWorkloadServiceTables) {
    Y_UNIT_TEST(TestCreateWorkloadSerivceTables) {
        constexpr ui32 nodesCount = 5;
        auto ydb = TYdbSetupSettings()
            .NodeCount(nodesCount)
            .ConcurrentQueryLimit(nodesCount)
            .Create();

        std::vector<TQueryRunnerResultAsync> asyncResults;
        asyncResults.reserve(nodesCount);
        for (size_t i = 0; i < nodesCount; ++i) {
            asyncResults.emplace_back(ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().NodeIndex(i)));
        }

        for (const auto& result : asyncResults) {
            TSampleQueries::TSelect42::CheckResult(result.GetResult());
        }

        // Check that tables in .metadata/workload_manager created
        auto listResult = ydb->GetSchemeClient().ListDirectory(
            CanonizePath({ydb->GetSettings().DomainName_, ".metadata/workload_manager"})
        ).GetValue(FUTURE_WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(listResult.GetStatus(), NYdb::EStatus::SUCCESS, listResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(listResult.GetChildren().size(), 2);
    }

    Y_UNIT_TEST(TestTablesIsNotCreatingForUnlimitedPool) {
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(-1)
            .QueueSize(10)
            .Create();

        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query));

        // Check that there is no .metadata folder
        auto listResult = ydb->GetSchemeClient().ListDirectory(
            CanonizePath(ydb->GetSettings().DomainName_)
        ).GetValue(FUTURE_WAIT_TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL_C(listResult.GetStatus(), NYdb::EStatus::SUCCESS, listResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(listResult.GetChildren().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(listResult.GetChildren()[0].Name, ".resource_pools");
        UNIT_ASSERT_VALUES_EQUAL(listResult.GetChildren()[1].Name, ".sys");
    }

    Y_UNIT_TEST(TestPoolStateFetcherActor) {
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(1)
            .Create();

        // Create tables
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query));

        CheckPoolDescription(ydb, 0, 0);

        auto request = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().HangUpDuringExecution(true));
        ydb->WaitQueryExecution(request);
        CheckPoolDescription(ydb, 0, 1);

        DelayRequest(ydb, "test_session");
        CheckPoolDescription(ydb, 1, 1);
    }

    Y_UNIT_TEST(TestCleanupOnServiceRestart) {
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(1)
            .Create();

        // Create tables
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query));

        StartRequest(ydb, "test_session", 2 * FUTURE_WAIT_TIMEOUT);
        DelayRequest(ydb, "test_session",  2 * FUTURE_WAIT_TIMEOUT);
        CheckPoolDescription(ydb, 1, 1);

        // Restart workload service
        ydb->StopWorkloadService();
        auto runtime = ydb->GetRuntime();
        runtime->Register(CreateKqpWorkloadService(runtime->GetAppData().Counters));

        // Check that tables will be cleanuped
        ydb->WaitPoolState({.DelayedRequests = 0, .RunningRequests = 0});
    }

    Y_UNIT_TEST(TestLeaseExpiration) {
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(1)
            .QueryCancelAfter(TDuration::Zero())
            .Create();

        // Create tables
        auto hangingRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().HangUpDuringExecution(true));
        ydb->WaitQueryExecution(hangingRequest);

        auto delayedRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().ExecutionExpected(false));
        ydb->WaitPoolState({.DelayedRequests = 1, .RunningRequests = 1});

        ydb->StopWorkloadService();
        ydb->WaitPoolHandlersCount(0);

        // Check that lease expired
        IYdbSetup::WaitFor(TDuration::Seconds(60), "lease expiration", [ydb](TString& errorString) {
            auto description = ydb->GetPoolDescription(TDuration::Zero());

            errorString = TStringBuilder() << "delayed = " << description.DelayedRequests << ", running = " << description.RunningRequests;
            return description.AmountRequests() == 0;
        });
    }

    Y_UNIT_TEST(TestLeaseUpdates) {
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(1)
            .Create();

        // Create tables
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query));
        ydb->StopWorkloadService();
        ydb->WaitPoolHandlersCount(0);

        const TDuration leaseDuration = TDuration::Seconds(10);
        StartRequest(ydb, "test_session", leaseDuration);
        DelayRequest(ydb, "test_session",  leaseDuration);
        CheckPoolDescription(ydb, 1, 1, leaseDuration);

        // Wait lease duration time and check state
        Sleep(2 * leaseDuration / 3);
        CheckPoolDescription(ydb, 1, 1);

        Sleep(2 * leaseDuration / 3);
        CheckPoolDescription(ydb, 1, 1);
    }
}

}  // namespace NKikimr::NKqp
