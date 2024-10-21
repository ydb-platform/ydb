#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/path.h>

#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>


namespace NKikimr::NKqp {

namespace {

using namespace NWorkload;
using namespace NYdb;


void StartTestConcurrentQueryLimit(const ui64 activeCountLimit, const ui64 queueSize, const ui64 nodeCount = 1) {
    auto ydb = TYdbSetupSettings()
        .NodeCount(nodeCount)
        .ConcurrentQueryLimit(activeCountLimit)
        .QueueSize(queueSize)
        .QueryCancelAfter(FUTURE_WAIT_TIMEOUT * queueSize)
        .Create();

    auto settings = TQueryRunnerSettings()
        .InFlightCoordinatorActorId(ydb->CreateInFlightCoordinator(queueSize, activeCountLimit))
        .HangUpDuringExecution(true);

    // Initialize queue
    std::vector<TQueryRunnerResultAsync> asyncResults;
    for (size_t i = 0; i < queueSize; ++i) {
        asyncResults.emplace_back(ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, settings.NodeIndex(i % nodeCount)));
    }

    for (const auto& asyncResult : asyncResults) {
        TSampleQueries::TSelect42::CheckResult(asyncResult.GetResult());
    }

    ydb->ValidateWorkloadServiceCounters();
}

}  // anonymous namespace

Y_UNIT_TEST_SUITE(KqpWorkloadService) {
    Y_UNIT_TEST(WorkloadServiceDisabledByFeatureFlag) {
        auto ydb = TYdbSetupSettings()
            .EnableResourcePools(false)
            .Create();

        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().PoolId("another_pool_id")));
    }

    Y_UNIT_TEST(WorkloadServiceDisabledByFeatureFlagOnServerless) {
        auto ydb = TYdbSetupSettings()
            .CreateSampleTenants(true)
            .EnableResourcePoolsOnServerless(false)
            .Create();

        const TString& poolId = "another_pool_id";
        auto settings = TQueryRunnerSettings().PoolId(poolId);

        // Dedicated, enabled
        TSampleQueries::CheckNotFound(ydb->ExecuteQuery(
            TSampleQueries::TSelect42::Query,
            settings.Database(ydb->GetSettings().GetDedicatedTenantName()).NodeIndex(1)
        ), poolId);

        // Shared, enabled
        TSampleQueries::CheckNotFound(ydb->ExecuteQuery(
            TSampleQueries::TSelect42::Query,
            settings.Database(ydb->GetSettings().GetSharedTenantName()).NodeIndex(2)
        ), poolId);

        // Serverless, disabled
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(
            TSampleQueries::TSelect42::Query,
            settings.Database(ydb->GetSettings().GetServerlessTenantName()).NodeIndex(2)
        ));
    }

    Y_UNIT_TEST(WorkloadServiceDisabledByInvalidDatabasePath) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& poolId = "another_pool_id";
        auto settings = TQueryRunnerSettings().PoolId(poolId);

        TSampleQueries::CheckNotFound(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings), poolId);

        const TString& tabmleName = "sub_path";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE TABLE )" << tabmleName << R"( (
                Key Int32,
                PRIMARY KEY (Key)
            );
        )");

        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(
            TSampleQueries::TSelect42::Query,
            settings.Database(TStringBuilder() << CanonizePath(ydb->GetSettings().DomainName_) << "/" << tabmleName)
        ));
    }

    TQueryRunnerResultAsync StartQueueSizeCheckRequests(TIntrusivePtr<IYdbSetup> ydb, const TQueryRunnerSettings& settings) {
        // One of these requests should be rejected by QueueSize
        auto firstRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, settings);
        auto secondRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, settings);
        WaitAny(firstRequest.GetFuture(), secondRequest.GetFuture()).GetValue(FUTURE_WAIT_TIMEOUT);

        if (secondRequest.HasValue()) {
            std::swap(firstRequest, secondRequest);
        }
        UNIT_ASSERT_C(firstRequest.HasValue(), "One of two requests shoud be rejected");
        UNIT_ASSERT_C(!secondRequest.HasValue(), "One of two requests shoud be placed in pool");

        auto result = firstRequest.GetResult();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::OVERLOADED, result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), TStringBuilder() << "Request was rejected, number of local pending requests is 2, number of global delayed/running requests is 1, sum of them is larger than allowed limit 1 (including concurrent query limit 1) for pool " << ydb->GetSettings().PoolId_);

        return secondRequest;
    }

    Y_UNIT_TEST(TestQueueSizeSimple) {
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(1)
            .QueueSize(1)
            .Create();

        auto hangingRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().HangUpDuringExecution(true));
        ydb->WaitQueryExecution(hangingRequest);

        auto delayedRequest = StartQueueSizeCheckRequests(ydb, TQueryRunnerSettings().ExecutionExpected(false));

        ydb->ContinueQueryExecution(delayedRequest);
        ydb->ContinueQueryExecution(hangingRequest);
        TSampleQueries::TSelect42::CheckResult(hangingRequest.GetResult());
        TSampleQueries::TSelect42::CheckResult(delayedRequest.GetResult());
    }

    Y_UNIT_TEST(TestQueueSizeManyQueries) {
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(1)
            .QueueSize(1)
            .Create();

        auto settings = TQueryRunnerSettings().HangUpDuringExecution(true);
        auto hangingRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, settings);
        ydb->WaitQueryExecution(hangingRequest);

        const ui64 numberRuns = 5;
        for (size_t i = 0; i < numberRuns; ++i) {
            auto delayedRequest = StartQueueSizeCheckRequests(ydb, settings);

            ydb->ContinueQueryExecution(hangingRequest);
            TSampleQueries::TSelect42::CheckResult(hangingRequest.GetResult());

            hangingRequest = delayedRequest;
            ydb->WaitQueryExecution(hangingRequest);
        }

        ydb->ContinueQueryExecution(hangingRequest);
        TSampleQueries::TSelect42::CheckResult(hangingRequest.GetResult());
    }

    Y_UNIT_TEST(TestZeroQueueSize) {
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(1)
            .QueueSize(0)
            .Create();

        auto hangingRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().HangUpDuringExecution(true));
        ydb->WaitQueryExecution(hangingRequest);

        auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().ExecutionExpected(false));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::OVERLOADED, result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), TStringBuilder() << "Request was rejected, number of local pending requests is 1, number of global delayed/running requests is 1, sum of them is larger than allowed limit 0 (including concurrent query limit 1) for pool " << ydb->GetSettings().PoolId_);

        ydb->ContinueQueryExecution(hangingRequest);
        TSampleQueries::TSelect42::CheckResult(hangingRequest.GetResult());
    }

    Y_UNIT_TEST(TestZeroQueueSizeManyQueries) {
        const i32 inFlight = 10;
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(inFlight)
            .QueueSize(0)
            .QueryCancelAfter(FUTURE_WAIT_TIMEOUT * inFlight)
            .Create();

        auto settings = TQueryRunnerSettings().HangUpDuringExecution(true);

        std::vector<TQueryRunnerResultAsync> asyncResults;
        for (size_t i = 0; i < inFlight; ++i) {
            asyncResults.emplace_back(ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, settings));
        }

        for (const auto& asyncResult : asyncResults) {
            ydb->WaitQueryExecution(asyncResult);
        }

        auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().ExecutionExpected(false));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::OVERLOADED, result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), TStringBuilder() << "Request was rejected, number of local pending requests is 1, number of global delayed/running requests is " << inFlight << ", sum of them is larger than allowed limit 0 (including concurrent query limit " << inFlight << ") for pool " << ydb->GetSettings().PoolId_);

        for (const auto& asyncResult : asyncResults) {
            ydb->ContinueQueryExecution(asyncResult);
            TSampleQueries::TSelect42::CheckResult(asyncResult.GetResult());
        }
    }

    Y_UNIT_TEST(TestQueryCancelAfterUnlimitedPool) {
        auto ydb = TYdbSetupSettings()
            .QueryCancelAfter(TDuration::Seconds(10))
            .Create();

        TSampleQueries::CheckCancelled(ydb->ExecuteQueryAsync(
            TSampleQueries::TSelect42::Query,
            TQueryRunnerSettings().HangUpDuringExecution(true)
        ).GetResult());
    }

    Y_UNIT_TEST(TestQueryCancelAfterPoolWithLimits) {
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(1)
            .QueueSize(1)
            .QueryCancelAfter(TDuration::Seconds(10))
            .Create();

        auto settings = TQueryRunnerSettings().HangUpDuringExecution(true);
        auto hangingRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, settings);
        ydb->WaitQueryExecution(hangingRequest);        

        auto delayedRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, settings);
        TSampleQueries::CheckCancelled(hangingRequest.GetResult());

        auto result = delayedRequest.GetResult();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::CANCELLED, result.GetIssues().ToString());

        // Check that queue is free
        UNIT_ASSERT_VALUES_EQUAL(ydb->GetPoolDescription().AmountRequests(), 0);
    }

    Y_UNIT_TEST(TestStartQueryAfterCancel) {
        const TDuration cancelAfter = TDuration::Seconds(10);
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(1)
            .QueryCancelAfter(cancelAfter)
            .Create();

        auto hangingRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().HangUpDuringExecution(true));
        ydb->WaitQueryExecution(hangingRequest);

        Sleep(cancelAfter / 2);

        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query));
        TSampleQueries::CheckCancelled(hangingRequest.GetResult());
    }

    Y_UNIT_TEST(TestLargeConcurrentQueryLimit) {
        StartTestConcurrentQueryLimit(5, 100);
    }

    Y_UNIT_TEST(TestLessConcurrentQueryLimit) {
        StartTestConcurrentQueryLimit(1, 100);
    }

    Y_UNIT_TEST(TestZeroConcurrentQueryLimit) {
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(0)
            .Create();

        auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), TStringBuilder() << "Resource pool " << ydb->GetSettings().PoolId_ << " was disabled due to zero concurrent query limit");
    }

    Y_UNIT_TEST(TestCpuLoadThreshold) {
        auto ydb = TYdbSetupSettings()
            .DatabaseLoadCpuThreshold(90)
            .QueryCancelAfter(TDuration::Seconds(10))
            .Create();

        // Simulate load
        ydb->UpdateNodeCpuInfo(1.0, 1);

        auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().ExecutionExpected(false));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::CANCELLED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), TStringBuilder() << "Request was delayed during");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), TStringBuilder() << ", that is larger than delay deadline 10.000000s in pool " << ydb->GetSettings().PoolId_ << ", request was canceled");
    }

    Y_UNIT_TEST(TestCpuLoadThresholdRefresh) {
        auto ydb = TYdbSetupSettings()
            .DatabaseLoadCpuThreshold(90)
            .Create();

        // Simulate load
        ydb->UpdateNodeCpuInfo(1.0, 1);

        // Delay request
        auto result = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().ExecutionExpected(false));
        ydb->WaitPoolState({.DelayedRequests = 1, .RunningRequests = 0});

        // Free load
        ydb->ContinueQueryExecution(result);
        ydb->UpdateNodeCpuInfo(0.0, 1);
        TSampleQueries::TSelect42::CheckResult(result.GetResult(TDuration::Seconds(5)));
    }

    Y_UNIT_TEST(TestHandlerActorCleanup) {
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(1)
            .Create();

        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query));
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID)));

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            DROP RESOURCE POOL )" << ydb->GetSettings().PoolId_ << R"(;
            DROP RESOURCE POOL )" << NResourcePool::DEFAULT_POOL_ID << R"(;
        )");

        ydb->WaitPoolHandlersCount(0, std::nullopt, TDuration::Seconds(95));
    }
}

Y_UNIT_TEST_SUITE(KqpWorkloadServiceDistributed) {
    Y_UNIT_TEST(TestDistributedQueue) {
        auto ydb = TYdbSetupSettings()
            .NodeCount(2)
            .ConcurrentQueryLimit(1)
            .QueueSize(1)
            .Create();

        auto hangingRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings()
            .HangUpDuringExecution(true)
            .NodeIndex(0)
        );
        ydb->WaitQueryExecution(hangingRequest);

        auto delayedRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings()
            .ExecutionExpected(false)
            .NodeIndex(1)
        );
        ydb->WaitPoolState({.DelayedRequests = 1, .RunningRequests = 1});

        // Check distributed queue size
        auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().NodeIndex(0));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::OVERLOADED, result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), TStringBuilder() << "Request was rejected, number of local pending requests is 1, number of global delayed/running requests is 2, sum of them is larger than allowed limit 1 (including concurrent query limit 1) for pool " << ydb->GetSettings().PoolId_);

        ydb->ContinueQueryExecution(delayedRequest);
        ydb->ContinueQueryExecution(hangingRequest);
        TSampleQueries::TSelect42::CheckResult(hangingRequest.GetResult());

        // Query should start faster than lease update time
        ydb->WaitQueryExecution(delayedRequest, TDuration::Seconds(5));
        TSampleQueries::TSelect42::CheckResult(delayedRequest.GetResult());
    }

    Y_UNIT_TEST(TestNodeDisconnect) {
        auto ydb = TYdbSetupSettings()
            .NodeCount(2)
            .ConcurrentQueryLimit(1)
            .QueryCancelAfter(TDuration::Minutes(2))
            .Create();

        auto hangingRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings()
            .HangUpDuringExecution(true)
            .NodeIndex(0)
        );
        ydb->WaitQueryExecution(hangingRequest);

        auto delayedRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings()
            .ExecutionExpected(false)
            .NodeIndex(0)
        );
        ydb->WaitPoolState({.DelayedRequests = 1, .RunningRequests = 1});

        auto request = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings()
            .ExecutionExpected(false)
            .NodeIndex(1)
        );
        ydb->WaitPoolState({.DelayedRequests = 2, .RunningRequests = 1});

        ydb->ContinueQueryExecution(request);
        ydb->StopWorkloadService(0);

        // Query should start after lease expiration
        TSampleQueries::TSelect42::CheckResult(request.GetResult(TDuration::Seconds(50)));
    }

    Y_UNIT_TEST(TestDistributedLargeConcurrentQueryLimit) {
        StartTestConcurrentQueryLimit(5, 100, 3);
    }

    Y_UNIT_TEST(TestDistributedLessConcurrentQueryLimit) {
        StartTestConcurrentQueryLimit(1, 100, 5);
    }
}

Y_UNIT_TEST_SUITE(ResourcePoolsDdl) {
    Y_UNIT_TEST(TestCreateResourcePool) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& poolId = "my_pool";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL )" << poolId << R"( WITH (
                CONCURRENT_QUERY_LIMIT=1,
                QUEUE_SIZE=0
            );
        )");

        auto hangingRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings()
            .HangUpDuringExecution(true)
            .PoolId(poolId)
        );
        ydb->WaitQueryExecution(hangingRequest);

        auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().PoolId(poolId));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::OVERLOADED, result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), TStringBuilder() << "Request was rejected, number of local pending requests is 1, number of global delayed/running requests is 1, sum of them is larger than allowed limit 0 (including concurrent query limit 1) for pool " << poolId);

        ydb->ContinueQueryExecution(hangingRequest);
        TSampleQueries::TSelect42::CheckResult(hangingRequest.GetResult());
    }

    Y_UNIT_TEST(TestCreateResourcePoolOnServerless) {
        auto ydb = TYdbSetupSettings()
            .CreateSampleTenants(true)
            .EnableResourcePoolsOnServerless(true)
            .Create();

        const auto& serverlessTenant = ydb->GetSettings().GetServerlessTenantName();
        auto settings = TQueryRunnerSettings()
            .PoolId("")
            .Database(serverlessTenant)
            .NodeIndex(1);

        const TString& poolId = "my_pool";
        TSampleQueries::CheckSuccess(ydb->ExecuteQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL )" << poolId << R"( WITH (
                CONCURRENT_QUERY_LIMIT=1,
                QUEUE_SIZE=0
            );
        )", settings));
        settings.PoolId(poolId);

        auto hangingRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, settings.HangUpDuringExecution(true));
        ydb->WaitQueryExecution(hangingRequest);

        settings.HangUpDuringExecution(false);

        {  // Rejected result
            auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings.PoolId(poolId));
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::OVERLOADED, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), TStringBuilder() << "Request was rejected, number of local pending requests is 1, number of global delayed/running requests is 1, sum of them is larger than allowed limit 0 (including concurrent query limit 1) for pool " << poolId);
        }

        {  // Check tables
            auto result = ydb->ExecuteQuery(R"(
                SELECT * FROM `.metadata/workload_manager/running_requests`
            )", settings.PoolId(NResourcePool::DEFAULT_POOL_ID).Database(ydb->GetSettings().GetSharedTenantName()));
            TSampleQueries::CheckSuccess(result);

            NYdb::TResultSetParser resultSet(result.GetResultSet(0));
            UNIT_ASSERT_C(resultSet.TryNextRow(), "Unexpected row count");

            const auto& databaseId = resultSet.ColumnParser("database").GetOptionalUtf8();
            UNIT_ASSERT_C(databaseId, "Unexpected database response");
            UNIT_ASSERT_VALUES_EQUAL_C(*databaseId, ydb->FetchDatabase(serverlessTenant)->Get()->DatabaseId, "Unexpected database id");
        }

        ydb->ContinueQueryExecution(hangingRequest);
        TSampleQueries::TSelect42::CheckResult(hangingRequest.GetResult());
    }

    Y_UNIT_TEST(TestDefaultPoolRestrictions) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& poolId = NResourcePool::DEFAULT_POOL_ID;
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL )" << poolId << R"( WITH (
                CONCURRENT_QUERY_LIMIT=0
            );
        )", EStatus::GENERIC_ERROR, "Cannot create default pool manually, pool will be created automatically during first request execution");

        // Create default pool
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().PoolId(poolId)));

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            ALTER RESOURCE POOL )" << poolId << R"( SET (
                CONCURRENT_QUERY_LIMIT=0
            );
        )", EStatus::GENERIC_ERROR, "Can not change property concurrent_query_limit for default pool");
    }

    Y_UNIT_TEST(TestAlterResourcePool) {
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(1)
            .Create();

        auto hangingRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().HangUpDuringExecution(true));
        ydb->WaitQueryExecution(hangingRequest);

        auto delayedRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().ExecutionExpected(false));
        ydb->WaitPoolState({.DelayedRequests = 1, .RunningRequests = 1});

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            ALTER RESOURCE POOL )" << ydb->GetSettings().PoolId_ << R"( SET (
                QUEUE_SIZE=0
            );
        )");

        auto result = delayedRequest.GetResult();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::OVERLOADED, result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), TStringBuilder() << "Request was rejected, number of local delayed requests is 1, that is larger than allowed limit 0 for pool " << ydb->GetSettings().PoolId_);

        ydb->ContinueQueryExecution(hangingRequest);
        TSampleQueries::TSelect42::CheckResult(hangingRequest.GetResult());
    }

    Y_UNIT_TEST(TestPoolSwitchToLimitedState) {
        auto ydb = TYdbSetupSettings()
            .Create();

        // Initialize pool
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query));

        // Change pool to limited
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            ALTER RESOURCE POOL )" << ydb->GetSettings().PoolId_ << R"( SET (
                CONCURRENT_QUERY_LIMIT=1
            );
        )");

        // Wait pool change
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query));  // Force pool update
        ydb->WaitPoolHandlersCount(2);

        // Check that pool using tables
        auto hangingRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().HangUpDuringExecution(true));
        ydb->WaitQueryExecution(hangingRequest);
        UNIT_ASSERT_VALUES_EQUAL(ydb->GetPoolDescription().AmountRequests(), 1);

        ydb->ContinueQueryExecution(hangingRequest);
        TSampleQueries::TSelect42::CheckResult(hangingRequest.GetResult());
    }

    Y_UNIT_TEST(TestPoolSwitchToUnlimitedState) {
        auto ydb = TYdbSetupSettings()
            .ConcurrentQueryLimit(1)
            .Create();

        // Initialize pool
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query));

        // Change pool to unlimited
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            ALTER RESOURCE POOL )" << ydb->GetSettings().PoolId_ << R"( RESET (
                CONCURRENT_QUERY_LIMIT
            );
        )");

        // Wait pool change
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query));  // Force pool update
        ydb->WaitPoolHandlersCount(2);

        // Check that pool is not using tables
        auto hangingRequest = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().HangUpDuringExecution(true));
        ydb->WaitQueryExecution(hangingRequest);
        UNIT_ASSERT_VALUES_EQUAL(ydb->GetPoolDescription().AmountRequests(), 0);

        ydb->ContinueQueryExecution(hangingRequest);
        TSampleQueries::TSelect42::CheckResult(hangingRequest.GetResult());
    }

    Y_UNIT_TEST(TestDropResourcePool) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& poolId = "my_pool";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL )" << poolId << R"( WITH (
                CONCURRENT_QUERY_LIMIT=1
            );
        )");

        auto settings = TQueryRunnerSettings().PoolId(poolId);
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings));

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            DROP RESOURCE POOL )" << poolId << ";"
        );

        IYdbSetup::WaitFor(FUTURE_WAIT_TIMEOUT, "pool drop", [ydb, poolId](TString& errorString) {
            auto kind = ydb->Navigate(TStringBuilder() << ".metadata/workload_manager/pools/" << poolId)->ResultSet.at(0).Kind;

            errorString = TStringBuilder() << "kind = " << kind;
            return kind == NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown;
        });

        auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::NOT_FOUND, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), TStringBuilder() << "Resource pool " << poolId << " not found");
    }

    Y_UNIT_TEST(TestResourcePoolAcl) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& poolId = "my_pool";
        const TString& userSID = "user@test";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL )" << poolId << R"( WITH (
                CONCURRENT_QUERY_LIMIT=1
            );
            GRANT DESCRIBE SCHEMA ON `/Root/.metadata/workload_manager/pools/)" << poolId << "` TO `" << userSID << "`;"
        );
        ydb->WaitPoolAccess(userSID, NACLib::EAccessRights::DescribeSchema, poolId);

        auto settings = TQueryRunnerSettings().PoolId(poolId).UserSID(userSID);
        auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::UNAUTHORIZED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), TStringBuilder() << "You don't have access permissions for resource pool " << poolId);

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            GRANT SELECT ROW ON `/Root/.metadata/workload_manager/pools/)" << poolId << "` TO `" << userSID << "`;"
        );
        ydb->WaitPoolAccess(userSID, NACLib::EAccessRights::SelectRow, poolId);
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings));
    }
}

Y_UNIT_TEST_SUITE(ResourcePoolClassifiersDdl) {
    Y_UNIT_TEST(TestResourcePoolClassifiersPermissions) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& userSID = "user@test";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            GRANT DESCRIBE SCHEMA ON `/Root` TO `)" << userSID << R"(`;
            GRANT DESCRIBE SCHEMA, SELECT ROW ON `/Root/.metadata/workload_manager/pools/)" << ydb->GetSettings().PoolId_ << "` TO `" << userSID << "`;"
        );
        ydb->WaitPoolAccess(userSID, NACLib::EAccessRights::DescribeSchema | NACLib::EAccessRights::SelectRow);

        auto settings = TQueryRunnerSettings().UserSID(userSID);

        ydb->WaitFor(TDuration::Seconds(5), "Database permissions", [ydb, settings](TString& errorString) {
            auto result = ydb->ExecuteQuery("DROP RESOURCE POOL CLASSIFIER MyResourcePoolClassifier", settings);

            errorString = result.GetIssues().ToOneLineString();
            return result.GetStatus() == EStatus::GENERIC_ERROR && errorString.Contains("You don't have access permissions for database Root");
        });

        auto createResult = ydb->ExecuteQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL CLASSIFIER MyResourcePoolClassifier WITH (
                RESOURCE_POOL=")" << NResourcePool::DEFAULT_POOL_ID << R"(",
                RANK=20
            );
        )", settings);
        UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::GENERIC_ERROR, createResult.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(createResult.GetIssues().ToOneLineString(), "You don't have access permissions for database Root");

        auto alterResult = ydb->ExecuteQuery(R"(
            ALTER RESOURCE POOL CLASSIFIER MyResourcePoolClassifier SET (
                RANK=20
            );
        )", settings);
        UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::GENERIC_ERROR, alterResult.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(alterResult.GetIssues().ToOneLineString(), "You don't have access permissions for database Root");
    }

    void CreateSampleResourcePoolClassifier(TIntrusivePtr<IYdbSetup> ydb, const TString& classifierId, const TQueryRunnerSettings& settings, const TString& poolId) {
        TSampleQueries::CheckSuccess(ydb->ExecuteQuery(TStringBuilder() << R"(
            GRANT ALL ON `)" << CanonizePath(settings.Database_ ? settings.Database_ : ydb->GetSettings().DomainName_) << R"(` TO `)" << settings.UserSID_ << R"(`;
            CREATE RESOURCE POOL )" << poolId << R"( WITH (
                CONCURRENT_QUERY_LIMIT=0
            );
            CREATE RESOURCE POOL CLASSIFIER )" << classifierId << R"( WITH (
                RESOURCE_POOL=")" << poolId << R"(",
                MEMBER_NAME=")" << settings.UserSID_ << R"("
            );
        )", TQueryRunnerSettings()
            .UserSID(BUILTIN_ACL_METADATA)
            .Database(settings.Database_)
            .NodeIndex(settings.NodeIndex_)
            .PoolId(NResourcePool::DEFAULT_POOL_ID)
        ));
    }

    TString CreateSampleResourcePoolClassifier(TIntrusivePtr<IYdbSetup> ydb, const TQueryRunnerSettings& settings, const TString& poolId) {
        const TString& classifierId = "my_pool_classifier";
        CreateSampleResourcePoolClassifier(ydb, classifierId, settings, poolId);
        return classifierId;
    }

    void WaitForFail(TIntrusivePtr<IYdbSetup> ydb, const TQueryRunnerSettings& settings, const TString& poolId) {
        ydb->WaitFor(TDuration::Seconds(10), "Resource pool classifier fail", [ydb, settings, poolId](TString& errorString) {
            auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings);

            errorString = result.GetIssues().ToOneLineString();
            return result.GetStatus() == EStatus::PRECONDITION_FAILED && errorString.Contains(TStringBuilder() << "Resource pool " << poolId << " was disabled due to zero concurrent query limit");
        });
    }

    void WaitForSuccess(TIntrusivePtr<IYdbSetup> ydb, const TQueryRunnerSettings& settings) {
        ydb->WaitFor(TDuration::Seconds(10), "Resource pool classifier success", [ydb, settings](TString& errorString) {
            auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings);

            errorString = result.GetIssues().ToOneLineString();
            return result.GetStatus() == EStatus::SUCCESS;
        });
    }

    Y_UNIT_TEST(TestCreateResourcePoolClassifier) {
        auto ydb = TYdbSetupSettings().Create();

        auto settings = TQueryRunnerSettings().PoolId("").UserSID("test@user");
        const TString& poolId = "my_pool";
        CreateSampleResourcePoolClassifier(ydb, settings, poolId);

        WaitForFail(ydb, settings, poolId);
    }

    Y_UNIT_TEST(TestCreateResourcePoolClassifierOnServerless) {
        auto ydb = TYdbSetupSettings()
            .CreateSampleTenants(true)
            .EnableResourcePoolsOnServerless(true)
            .Create();

        auto settings = TQueryRunnerSettings()
            .PoolId("")
            .UserSID("test@user")
            .Database(ydb->GetSettings().GetServerlessTenantName())
            .NodeIndex(1);

        const TString& poolId = "my_pool";
        CreateSampleResourcePoolClassifier(ydb, settings, poolId);

        WaitForFail(ydb, settings, poolId);
    }

    Y_UNIT_TEST(TestAlterResourcePoolClassifier) {
        auto ydb = TYdbSetupSettings().Create();

        auto settings = TQueryRunnerSettings().PoolId("").UserSID("test@user");
        const TString& poolId = "my_pool";
        const TString& classifierId = CreateSampleResourcePoolClassifier(ydb, settings, poolId);

        WaitForFail(ydb, settings, poolId);

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            ALTER RESOURCE POOL CLASSIFIER )" << classifierId << R"( SET (
                RESOURCE_POOL=")" << NResourcePool::DEFAULT_POOL_ID << R"("
            );
        )");

        WaitForSuccess(ydb, settings);
    }

    Y_UNIT_TEST(TestDropResourcePoolClassifier) {
        auto ydb = TYdbSetupSettings().Create();

        auto settings = TQueryRunnerSettings().PoolId("").UserSID("test@user");
        const TString& poolId = "my_pool";
        const TString& classifierId = CreateSampleResourcePoolClassifier(ydb, settings, poolId);

        WaitForFail(ydb, settings, poolId);

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            DROP RESOURCE POOL CLASSIFIER )" << classifierId << R"(;
        )");

        WaitForSuccess(ydb, settings);
    }

    Y_UNIT_TEST(TestDropResourcePool) {
        auto ydb = TYdbSetupSettings().Create();

        auto settings = TQueryRunnerSettings().PoolId("").UserSID("test@user");
        const TString& poolId = "my_pool";
        CreateSampleResourcePoolClassifier(ydb, settings, poolId);

        WaitForFail(ydb, settings, poolId);

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            DROP RESOURCE POOL )" << poolId << R"(;
        )");

        WaitForSuccess(ydb, settings);
    }

    Y_UNIT_TEST(TestResourcePoolClassifierRanks) {
        auto ydb = TYdbSetupSettings().Create();

        auto settings = TQueryRunnerSettings().PoolId("").UserSID("test@user");
        const TString& poolId = "my_pool";
        CreateSampleResourcePoolClassifier(ydb, settings, poolId);

        WaitForFail(ydb, settings, poolId);

        const TString& classifierId = "rank_classifier";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL CLASSIFIER )" << classifierId << R"( WITH (
                RANK="1",
                RESOURCE_POOL=")" << NResourcePool::DEFAULT_POOL_ID << R"(",
                MEMBER_NAME=")" << settings.UserSID_ << R"("
            );
        )");

        WaitForSuccess(ydb, settings);

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            ALTER RESOURCE POOL CLASSIFIER )" << classifierId << R"( RESET (
                RANK
            );
        )");

        WaitForFail(ydb, settings, poolId);
    }

    Y_UNIT_TEST(TestExplicitPoolId) {
        auto ydb = TYdbSetupSettings().Create();

        auto settings = TQueryRunnerSettings().PoolId("").UserSID("test@user");
        const TString& poolId = "my_pool";
        CreateSampleResourcePoolClassifier(ydb, settings, poolId);

        WaitForFail(ydb, settings, poolId);
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID)));
    }

    Y_UNIT_TEST(TestMultiGroupClassification) {
        auto ydb = TYdbSetupSettings().Create();

        auto settings = TQueryRunnerSettings().PoolId("");

        const TString& poolId = "my_pool";
        const TString& firstSID = "first@user";
        const TString& secondSID = "second@user";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL )" << poolId << R"( WITH (
                CONCURRENT_QUERY_LIMIT=0
            );
            CREATE RESOURCE POOL CLASSIFIER first_classifier WITH (
                RESOURCE_POOL=")" << poolId << R"(",
                MEMBER_NAME=")" << firstSID << R"(",
                RANK=1
            );
            CREATE RESOURCE POOL CLASSIFIER second_classifier WITH (
                RESOURCE_POOL=")" << NResourcePool::DEFAULT_POOL_ID << R"(",
                MEMBER_NAME=")" << secondSID << R"(",
                RANK=2
            );
        )");

        WaitForFail(ydb, settings.GroupSIDs({firstSID}), poolId);
        WaitForSuccess(ydb, settings.GroupSIDs({secondSID}));
        WaitForFail(ydb, settings.GroupSIDs({firstSID, secondSID}), poolId);

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            ALTER RESOURCE POOL CLASSIFIER second_classifier SET (
                RANK=0
            );
        )");

        WaitForSuccess(ydb, settings.GroupSIDs({firstSID, secondSID}));
    }
}

}  // namespace NKikimr::NKqp
