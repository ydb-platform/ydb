#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/database/database.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <thread>

namespace NKikimr::NStat {

Y_UNIT_TEST_SUITE(StatisticsSaveLoad) {
    Y_UNIT_TEST(Simple) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");

        auto sender = runtime.AllocateEdgeActor(0);
        runtime.Register(CreateStatisticsTableCreator(
            std::make_unique<TEvStatistics::TEvStatTableCreationResponse>(), "/Root/Database"),
            0, 0, TMailboxType::Simple, 0, sender);
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvStatTableCreationResponse>(sender);

        TPathId pathId(1, 1);
        ui64 statType = 1;
        std::vector<ui32> columnTags = {1, 2};
        std::vector<TString> data = {"dataA", "dataB"};

        runtime.Register(CreateSaveStatisticsQuery(sender, "/Root/Database",
            pathId, statType, std::move(columnTags), std::move(data)),
            0, 0, TMailboxType::Simple, 0, sender);
        auto saveResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvSaveStatisticsQueryResponse>(sender);
        UNIT_ASSERT(saveResponse->Get()->Success);

        runtime.Register(CreateLoadStatisticsQuery(sender, "/Root/Database", pathId, statType, 1, 1),
            0, 0, TMailboxType::Simple, 0, sender);
        auto loadResponseA = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
        UNIT_ASSERT(loadResponseA->Get()->Success);
        UNIT_ASSERT(loadResponseA->Get()->Data);
        UNIT_ASSERT_VALUES_EQUAL(*loadResponseA->Get()->Data, "dataA");

        runtime.Register(CreateLoadStatisticsQuery(sender, "/Root/Database", pathId, statType, 2, 1),
            0, 0, TMailboxType::Simple, 0, sender);
        auto loadResponseB = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
        UNIT_ASSERT(loadResponseB->Get()->Success);
        UNIT_ASSERT(loadResponseB->Get()->Data);
        UNIT_ASSERT_VALUES_EQUAL(*loadResponseB->Get()->Data, "dataB");
    }

    Y_UNIT_TEST(Delete) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");

        auto sender = runtime.AllocateEdgeActor(0);
        runtime.Register(CreateStatisticsTableCreator(
            std::make_unique<TEvStatistics::TEvStatTableCreationResponse>(), "/Root/Database"),
            0, 0, TMailboxType::Simple, 0, sender);
        runtime.GrabEdgeEvent<TEvStatistics::TEvStatTableCreationResponse>(sender);

        TPathId pathId(1, 1);
        ui64 statType = 1;
        std::vector<ui32> columnTags = {1, 2};
        std::vector<TString> data = {"dataA", "dataB"};

        runtime.Register(CreateSaveStatisticsQuery(sender, "/Root/Database",
            pathId, statType, std::move(columnTags), std::move(data)),
            0, 0, TMailboxType::Simple, 0, sender);
        auto saveResponse = runtime.GrabEdgeEvent<TEvStatistics::TEvSaveStatisticsQueryResponse>(sender);
        UNIT_ASSERT(saveResponse->Get()->Success);

        runtime.Register(CreateDeleteStatisticsQuery(sender, "/Root/Database", pathId),
            0, 0, TMailboxType::Simple, 0, sender);
        auto deleteResponse = runtime.GrabEdgeEvent<TEvStatistics::TEvDeleteStatisticsQueryResponse>(sender);
        UNIT_ASSERT(deleteResponse->Get()->Success);

        runtime.Register(CreateLoadStatisticsQuery(sender, "/Root/Database", pathId, statType, 1, 1),
            0, 0, TMailboxType::Simple, 0, sender);
        auto loadResponseA = runtime.GrabEdgeEvent<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
        UNIT_ASSERT(!loadResponseA->Get()->Success);
    }

    Y_UNIT_TEST(ForbidAccess) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database", 1, true);
        CreateUniformTable(env, "Database", "Table");

        NYdb::EStatus status;
        auto test = [&] () {
            auto driverConfig = NYdb::TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetAuthToken("user@builtin");
            auto driver = NYdb::TDriver(driverConfig);
            auto db = NYdb::NTable::TTableClient(driver);
            auto session = db.CreateSession().GetValueSync().GetSession();

            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/Database/.metadata/_statistics`;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            status = result.GetStatus();
        };
        std::thread testThread(test);

        runtime.SimulateSleep(TDuration::Seconds(1));
        testThread.join();

        UNIT_ASSERT_VALUES_EQUAL(status, NYdb::EStatus::SCHEME_ERROR);
    }

}

} // NKikimr::NStat
