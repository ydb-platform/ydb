#include "ut_common.h"

#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/save_load_stats.h>

namespace NKikimr::NStat {

Y_UNIT_TEST_SUITE(StatisticsSaveLoad) {
    Y_UNIT_TEST(Simple) {
        TTestEnv env(1, 1);
        CreateDatabase(env, "Database");

        auto& runtime = *env.GetServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor(0);
        runtime.Register(CreateStatisticsTableCreator(std::make_unique<TEvStatistics::TEvStatTableCreationResponse>()),
            0, 0, TMailboxType::Simple, 0, sender);
        runtime.GrabEdgeEvent<TEvStatistics::TEvStatTableCreationResponse>(sender);

        TPathId pathId(1, 1);
        ui64 statType = 1;
        std::vector<TString> columnNames = {"colA", "colB"};
        std::vector<TString> data = {"dataA", "dataB"};

        runtime.Register(CreateSaveStatisticsQuery(
            pathId, statType, std::move(columnNames), std::move(data)),
            0, 0, TMailboxType::Simple, 0, sender);
        auto saveResponse = runtime.GrabEdgeEvent<TEvStatistics::TEvSaveStatisticsQueryResponse>(sender);
        UNIT_ASSERT(saveResponse->Get()->Success);

        runtime.Register(CreateLoadStatisticsQuery(pathId, statType, "colA"),
            0, 0, TMailboxType::Simple, 0, sender);
        auto loadResponseA = runtime.GrabEdgeEvent<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
        UNIT_ASSERT(loadResponseA->Get()->Success);
        UNIT_ASSERT(loadResponseA->Get()->Data);
        UNIT_ASSERT_VALUES_EQUAL(*loadResponseA->Get()->Data, "dataA");

        runtime.Register(CreateLoadStatisticsQuery(pathId, statType, "colB"),
            0, 0, TMailboxType::Simple, 0, sender);
        auto loadResponseB = runtime.GrabEdgeEvent<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
        UNIT_ASSERT(loadResponseB->Get()->Success);
        UNIT_ASSERT(loadResponseB->Get()->Data);
        UNIT_ASSERT_VALUES_EQUAL(*loadResponseB->Get()->Data, "dataB");
    }
}

} // NKikimr::NStat
