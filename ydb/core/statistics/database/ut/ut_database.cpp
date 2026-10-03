#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/library/testlib/helpers.h>

#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/database/database.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/testlib/actors/block_events.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <util/string/escape.h>

#include <thread>

namespace NKikimr::NStat {

Y_UNIT_TEST_SUITE(StatisticsSaveLoad) {
    Y_UNIT_TEST_QUAD(SaveLoadSampledStatistics, MultiColumn, EnableNewRbo) {
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            auto* tableService = settings.AppConfig->MutableTableServiceConfig();
            tableService->SetEnableNewRBO(EnableNewRbo);
            tableService->SetEnableFallbackToYqlOptimizer(false);
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto sender = runtime.AllocateEdgeActor();
        runtime.Register(CreateStatisticsTableCreator(
            std::make_unique<TEvStatistics::TEvStatTableCreationResponse>(), "/Root/Database"),
            0, 0, TMailboxType::Simple, 0, sender);
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvStatTableCreationResponse>(sender);
        const TPathId pathId(1, 1);
        const auto type = EStatType::COUNT_MIN_SKETCH;
        const TColumnTags columns = MultiColumn ? TColumnTags(std::vector<ui32>{1, 2}) : TColumnTags(1u);
        runtime.Register(CreateSaveStatisticsQuery(sender, "/Root/Database", pathId, {}));
        const auto emptySave = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvSaveStatisticsQueryResponse>(sender);
        UNIT_ASSERT_C(emptySave->Get()->Success, emptySave->Get()->Issues.ToString());
        const auto save = [&](TString data, bool sampled) {
            TStatisticsItem item(1, type, std::move(data));
            item.ColumnTags = columns;
            if (sampled) {
                auto& metadata = item.Sampling.emplace();
                metadata.SetRequestedRate(0.5);
                metadata.SetEligibleUnits(4);
                metadata.SetSelectedUnits(2);
                metadata.SetSampleRows(100);
            }
            runtime.Register(CreateSaveStatisticsQuery(sender, "/Root/Database", pathId, {std::move(item)}));
            const auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvSaveStatisticsQueryResponse>(sender);
            UNIT_ASSERT_C(response->Get()->Success, response->Get()->Issues.ToString());
        };
        const auto read = [&](bool sampled, EStatType statType = EStatType::COUNT_MIN_SKETCH) {
            runtime.RunCall([&] {
                DispatchLoadStatisticsQuery(sender, 123, "/Root/Database", pathId, statType, columns, sampled);
                return 0;
            });
            return runtime.GrabEdgeEventRethrow<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
        };
        save("sample-only", true);
        auto result = read(true);
        UNIT_ASSERT(result->Get()->Success && result->Get()->Sampling);
        UNIT_ASSERT_VALUES_EQUAL(*result->Get()->Data, "sample-only");
        result = read(false);
        UNIT_ASSERT(!result->Get()->Success && !result->Get()->Data && !result->Get()->Sampling);

        save("full", false);
        result = read(true);
        UNIT_ASSERT(result->Get()->Success && !result->Get()->Sampling);
        UNIT_ASSERT_VALUES_EQUAL(*result->Get()->Data, "full");
        save("sample", true);
        result = read(true);
        UNIT_ASSERT(result->Get()->Success && result->Get()->Sampling);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Sampling->GetSampleRows(), 100);
        UNIT_ASSERT_VALUES_EQUAL(*result->Get()->Data, "sample");
        result = read(false);
        UNIT_ASSERT(result->Get()->Success && !result->Get()->Sampling);
        UNIT_ASSERT_VALUES_EQUAL(*result->Get()->Data, "full");
        save("new-sample", true);
        result = read(true);
        UNIT_ASSERT(result->Get()->Success && result->Get()->Sampling && result->Get()->Data);
        UNIT_ASSERT_VALUES_EQUAL(*result->Get()->Data, "new-sample");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Sampling->GetRequestedRate(), 0.5);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Sampling->GetEligibleUnits(), 4);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Sampling->GetSelectedUnits(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Sampling->GetSampleRows(), 100);
        UNIT_ASSERT_VALUES_EQUAL(*read(false)->Get()->Data, "full");

        TStatisticsItem anotherType(1, EStatType::SIMPLE_COLUMN, "another-type");
        anotherType.ColumnTags = columns;
        TStatisticsItem sampled(1, type, "mixed-sample");
        sampled.ColumnTags = columns;
        sampled.Sampling = result->Get()->Sampling;
        TBlockEvents<NKqp::TEvKqp::TEvQueryRequest> fullSave(runtime, [](const auto& ev) {
            const auto& sql = ev->Get()->GetQuery();
            return sql.Contains(StatisticsTablePath) && sql.Contains("NULL AS sampled_data");
        });
        runtime.Register(CreateSaveStatisticsQuery(sender, "/Root/Database", pathId,
            {TStatisticsItem(3, type, "another-column"), std::move(anotherType), std::move(sampled)}));
        runtime.WaitFor("full statistics save", [&] { return !fullSave.empty(); });
        // The sampled update must remain invisible until the full rows are committed too.
        UNIT_ASSERT_VALUES_EQUAL(*read(true)->Get()->Data, "new-sample");
        UNIT_ASSERT(!read(false, EStatType::SIMPLE_COLUMN)->Get()->Success);
        fullSave.Stop().Unblock();
        const auto mixedSave = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvSaveStatisticsQueryResponse>(sender);
        UNIT_ASSERT_C(mixedSave->Get()->Success, mixedSave->Get()->Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(*read(true)->Get()->Data, "mixed-sample");
        UNIT_ASSERT_VALUES_EQUAL(*read(false)->Get()->Data, "full");
        UNIT_ASSERT_VALUES_EQUAL(*read(false, EStatType::SIMPLE_COLUMN)->Get()->Data, "another-type");

        save("new-full", false);
        result = read(true);
        UNIT_ASSERT(result->Get()->Success && !result->Get()->Sampling);
        UNIT_ASSERT_VALUES_EQUAL(*result->Get()->Data, "new-full");
    }

    Y_UNIT_TEST(MalformedSampleFallsBackToFullStatistics) {
        TTestEnv env(1, 1, false);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto sender = runtime.AllocateEdgeActor();
        runtime.Register(CreateStatisticsTableCreator(
            std::make_unique<TEvStatistics::TEvStatTableCreationResponse>(), "/Root/Database"),
            0, 0, TMailboxType::Simple, 0, sender);
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvStatTableCreationResponse>(sender);
        const TPathId pathId(1, 1);
        runtime.Register(CreateSaveStatisticsQuery(sender, "/Root/Database", pathId,
            {TStatisticsItem(1, EStatType::SIMPLE_COLUMN, "full")}));
        UNIT_ASSERT(runtime.GrabEdgeEventRethrow<TEvStatistics::TEvSaveStatisticsQueryResponse>(sender)->Get()->Success);
        NKikimrStat::TSampledStatistic incomplete;
        incomplete.SetData("sample");
        for (const auto& data : {TString("broken"), incomplete.SerializeAsString()}) {
            ExecuteYqlScript(env, TStringBuilder()
                << "UPSERT INTO `/Root/Database/.metadata/statistics_v2`"
                << " (owner_id, local_path_id, stat_type, column_tags, sampled_data)"
                << " VALUES (1ul, 1ul, 1u, '1', \"" << EscapeC(data) << "\");");
            runtime.RunCall([&] {
                DispatchLoadStatisticsQuery(sender, 123, "/Root/Database", pathId, EStatType::SIMPLE_COLUMN, TColumnTags(1u), true);
                return 0;
            });
            const auto result = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
            UNIT_ASSERT(result->Get()->Success && !result->Get()->Sampling);
            UNIT_ASSERT_VALUES_EQUAL(*result->Get()->Data, "full");
        }
    }

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
        EStatType statType = EStatType::COUNT_MIN_SKETCH;
        std::vector<TStatisticsItem> statItems;
        statItems.emplace_back(1, statType, "dataA");
        statItems.emplace_back(2, statType, "dataB");

        runtime.Register(CreateSaveStatisticsQuery(sender, "/Root/Database",
            pathId, std::move(statItems)),
            0, 0, TMailboxType::Simple, 0, sender);
        auto saveResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvSaveStatisticsQueryResponse>(sender);
        UNIT_ASSERT(saveResponse->Get()->Success);

        runtime.RunCall([&] {
            DispatchLoadStatisticsQuery(sender, 123, "/Root/Database", pathId, statType, TColumnTags(1u));
            return 0;
        });
        auto loadResponseA = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
        UNIT_ASSERT(loadResponseA->Get()->Success);
        UNIT_ASSERT(loadResponseA->Get()->Data);
        UNIT_ASSERT_VALUES_EQUAL(*loadResponseA->Get()->Data, "dataA");

        runtime.RunCall([&] {
            DispatchLoadStatisticsQuery(sender, 345, "/Root/Database", pathId, statType, TColumnTags(2u));
            return 0;
        });
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
        EStatType statType = EStatType::COUNT_MIN_SKETCH;
        std::vector<TStatisticsItem> statItems;
        statItems.emplace_back(1, statType, "dataA");
        statItems.emplace_back(2, statType, "dataB");

        runtime.Register(CreateSaveStatisticsQuery(sender, "/Root/Database",
            pathId, std::move(statItems)),
            0, 0, TMailboxType::Simple, 0, sender);
        auto saveResponse = runtime.GrabEdgeEvent<TEvStatistics::TEvSaveStatisticsQueryResponse>(sender);
        UNIT_ASSERT(saveResponse->Get()->Success);

        runtime.Register(CreateDeleteStatisticsQuery(sender, "/Root/Database", pathId),
            0, 0, TMailboxType::Simple, 0, sender);
        auto deleteResponse = runtime.GrabEdgeEvent<TEvStatistics::TEvDeleteStatisticsQueryResponse>(sender);
        UNIT_ASSERT(deleteResponse->Get()->Success);

        runtime.RunCall([&] {
            DispatchLoadStatisticsQuery(sender, 123, "/Root/Database", pathId, statType, TColumnTags(1u));
            return 0;
        });
        auto loadResponseA = runtime.GrabEdgeEvent<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
        UNIT_ASSERT(!loadResponseA->Get()->Success);
    }

    Y_UNIT_TEST(SimpleMultiColumn) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");

        auto sender = runtime.AllocateEdgeActor(0);
        runtime.Register(CreateStatisticsTableCreator(
            std::make_unique<TEvStatistics::TEvStatTableCreationResponse>(), "/Root/Database"),
            0, 0, TMailboxType::Simple, 0, sender);
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvStatTableCreationResponse>(sender);

        TPathId pathId(1, 1);
        EStatType statType = EStatType::COUNT_MIN_SKETCH;
        std::vector<TStatisticsItem> statItems;
        statItems.emplace_back(std::vector<ui32>{1, 2}, statType, "dataA");
        statItems.emplace_back(std::vector<ui32>{3, 4}, statType, "dataB");

        runtime.Register(CreateSaveStatisticsQuery(sender, "/Root/Database",
            pathId, std::move(statItems)),
            0, 0, TMailboxType::Simple, 0, sender);
        auto saveResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvSaveStatisticsQueryResponse>(sender);
        UNIT_ASSERT(saveResponse->Get()->Success);

        runtime.RunCall([&] {
            DispatchLoadStatisticsQuery(sender, 123, "/Root/Database", pathId, statType, TColumnTags(std::vector<ui32>{1, 2}));
            return 0;
        });
        auto loadResponseA = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
        UNIT_ASSERT(loadResponseA->Get()->Success);
        UNIT_ASSERT(loadResponseA->Get()->Data);
        UNIT_ASSERT_VALUES_EQUAL(*loadResponseA->Get()->Data, "dataA");

        runtime.RunCall([&] {
            DispatchLoadStatisticsQuery(sender, 345, "/Root/Database", pathId, statType, TColumnTags(std::vector<ui32>{3, 4}));
            return 0;
        });
        auto loadResponseB = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
        UNIT_ASSERT(loadResponseB->Get()->Success);
        UNIT_ASSERT(loadResponseB->Get()->Data);
        UNIT_ASSERT_VALUES_EQUAL(*loadResponseB->Get()->Data, "dataB");

        runtime.RunCall([&] {
            DispatchLoadStatisticsQuery(sender, 567, "/Root/Database", pathId, statType, TColumnTags(std::vector<ui32>{2, 1}));
            return 0;
        });
        auto loadResponseC = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
        UNIT_ASSERT(!loadResponseC->Get()->Success);
    }

    Y_UNIT_TEST(DeleteMultiColumn) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");

        auto sender = runtime.AllocateEdgeActor(0);
        runtime.Register(CreateStatisticsTableCreator(
            std::make_unique<TEvStatistics::TEvStatTableCreationResponse>(), "/Root/Database"),
            0, 0, TMailboxType::Simple, 0, sender);
        runtime.GrabEdgeEvent<TEvStatistics::TEvStatTableCreationResponse>(sender);

        TPathId pathId(1, 1);
        EStatType statType = EStatType::COUNT_MIN_SKETCH;
        std::vector<TStatisticsItem> statItems;
        statItems.emplace_back(std::vector<ui32>{1, 2}, statType, "dataA");
        statItems.emplace_back(std::vector<ui32>{3, 4}, statType, "dataB");

        runtime.Register(CreateSaveStatisticsQuery(sender, "/Root/Database",
            pathId, std::move(statItems)),
            0, 0, TMailboxType::Simple, 0, sender);
        auto saveResponse = runtime.GrabEdgeEvent<TEvStatistics::TEvSaveStatisticsQueryResponse>(sender);
        UNIT_ASSERT(saveResponse->Get()->Success);

        runtime.Register(CreateDeleteStatisticsQuery(sender, "/Root/Database", pathId),
            0, 0, TMailboxType::Simple, 0, sender);
        auto deleteResponse = runtime.GrabEdgeEvent<TEvStatistics::TEvDeleteStatisticsQueryResponse>(sender);
        UNIT_ASSERT(deleteResponse->Get()->Success);

        runtime.RunCall([&] {
            DispatchLoadStatisticsQuery(sender, 123, "/Root/Database", pathId, statType, TColumnTags(std::vector<ui32>{1, 2}));
            return 0;
        });
        auto loadResponseA = runtime.GrabEdgeEvent<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
        UNIT_ASSERT(!loadResponseA->Get()->Success);
    }

    Y_UNIT_TEST(ForbidAccess) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database", 1, true);
        PrepareUniformTable(env, "Database", "Table");

        NYdb::EStatus status;
        auto test = [&] () {
            auto driverConfig = NYdb::TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetDatabase("/Root")
                .SetAuthToken("user@builtin");
            auto driver = NYdb::TDriver(driverConfig);
            auto db = NYdb::NTable::TTableClient(driver);
            auto session = db.CreateSession().GetValueSync().GetSession();

            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/Database/.metadata/statistics_v2`;
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
