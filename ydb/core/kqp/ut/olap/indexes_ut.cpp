#include "helpers/local.h"
#include "helpers/writer.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>

#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapIndexes) {
    Y_UNIT_TEST(IndexesActualization) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideReduceMemoryIntervalLimit(1LLU << 30);
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        Tests::NCommon::TLoggerInit(kikimr)
            .SetComponents({ NKikimrServices::TX_COLUMNSHARD }, "CS")
            .SetPriority(NActors::NLog::PRI_DEBUG)
            .Initialize();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1200000, 300200000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1300000, 300300000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1400000, 300400000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 2000000, 200000000, 70000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 3000000, 100000000, 110000);
        }
        csController->WaitCompactions(TDuration::Seconds(5));

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_names" : ["uid"], "false_positive_probability" : 0.05}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        {
            auto alterQuery =
                TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_names" : ["resource_id", "level"], "false_positive_probability" : 0.05}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery =
                TStringBuilder()
                << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        csController->WaitActualization(TDuration::Seconds(10));
        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
                WHERE ((resource_id = '2' AND level = 222222) OR (resource_id = '1' AND level = 111111) OR (resource_id LIKE '%11dd%')) AND uid = '222'
            )")
                          .GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cerr << result << Endl;
            Cerr << csController->GetIndexesSkippingOnSelect().Val() << " / " << csController->GetIndexesApprovedOnSelect().Val() << Endl;
            CompareYson(result, R"([[0u;]])");
            AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0);
            AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() < csController->GetIndexesSkippingOnSelect().Val())
            ("approve", csController->GetIndexesApprovedOnSelect().Val())("skip", csController->GetIndexesSkippingOnSelect().Val());
        }
    }

    Y_UNIT_TEST(CountMinSketchIndex) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideReduceMemoryIntervalLimit(1LLU << 30);

        TLocalHelper(kikimr).CreateTestOlapTableWithoutStore();
        auto tableClient = kikimr.GetTableClient();
        auto& client = kikimr.GetTestClient();

        Tests::NCommon::TLoggerInit(kikimr)
            .SetComponents({ NKikimrServices::TX_COLUMNSHARD }, "CS")
            .SetPriority(NActors::NLog::PRI_DEBUG)
            .Initialize();

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_ts, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['timestamp']}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_res_id, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['resource_id']}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_uid, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['uid']}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_level, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['level']}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_message, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['message']}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        WriteTestData(kikimr, "/Root/olapTable", 1000000, 300000000, 10000);
        WriteTestData(kikimr, "/Root/olapTable", 1100000, 300100000, 10000);
        WriteTestData(kikimr, "/Root/olapTable", 1200000, 300200000, 10000);
        WriteTestData(kikimr, "/Root/olapTable", 1300000, 300300000, 10000);
        WriteTestData(kikimr, "/Root/olapTable", 1400000, 300400000, 10000);
        WriteTestData(kikimr, "/Root/olapTable", 2000000, 200000000, 70000);
        WriteTestData(kikimr, "/Root/olapTable", 3000000, 100000000, 110000);

        csController->WaitActualization(TDuration::Seconds(10));

        {
            auto res = client.Ls("/Root/olapTable");
            auto description = res->Record.GetPathDescription().GetColumnTableDescription();
            auto indexes = description.GetSchema().GetIndexes();
            UNIT_ASSERT(indexes.size() == 5);

            std::unordered_set<TString> indexNames{ "cms_ts", "cms_res_id", "cms_uid", "cms_level", "cms_message" };
            for (const auto& i : indexes) {
                Cerr << ">>> " << i.GetName() << " of class name " << i.GetClassName() << Endl;
                UNIT_ASSERT(i.GetClassName() == "COUNT_MIN_SKETCH");
                UNIT_ASSERT(indexNames.erase(i.GetName()));
            }
            UNIT_ASSERT(indexNames.empty());
        }

        {
            auto runtime = kikimr.GetTestServer().GetRuntime();
            auto sender = runtime->AllocateEdgeActor();

            TAutoPtr<IEventHandle> handle;

            size_t shard = 0;
            std::set<ui64> pathids;
            for (auto&& i : csController->GetShardActualIds()) {
                Cerr << ">>> shard actual id: " << i << Endl;
                for (auto&& j : csController->GetPathIds(i)) {
                    Cerr << ">>> path id: " << j << Endl;
                    pathids.insert(j);
                }
                if (++shard == 3) {
                    break;
                }
            }

            UNIT_ASSERT(pathids.size() == 1);
            ui64 pathId = *pathids.begin();

            shard = 0;
            for (auto&& i : csController->GetShardActualIds()) {
                auto request = std::make_unique<NStat::TEvStatistics::TEvStatisticsRequest>();
                request->Record.MutableTable()->MutablePathId()->SetLocalId(pathId);

                runtime->Send(MakePipePerNodeCacheID(false), sender, new TEvPipeCache::TEvForward(request.release(), i, false));
                if (++shard == 3) {
                    break;
                }
            }

            auto sketch = std::unique_ptr<TCountMinSketch>(TCountMinSketch::Create());
            for (size_t shard = 0; shard < 3; ++shard) {
                auto event = runtime->GrabEdgeEvent<NStat::TEvStatistics::TEvStatisticsResponse>(handle);
                UNIT_ASSERT(event);

                auto& response = event->Record;
                // Cerr << response << Endl;
                UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS);
                UNIT_ASSERT(response.ColumnsSize() == 5);
                TString someData = response.GetColumns(0).GetStatistics(0).GetData();
                *sketch += *std::unique_ptr<TCountMinSketch>(TCountMinSketch::FromString(someData.data(), someData.size()));
                Cerr << ">>> sketch.GetElementCount() = " << sketch->GetElementCount() << Endl;
                UNIT_ASSERT(sketch->GetElementCount() > 0);
            }
        }
    }

    Y_UNIT_TEST(SchemeActualizationOnceOnStart) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        std::vector<TString> uids;
        std::vector<TString> resourceIds;
        std::vector<ui32> levels;

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);

            const auto filler = [&](const ui32 startRes, const ui32 startUid, const ui32 count) {
                for (ui32 i = 0; i < count; ++i) {
                    uids.emplace_back("uid_" + ::ToString(startUid + i));
                    resourceIds.emplace_back(::ToString(startRes + i));
                    levels.emplace_back(i % 5);
                }
            };

            filler(1000000, 300000000, 10000);
            filler(1100000, 300100000, 10000);
        }
        const ui64 initCount = csController->GetActualizationRefreshSchemeCount().Val();
        AFL_VERIFY(initCount == 3)("started_value", initCount);

        for (ui32 i = 0; i < 10; ++i) {
            auto alterQuery =
                TStringBuilder()
                << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        const ui64 updatesCount = csController->GetActualizationRefreshSchemeCount().Val();
        AFL_VERIFY(updatesCount == 30 + initCount)("after_modification", updatesCount);

        for (auto&& i : csController->GetShardActualIds()) {
            kikimr.GetTestServer().GetRuntime()->Send(
                MakePipePerNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(new TEvents::TEvPoisonPill(), i, false));
        }

        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )")
                          .GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[20000u;]])");
        }

        AFL_VERIFY(updatesCount + 6 ==
            (ui64)csController->GetActualizationRefreshSchemeCount().Val())("updates", updatesCount)(
                                       "count", csController->GetActualizationRefreshSchemeCount().Val());
    }

    class TTestIndexesScenario {
    private:
        TKikimrSettings Settings;
        std::unique_ptr<TKikimrRunner> Kikimr;
        YDB_ACCESSOR(TString, StorageId, "__DEFAULT");

        ui64 SkipStart = 0;
        ui64 NoDataStart = 0;
        ui64 ApproveStart = 0;

        template <class TController>
        void ResetZeroLevel(TController& g) {
            SkipStart = g->GetIndexesSkippingOnSelect().Val();
            ApproveStart = g->GetIndexesApprovedOnSelect().Val();
            NoDataStart = g->GetIndexesSkippedNoData().Val();
        }

        void ExecuteSQL(const TString& text, const TString& expectedResult) const {
            auto tableClient = Kikimr->GetTableClient();
            auto it = tableClient.StreamExecuteScanQuery(text).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("result", result)("expected", expectedResult);
            auto* controller = NYDBTest::TControllers::GetControllerAs<NOlap::TWaitCompactionController>();
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("skip", controller->GetIndexesSkippingOnSelect().Val() - SkipStart)(
                "check", controller->GetIndexesApprovedOnSelect().Val() - ApproveStart)(
                "no_data", controller->GetIndexesSkippedNoData().Val() - NoDataStart);
            CompareYson(result, expectedResult);
        }

    public:
        TTestIndexesScenario& Initialize() {
            Settings = TKikimrSettings().SetWithSampleTables(false);
            Kikimr = std::make_unique<TKikimrRunner>(Settings);
            return *this;
        }

        void Execute() {
            auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
            csController->SetOverrideReduceMemoryIntervalLimit(1LLU << 30);
            csController->SetOverrideMemoryLimitForPortionReading(1e+10);
            TLocalHelper(*Kikimr).CreateTestOlapTable();
            auto tableClient = Kikimr->GetTableClient();

            //        Tests::NCommon::TLoggerInit(kikimr).Initialize();

            {
                auto alterQuery =
                    TStringBuilder() << Sprintf(
                        R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_names" : ["uid"], "false_positive_probability" : 0.05, "storage_id" : "%s"}`);
                )",
                        StorageId.data());
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery =
                    TStringBuilder() << Sprintf(
                        R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_uid, TYPE=BLOOM_NGRAMM_FILTER,
                    FEATURES=`{"column_name" : "resource_id", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 64024}`);
                )",
                        StorageId.data());
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery =
                    TStringBuilder() << Sprintf(
                        R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_names" : ["resource_id", "level"], "false_positive_probability" : 0.05, "storage_id" : "%s"}`);
                )",
                        StorageId.data());
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }

            std::vector<TString> uids;
            std::vector<TString> resourceIds;
            std::vector<ui32> levels;

            {
                for (ui32 i = 0; i < 2; ++i) {
                    WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
                    WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
                    WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1200000, 300200000, 10000);
                    WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1300000, 300300000, 10000);
                    WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1400000, 300400000, 10000);
                    WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 2000000, 200000000, 70000);
                    WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 3000000, 100000000, 110000);
                }

                const auto filler = [&](const ui32 startRes, const ui32 startUid, const ui32 count) {
                    for (ui32 i = 0; i < count; ++i) {
                        uids.emplace_back("uid_" + ::ToString(startUid + i));
                        resourceIds.emplace_back(::ToString(startRes + i));
                        levels.emplace_back(i % 5);
                    }
                };

                filler(1000000, 300000000, 10000);
                filler(1100000, 300100000, 10000);
                filler(1200000, 300200000, 10000);
                filler(1300000, 300300000, 10000);
                filler(1400000, 300400000, 10000);
                filler(2000000, 200000000, 70000);
                filler(3000000, 100000000, 110000);
            }

            ExecuteSQL(R"(SELECT COUNT(*) FROM `/Root/olapStore/olapTable`)", "[[230000u;]]");

            AFL_VERIFY(csController->GetIndexesSkippingOnSelect().Val() == 0);
            AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() == 0);
            TInstant start = Now();
            ui32 compactionsStart = csController->GetCompactionStartedCounter().Val();
            while (Now() - start < TDuration::Seconds(10)) {
                if (compactionsStart != csController->GetCompactionStartedCounter().Val()) {
                    compactionsStart = csController->GetCompactionStartedCounter().Val();
                    start = Now();
                }
                Cerr << "WAIT_COMPACTION: " << csController->GetCompactionStartedCounter().Val() << Endl;
                Sleep(TDuration::Seconds(1));
            }
            // important checker for control compactions (<=21) and control indexes constructed (>=21)
            AFL_VERIFY(csController->GetCompactionStartedCounter().Val() == 21)("count", csController->GetCompactionStartedCounter().Val());

            {
                ExecuteSQL(R"(SELECT COUNT(*)
                    FROM `/Root/olapStore/olapTable`
                    WHERE resource_id LIKE '%110a151' AND resource_id LIKE '110a%' AND resource_id LIKE '%dd%')", "[[0u;]]");
                AFL_VERIFY(!csController->GetIndexesApprovedOnSelect().Val());
                AFL_VERIFY(csController->GetIndexesSkippingOnSelect().Val());
            }
            {
                ResetZeroLevel(csController);
                ExecuteSQL(R"(SELECT COUNT(*)
                    FROM `/Root/olapStore/olapTable`
                    WHERE resource_id LIKE '%110a151%')", "[[0u;]]");
                AFL_VERIFY(!csController->GetIndexesApprovedOnSelect().Val());
                AFL_VERIFY(csController->GetIndexesSkippingOnSelect().Val() - SkipStart);
            }
            {
                ResetZeroLevel(csController);
                ExecuteSQL(R"(SELECT COUNT(*)
                    FROM `/Root/olapStore/olapTable`
                    WHERE ((resource_id = '2' AND level = 222222) OR (resource_id = '1' AND level = 111111) OR (resource_id LIKE '%11dd%')) AND uid = '222')", "[[0u;]]");

                AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0)("val", csController->GetIndexesSkippedNoData().Val());
                AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() - ApproveStart < csController->GetIndexesSkippingOnSelect().Val() - SkipStart);
            }
            {
                ResetZeroLevel(csController);
                ui32 requestsCount = 100;
                for (ui32 i = 0; i < requestsCount; ++i) {
                    const ui32 idx = RandomNumber<ui32>(uids.size());
                    const auto query = [](const TString& res, const TString& uid, const ui32 level) {
                        TStringBuilder sb;
                        sb << "SELECT COUNT(*) FROM `/Root/olapStore/olapTable`" << Endl;
                        sb << "WHERE(" << Endl;
                        sb << "resource_id = '" << res << "' AND" << Endl;
                        sb << "uid= '" << uid << "' AND" << Endl;
                        sb << "level= " << level << Endl;
                        sb << ")";
                        return sb;
                    };
                    ExecuteSQL(query(resourceIds[idx], uids[idx], levels[idx]), "[[1u;]]");
                }
                AFL_VERIFY((csController->GetIndexesApprovedOnSelect().Val() - ApproveStart) * 5 < csController->GetIndexesSkippingOnSelect().Val() - SkipStart)
                ("approved", csController->GetIndexesApprovedOnSelect().Val() - ApproveStart)(
                    "skipped", csController->GetIndexesSkippingOnSelect().Val() - SkipStart);
            }
            {
                ResetZeroLevel(csController);
                ui32 requestsCount = 100;
                for (ui32 i = 0; i < requestsCount; ++i) {
                    const ui32 idx = RandomNumber<ui32>(uids.size());
                    const auto query = [](const TString& res, const TString& uid, const ui32 level) {
                        TStringBuilder sb;
                        sb << "SELECT COUNT(*) FROM `/Root/olapStore/olapTable`" << Endl;
                        sb << "WHERE" << Endl;
                        sb << "resource_id LIKE '%" << res << "%'" << Endl;
                        return sb;
                    };
                    ExecuteSQL(query(resourceIds[idx], uids[idx], levels[idx]), "[[1u;]]");
                }
                AFL_VERIFY(csController->GetIndexesSkippingOnSelect().Val() - SkipStart > 1)("approved", csController->GetIndexesApprovedOnSelect().Val() - ApproveStart)(
                    "skipped", csController->GetIndexesSkippingOnSelect().Val() - SkipStart);
            }
            {
                ResetZeroLevel(csController);
                ui32 requestsCount = 100;
                for (ui32 i = 0; i < requestsCount; ++i) {
                    const ui32 idx = RandomNumber<ui32>(uids.size());
                    const auto query = [](const TString& res, const TString& uid, const ui32 level) {
                        TStringBuilder sb;
                        sb << "SELECT COUNT(*) FROM `/Root/olapStore/olapTable`" << Endl;
                        sb << "WHERE" << Endl;
                        sb << "resource_id LIKE '" << res << "%'" << Endl;
                        return sb;
                    };
                    ExecuteSQL(query(resourceIds[idx], uids[idx], levels[idx]), "[[1u;]]");
                }
                AFL_VERIFY(csController->GetIndexesSkippingOnSelect().Val() - SkipStart > 1)(
                    "approved", csController->GetIndexesApprovedOnSelect().Val() - ApproveStart)(
                    "skipped", csController->GetIndexesSkippingOnSelect().Val() - SkipStart);
            }
            {
                ResetZeroLevel(csController);
                ui32 requestsCount = 100;
                for (ui32 i = 0; i < requestsCount; ++i) {
                    const ui32 idx = RandomNumber<ui32>(uids.size());
                    const auto query = [](const TString& res, const TString& uid, const ui32 level) {
                        TStringBuilder sb;
                        sb << "SELECT COUNT(*) FROM `/Root/olapStore/olapTable`" << Endl;
                        sb << "WHERE" << Endl;
                        sb << "resource_id LIKE '%" << res << "'" << Endl;
                        return sb;
                    };
                    ExecuteSQL(query(resourceIds[idx], uids[idx], levels[idx]), "[[1u;]]");
                }
                AFL_VERIFY(csController->GetIndexesSkippingOnSelect().Val() - SkipStart > 1)(
                    "approved", csController->GetIndexesApprovedOnSelect().Val() - ApproveStart)(
                    "skipped", csController->GetIndexesSkippingOnSelect().Val() - SkipStart);
            }
        }
    };

    Y_UNIT_TEST(IndexesInBS) {
        TTestIndexesScenario().SetStorageId("__DEFAULT").Initialize().Execute();
    }

    Y_UNIT_TEST(IndexesInLocalMetadata) {
        TTestIndexesScenario().SetStorageId("__LOCAL_METADATA").Initialize().Execute();
    }

    Y_UNIT_TEST(IndexesModificationError) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        //        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_names" : ["uid"], "false_positive_probability" : 0.05}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_names" : ["uid", "resource_id"], "false_positive_probability" : 0.05}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(alterResult.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_names" : ["uid"], "false_positive_probability" : 0.005}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(alterResult.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_names" : ["uid"], "false_positive_probability" : 0.01}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=DROP_INDEX, NAME=index_uid);";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
    }
}

}   // namespace NKikimr::NKqp
