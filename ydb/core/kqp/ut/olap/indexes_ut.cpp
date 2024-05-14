#include "helpers/local.h"
#include "helpers/writer.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapIndexes) {
    Y_UNIT_TEST(IndexesActualization) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideReduceMemoryIntervalLimit(1LLU << 30);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        Tests::NCommon::TLoggerInit(kikimr).SetComponents({NKikimrServices::TX_COLUMNSHARD}, "CS").SetPriority(NActors::NLog::PRI_DEBUG).Initialize();

        std::vector<TString> uids;
        std::vector<TString> resourceIds;
        std::vector<ui32> levels;

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1200000, 300200000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1300000, 300300000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1400000, 300400000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 2000000, 200000000, 70000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 3000000, 100000000, 110000);

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
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id, TYPE=BLOOM_FILTER, 
                    FEATURES=`{"column_names" : ["resource_id", "level"], "false_positive_probability" : 0.05}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() <<
                "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`, EXTERNAL_GUARANTEE_EXCLUSIVE_PK=`true`);";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        csController->WaitActualization(TDuration::Seconds(10));
        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
                WHERE ((resource_id = '2' AND level = 222222) OR (resource_id = '1' AND level = 111111) OR (resource_id LIKE '%11dd%')) AND uid = '222'
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cerr << result << Endl;
            Cerr << csController->GetIndexesSkippingOnSelect().Val() << " / " << csController->GetIndexesApprovedOnSelect().Val() << Endl;
            CompareYson(result, R"([[0u;]])");
            AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0);
            AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() < csController->GetIndexesSkippingOnSelect().Val() * 0.4)
                ("approve", csController->GetIndexesApprovedOnSelect().Val())("skip", csController->GetIndexesSkippingOnSelect().Val());
        }
    }

    Y_UNIT_TEST(SchemeActualizationOnceOnStart) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));

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

        for (ui32 i = 0; i < 10; ++i) {
            auto alterQuery = TStringBuilder() <<
                "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        const ui64 startCount = csController->GetActualizationRefreshSchemeCount().Val();
        AFL_VERIFY(startCount == 30);

        for (auto&& i : csController->GetShardActualIds()) {
            kikimr.GetTestServer().GetRuntime()->Send(MakePipePeNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                new TEvents::TEvPoisonPill(), i, false));
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[20000u;]])");
        }

        AFL_VERIFY(startCount + 3 /*tables count*/ * 3 /*2 * normalizers + main_load*/ == 
            (ui64)csController->GetActualizationRefreshSchemeCount().Val())("start", startCount)("count", csController->GetActualizationRefreshSchemeCount().Val());
    }

    Y_UNIT_TEST(Indexes) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

//        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverrideReduceMemoryIntervalLimit(1LLU << 30);

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
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id, TYPE=BLOOM_FILTER, 
                    FEATURES=`{"column_names" : ["resource_id", "level"], "false_positive_probability" : 0.05}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        {
            auto alterQuery = TStringBuilder() <<
                "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, EXTERNAL_GUARANTEE_EXCLUSIVE_PK=`true`);";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        std::vector<TString> uids;
        std::vector<TString> resourceIds;
        std::vector<ui32> levels;

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1200000, 300200000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1300000, 300300000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1400000, 300400000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 2000000, 200000000, 70000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 3000000, 100000000, 110000);

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

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[230000u;]])");
        }

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

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
                WHERE ((resource_id = '2' AND level = 222222) OR (resource_id = '1' AND level = 111111) OR (resource_id LIKE '%11dd%')) AND uid = '222'
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            Cout << csController->GetIndexesSkippingOnSelect().Val() << " / " << csController->GetIndexesApprovedOnSelect().Val() << Endl;
            CompareYson(result, R"([[0u;]])");
            AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0);
            AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() < csController->GetIndexesSkippingOnSelect().Val() * 0.3);
        }
        ui32 requestsCount = 100;
        for (ui32 i = 0; i < requestsCount; ++i) {
            const ui32 idx = RandomNumber<ui32>(uids.size());
            const auto query = [](const TString& res, const TString& uid, const ui32 level) {
                TStringBuilder sb;
                sb << "SELECT" << Endl;
                sb << "COUNT(*)" << Endl;
                sb << "FROM `/Root/olapStore/olapTable`" << Endl;
                sb << "WHERE(" << Endl;
                sb << "resource_id = '" << res << "' AND" << Endl;
                sb << "uid= '" << uid << "' AND" << Endl;
                sb << "level= " << level << Endl;
                sb << ")";
                return sb;
            };
            auto it = tableClient.StreamExecuteScanQuery(query(resourceIds[idx], uids[idx], levels[idx])).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << csController->GetIndexesSkippingOnSelect().Val() << " / " << csController->GetIndexesApprovedOnSelect().Val() << " / " << csController->GetIndexesSkippedNoData().Val() << Endl;
            CompareYson(result, R"([[1u;]])");
        }

        AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() < 0.20 * csController->GetIndexesSkippingOnSelect().Val());

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

}
