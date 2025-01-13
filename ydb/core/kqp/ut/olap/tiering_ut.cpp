#include "helpers/get_value.h"
#include "helpers/local.h"
#include "helpers/query_executor.h"
#include "helpers/typed_local.h"
#include "helpers/writer.h"

#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/wrappers/fake_storage.h>

namespace NKikimr::NKqp {

class TTestEvictionBase {
protected:
    std::optional<TTestHelper> TestHelper;
    TString TieringRule;

protected:
    virtual void UnevictAll() = 0;

public:
    void RunTest() {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csController->SetSkipSpecialCheckForEvict(true);

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TestHelper.emplace(runnerSettings);
        TLocalHelper localHelper(TestHelper->GetKikimr());
        // TestHelper->GetRuntime().SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        // TestHelper->GetRuntime().SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);
        TestHelper->GetRuntime().SetLogPriority(NKikimrServices::TX_TIERING, NActors::NLog::PRI_DEBUG);
        // TestHelper->GetRuntime().SetLogPriority(NKikimrServices::KQP_GATEWAY, NActors::NLog::PRI_DEBUG);
        // TestHelper->GetRuntime().SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_DEBUG);
        // TestHelper->GetRuntime().SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
        NYdb::NTable::TTableClient tableClient = TestHelper->GetKikimr().GetTableClient();
        Tests::NCommon::TLoggerInit(TestHelper->GetKikimr()).Initialize();
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->SetSecretKey("fakeSecret");

        localHelper.CreateTestOlapTable();
        TestHelper->CreateTier("tier1");

        for (ui64 i = 0; i < 100; ++i) {
            WriteTestData(TestHelper->GetKikimr(), "/Root/olapStore/olapTable", 0, 3600000000 + i * 10000, 1000);
            WriteTestData(TestHelper->GetKikimr(), "/Root/olapStore/olapTable", 0, 3600000000 + i * 10000, 1000);
        }

        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));

        ui64 columnRawBytes = 0;
        {
            auto selectQuery = TString(R"(
                SELECT
                    TierName, SUM(ColumnRawBytes) As RawBytes
                FROM `/Root/olapStore/olapTable/.sys/primary_index_portion_stats`
                WHERE Activity == 1
                GROUP BY TierName
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("TierName")), "__DEFAULT");

            columnRawBytes = GetUint64(rows[0].at("RawBytes"));
            UNIT_ASSERT_GT(columnRawBytes, 0);
        }

        TestHelper->SetTiering("/Root/olapStore/olapTable", "/Root/tier1", "timestamp");
        csController->WaitActualization(TDuration::Seconds(5));

        {
            auto selectQuery = TString(R"(
                SELECT
                    TierName, SUM(ColumnRawBytes) As RawBytes
                FROM `/Root/olapStore/olapTable/.sys/primary_index_portion_stats`
                WHERE Activity == 1
                GROUP BY TierName
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("TierName")), "/Root/tier1");
            UNIT_ASSERT_VALUES_EQUAL_C(GetUint64(rows[0].at("RawBytes")), columnRawBytes,
                TStringBuilder() << "RawBytes changed after eviction: before=" << columnRawBytes
                                << " after=" << GetUint64(rows[0].at("RawBytes")));
        }

        UnevictAll();
        csController->WaitCompactions(TDuration::Seconds(5));

        {
            auto selectQuery = TString(R"(
                SELECT
                    TierName, SUM(ColumnRawBytes) As RawBytes
                FROM `/Root/olapStore/olapTable/.sys/primary_index_portion_stats`
                WHERE Activity == 1
                GROUP BY TierName
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("TierName")), "__DEFAULT");
            UNIT_ASSERT_VALUES_EQUAL_C(GetUint64(rows[0].at("RawBytes")), columnRawBytes,
                TStringBuilder() << "RawBytes changed after resetting tiering: before=" << columnRawBytes
                                << " after=" << GetUint64(rows[0].at("RawBytes")));
        }

    }
};

class TTestEvictionResetTiering : public TTestEvictionBase {
    private:
    void UnevictAll() {
        TestHelper->ResetTiering("/Root/olapStore/olapTable");
    }
};

class TTestEvictionIncreaseDuration : public TTestEvictionBase {
    private:
    void UnevictAll() {
        const TString query = R"(ALTER TABLE `/Root/olapStore/olapTable` SET TTL Interval("P30000D") TO EXTERNAL DATA SOURCE `/Root/tier1` ON timestamp)";
        auto result = TestHelper->GetSession().ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    }
};

Y_UNIT_TEST_SUITE(KqpOlapTiering) {

    Y_UNIT_TEST(EvictionResetTiering) {
        TTestEvictionResetTiering().RunTest();
    }

    Y_UNIT_TEST(EvictionIncreaseDuration) {
        TTestEvictionIncreaseDuration().RunTest();
    }

    Y_UNIT_TEST(TieringValidation) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);
        TLocalHelper localHelper(testHelper.GetKikimr());
        testHelper.GetRuntime().SetLogPriority(NKikimrServices::TX_TIERING, NActors::NLog::PRI_DEBUG);
        NYdb::NTable::TTableClient tableClient = testHelper.GetKikimr().GetTableClient();
        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).Initialize();

        localHelper.CreateTestOlapTable();
        testHelper.CreateTier("tier1");

        {
            const TString query = R"(ALTER TABLE `/Root/olapStore/olapTable` SET TTL Interval("P10D") TO EXTERNAL DATA SOURCE `/Root/tier1` ON unknown_column;)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        {
            const TString query = R"(ALTER TABLE `/Root/olapStore/olapTable` SET TTL Interval("P10D") TO EXTERNAL DATA SOURCE `/Root/tier1` ON uid;)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        testHelper.SetTiering("/Root/olapStore/olapTable", "/Root/tier1", "timestamp");
    }
}

}   // namespace NKikimr::NKqp
