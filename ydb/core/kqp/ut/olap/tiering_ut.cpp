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
        TieringRule = TestHelper->CreateTieringRule("tier1", "timestamp");

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

        TestHelper->SetTiering("/Root/olapStore/olapTable", TieringRule);
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
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("TierName")), "tier1");
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
        const TString query = R"(ALTER OBJECT )" + TieringRule + R"( (TYPE TIERING_RULE)
            SET (description = `{
                "rules" : [
                    {
                        "tierName" : "tier1",
                        "durationForEvict" : "100000000000d"
                    }
                ]
            }`))";
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

    Y_UNIT_TEST(TieringRuleValidation) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);
        TLocalHelper localHelper(testHelper.GetKikimr());
        // testHelper.GetRuntime().SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        testHelper.GetRuntime().SetLogPriority(NKikimrServices::TX_TIERING, NActors::NLog::PRI_DEBUG);
        // testHelper.GetRuntime().SetLogPriority(NKikimrServices::KQP_GATEWAY, NActors::NLog::PRI_DEBUG);
        // testHelper.GetRuntime().SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_DEBUG);
        // testHelper.GetRuntime().SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
        NYdb::NTable::TTableClient tableClient = testHelper.GetKikimr().GetTableClient();
        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).Initialize();
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->SetSecretKey("fakeSecret");

        localHelper.CreateTestOlapTable();
        testHelper.CreateTier("tier1");

        {
            const TString query = R"(ALTER TABLE `/Root/olapStore/olapTable` SET TIERING = "unknown_tiering";)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        {
            const TString query = R"(
            CREATE OBJECT IF NOT EXISTS empty_tiering_rule (TYPE TIERING_RULE)
                WITH (defaultColumn = timestamp, description = `{"rules": []}`))";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        {
            const TString query = R"(
            CREATE OBJECT IF NOT EXISTS empty_default_column (TYPE TIERING_RULE)
                WITH (defaultColumn = ``, description = `{"rules": [{ "tierName" : "tier1", "durationForEvict" : "10d" }]}`))";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        {
            const TString query = R"(
            CREATE OBJECT IF NOT EXISTS no_default_column (TYPE TIERING_RULE)
                WITH (description = `{"rules": [{ "tierName" : "tier1", "durationForEvict" : "10d" }]}`))";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        {
            TString query = R"(
            CREATE OBJECT IF NOT EXISTS wrong_default_column (TYPE TIERING_RULE)
                WITH (defaultColumn = `unknown_column`, description = `{"rules": [{ "tierName" : "tier1", "durationForEvict" : "10d" }]}`))";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);

            query = R"(ALTER TABLE `/Root/olapStore/olapTable` SET TIERING = "wrong_default_column";)";
            result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        const TString correctTieringRule = testHelper.CreateTieringRule("tier1", "timestamp");
        {
            const TString query = "ALTER OBJECT " + correctTieringRule + R"( (TYPE TIERING_RULE) SET description `{"rules": []}`)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        {
            const TString query = "ALTER OBJECT " + correctTieringRule + R"( (TYPE TIERING_RULE) SET description `{"rules": []}`)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        {
            const TString query = "ALTER OBJECT " + correctTieringRule + R"( (TYPE TIERING_RULE) SET defaultColumn ``)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        {
            const TString query = "ALTER OBJECT " + correctTieringRule + R"( (TYPE TIERING_RULE) RESET defaultColumn)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        {
            const TString query = "DROP OBJECT tier1 (TYPE TIER)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
        }
    }
}

}   // namespace NKikimr::NKqp
