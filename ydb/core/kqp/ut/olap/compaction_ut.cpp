#include "helpers/local.h"
#include "helpers/writer.h"

#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace fmt::literals;

class TCompactionTestHelper {
private:
    std::optional<TTestHelper> TestHelper;
    std::optional<TLocalHelper> OlapHelper;

public:
    TCompactionTestHelper() {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        runnerSettings.SetColumnShardAlterObjectEnabled(true);
        TestHelper.emplace(runnerSettings);
        OlapHelper.emplace(TestHelper->GetKikimr());
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->SetSecretKey("fakeSecret");
    }

    TTestHelper& GetTestHelper() {
        AFL_VERIFY(TestHelper);
        return *TestHelper;
    }

    TLocalHelper& GetOlapHelper() {
        AFL_VERIFY(OlapHelper);
        return *OlapHelper;
    }

    void SetCompaction(const TString& tablePath, uint64_t portionSize) {
        const TString alterQuery = fmt::format(R"(
                ALTER OBJECT `{table_path}` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                        {{"levels" : 
                            [
                                {{"class_name" : "Zero", "expected_blobs_size" : {portion_size}}},
                                {{"class_name" : "Zero", "expected_blobs_size" : {portion_size}}}
                            ]
                        }}`);
            )", 
            "table_path"_a = tablePath, 
            "portion_size"_a = portionSize);
        auto result = GetQueryClient().ExecuteQuery(alterQuery, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    }

    void WriteSampleData(const TString& tablePath, size_t count = 1000) {
        for (ui64 i = 0; i < count; ++i) {
            WriteTestData(TestHelper->GetKikimr(), tablePath, 0, 3600000000 + i * 100000, 1000);
        }
    }

    NYdb::NQuery::TQueryClient GetQueryClient() {
        return GetTestHelper().GetKikimr().GetQueryClient();
    }

    void WaitTierData(const TString& tablePath) {
        NYdb::NTable::TTableClient tableClient = TestHelper->GetKikimr().GetTableClient();
        for (ui64 i = 0; i < 200; ++i) {
            const TString selectQuery = fmt::format(R"(
                    SELECT
                        TierName
                    FROM `{table_path}/.sys/primary_index_portion_stats` 
                    WHERE TierName != "__DEFAULT"
                )", 
                "table_path"_a = tablePath);
            auto result = GetQueryClient().ExecuteQuery(selectQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            if (result.GetResultSet(0).RowsCount() > 0) {
                return;
            }
            Sleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_C(false, "Tier data not found");
    }

    void ValidatePortionTierSize(const TString& tablePath, uint64_t leftBorder, uint64_t rightBorder) {
        const TString selectQuery = fmt::format(R"(
                SELECT
                    TierName, ColumnBlobBytes
                FROM `{table_path}/.sys/primary_index_portion_stats` 
                WHERE TierName != "__DEFAULT"
            )", 
            "table_path"_a = tablePath);
        auto result = GetQueryClient().ExecuteQuery(selectQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        NYdb::TResultSetParser resultSet(result.GetResultSet(0));
        while (resultSet.TryNextRow()) {
            auto tierName = resultSet.ColumnParser(0).GetOptionalUtf8().value_or("");
            auto columnBlobSize = resultSet.ColumnParser(1).GetOptionalUint64().value_or(0);
            UNIT_ASSERT_C(leftBorder <= columnBlobSize && columnBlobSize <= rightBorder, 
                fmt::format("Tier size is not in range for {tier_name}: {left_border} <= {value} <= {right_border}", 
                    "tier_name"_a = tierName,
                    "left_border"_a = leftBorder,
                    "value"_a = columnBlobSize,
                    "right_border"_a = rightBorder));
        }
    }
};

Y_UNIT_TEST_SUITE(KqpOlapCompaction) {
    Y_UNIT_TEST(TieringPortionSize) {
        TCompactionTestHelper compactionHelper;
        auto& testHelper = compactionHelper.GetTestHelper();
        auto& olapHelper = compactionHelper.GetOlapHelper();

        olapHelper.CreateTestOlapStandaloneTable("table1");
        testHelper.CreateTier("tier1");
        testHelper.SetTiering("/Root/table1", "/Root/tier1", "timestamp");
        compactionHelper.SetCompaction("/Root/table1", 3000000);
        compactionHelper.WriteSampleData("/Root/table1", 1000);
        compactionHelper.WaitTierData("/Root/table1");
        compactionHelper.ValidatePortionTierSize("/Root/table1", 0.5 * 3000000, 2 * 3000000);

        olapHelper.CreateTestOlapStandaloneTable("table2");
        testHelper.CreateTier("tier2");
        testHelper.SetTiering("/Root/table2", "/Root/tier2", "timestamp");
        compactionHelper.SetCompaction("/Root/table2", 1000000);
        compactionHelper.WriteSampleData("/Root/table2", 1000);
        compactionHelper.WaitTierData("/Root/table2");
        compactionHelper.ValidatePortionTierSize("/Root/table2", 0.5 * 1000000, 2 * 1000000);
    }
}

}   // namespace NKikimr::NKqp