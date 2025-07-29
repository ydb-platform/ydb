#include "helpers/get_value.h"
#include "helpers/local.h"
#include "helpers/query_executor.h"
#include "helpers/writer.h"

#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/fake_storage.h>

namespace NKikimr::NKqp {

static const TString DEFAULT_TABLE_NAME = "/Root/olapStore/olapTable";
static const TString DEFAULT_TIER_NAME = "/Root/tier1";
static const TString DEFAULT_COLUMN_NAME = "timestamp";

class TAbortedWriteCounterController final: public NOlap::TWaitCompactionController {
public:
    ui64 GetAbortedWrites() const {
        return AbortedWrites.load();
    }

protected:
    bool DoOnWriteIndexComplete(const NOlap::TColumnEngineChanges& change, const ::NKikimr::NColumnShard::TColumnShard& shard) override {
        if (change.IsAborted()) {
            ++AbortedWrites;
        }

        return NOlap::TWaitCompactionController::DoOnWriteIndexComplete(change, shard);
    }

private:
    std::atomic<ui64> AbortedWrites{ 0 };
};

template <class TCtrl = NOlap::TWaitCompactionController>
class TTieringTestHelper {
private:
    using TCtrlGuard = NYDBTest::TControllers::TGuard<TCtrl>;
    std::optional<TTestHelper> TestHelper;
    std::optional<TLocalHelper> OlapHelper;
    std::optional<TCtrlGuard> CsController;

    YDB_ACCESSOR(TString, TablePath, DEFAULT_TABLE_NAME);

public:
    TTieringTestHelper() {
        CsController.emplace(NYDBTest::TControllers::RegisterCSControllerGuard<TCtrl>());
        (*CsController)->SetSkipSpecialCheckForEvict(true);

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TestHelper.emplace(runnerSettings);
        OlapHelper.emplace(TestHelper->GetKikimr());
        TestHelper->GetRuntime().SetLogPriority(NKikimrServices::TX_TIERING, NActors::NLog::PRI_DEBUG);
        TestHelper->GetRuntime().SetLogPriority(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION, NActors::NLog::PRI_DEBUG);
        TestHelper->GetRuntime().SetLogPriority(NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER, NActors::NLog::PRI_DEBUG);
        Tests::NCommon::TLoggerInit(TestHelper->GetKikimr()).Initialize();
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

    TCtrlGuard& GetCsController() {
        AFL_VERIFY(CsController);
        return *CsController;
    }

    void WriteSampleData() {
        for (ui64 i = 0; i < 100; ++i) {
            WriteTestData(TestHelper->GetKikimr(), TablePath, 0, 3600000000 + i * 10000, 1000);
            WriteTestData(TestHelper->GetKikimr(), TablePath, 0, 3600000000 + i * 10000, 1000);
        }
    }

    void CheckAllDataInTier(const TString& tierName, const bool onlyActive = true) {
        NYdb::NTable::TTableClient tableClient = TestHelper->GetKikimr().GetTableClient();

        auto selectQuery = TStringBuilder();
        selectQuery << R"(
            SELECT
                TierName, SUM(ColumnRawBytes) AS RawBytes, SUM(Rows) AS Rows
            FROM `)" << TablePath
                    << R"(/.sys/primary_index_portion_stats`)";
        if (onlyActive) {
            selectQuery << " WHERE Activity == 1";
        }
        selectQuery << " GROUP BY TierName";

        auto rows = ExecuteScanQuery(tableClient, selectQuery);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("TierName")), tierName);
    }
};

Y_UNIT_TEST_SUITE(KqpOlapTiering) {
    Y_UNIT_TEST(EvictionResetTiering) {
        TTieringTestHelper tieringHelper;
        auto& csController = tieringHelper.GetCsController();
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();

        olapHelper.CreateTestOlapTable();
        testHelper.CreateTier("tier1");
        tieringHelper.WriteSampleData();
        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier("__DEFAULT");

        testHelper.SetTiering(DEFAULT_TABLE_NAME, DEFAULT_TIER_NAME, DEFAULT_COLUMN_NAME);
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier(DEFAULT_TIER_NAME);

        testHelper.ResetTiering(DEFAULT_TABLE_NAME);
        csController->WaitCompactions(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier("__DEFAULT");
    }

    Y_UNIT_TEST(EvictionIncreaseDuration) {
        TTieringTestHelper tieringHelper;
        auto& csController = tieringHelper.GetCsController();
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();

        olapHelper.CreateTestOlapTable();
        testHelper.CreateTier("tier1");
        tieringHelper.WriteSampleData();
        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier("__DEFAULT");

        testHelper.SetTiering(DEFAULT_TABLE_NAME, DEFAULT_TIER_NAME, DEFAULT_COLUMN_NAME);
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier(DEFAULT_TIER_NAME);

        {
            const TString query =
                R"(ALTER TABLE `/Root/olapStore/olapTable` SET TTL Interval("P30000D") TO EXTERNAL DATA SOURCE `/Root/tier1` ON timestamp)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        csController->WaitCompactions(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier("__DEFAULT");
    }

    Y_UNIT_TEST(LoadTtlSettings) {
        TTieringTestHelper tieringHelper;
        auto& csController = tieringHelper.GetCsController();
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();
        tieringHelper.SetTablePath("/Root/olapTable");

        olapHelper.CreateTestOlapStandaloneTable();
        testHelper.CreateTier("tier1");
        testHelper.SetTiering("/Root/olapTable", DEFAULT_TIER_NAME, DEFAULT_COLUMN_NAME);
        {
            const TString query = R"(ALTER TABLE `/Root/olapTable` ADD COLUMN f Int32)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }
        testHelper.RebootTablets("/Root/olapTable");

        tieringHelper.WriteSampleData();
        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier(DEFAULT_TIER_NAME);
    }

    Y_UNIT_TEST(EvictionWithStrippedEdsPath) {
        TTieringTestHelper tieringHelper;
        auto& csController = tieringHelper.GetCsController();
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();

        olapHelper.CreateTestOlapTable();
        testHelper.CreateTier("tier1");
        tieringHelper.WriteSampleData();

        testHelper.SetTiering(DEFAULT_TABLE_NAME, DEFAULT_TIER_NAME, DEFAULT_COLUMN_NAME);
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier(DEFAULT_TIER_NAME);
    }

    Y_UNIT_TEST(TieringValidation) {
        TTieringTestHelper tieringHelper;
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();

        olapHelper.CreateTestOlapTable();
        testHelper.CreateTier("tier1");

        {
            const TString query =
                R"(ALTER TABLE `/Root/olapStore/olapTable` SET TTL Interval("P10D") TO EXTERNAL DATA SOURCE `/Root/tier1` ON unknown_column;)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        {
            const TString query =
                R"(ALTER TABLE `/Root/olapStore/olapTable` SET TTL Interval("P10D") TO EXTERNAL DATA SOURCE `/Root/tier1` ON uid;)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        testHelper.SetTiering(DEFAULT_TABLE_NAME, DEFAULT_TIER_NAME, DEFAULT_COLUMN_NAME);
    }

    Y_UNIT_TEST(DeletedTier) {
        TTieringTestHelper tieringHelper;
        auto& csController = tieringHelper.GetCsController();
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();
        NYdb::NTable::TTableClient tableClient = testHelper.GetKikimr().GetTableClient();

        olapHelper.CreateTestOlapTable();
        testHelper.CreateTier("tier1");
        tieringHelper.WriteSampleData();
        testHelper.SetTiering(DEFAULT_TABLE_NAME, DEFAULT_TIER_NAME, DEFAULT_COLUMN_NAME);
        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));

        csController->DisableBackground(NYDBTest::ICSController::EBackground::TTL);
        testHelper.ResetTiering(DEFAULT_TABLE_NAME);
        testHelper.RebootTablets(DEFAULT_TABLE_NAME);
        tieringHelper.CheckAllDataInTier(DEFAULT_TIER_NAME);

        TString selectQuery = R"(SELECT MAX(level) AS level FROM `/Root/olapStore/olapTable`)";
        ui64 scanResult;
        {
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
            scanResult = GetInt32(rows[0].at("level"));
        }

        {
            auto result = testHelper.GetSession().ExecuteSchemeQuery(R"(DROP EXTERNAL DATA SOURCE `/Root/tier1`)").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        testHelper.RebootTablets(DEFAULT_TABLE_NAME);

        {
            auto it = tableClient.StreamExecuteScanQuery(selectQuery, NYdb::NTable::TStreamExecScanQuerySettings()).GetValueSync();
            auto streamPart = it.ReadNext().GetValueSync();
            UNIT_ASSERT(!streamPart.IsSuccess());
            UNIT_ASSERT_STRING_CONTAINS(streamPart.GetIssues().ToString(), "Error reading blob range");
        }

        testHelper.CreateTier("tier1");
        testHelper.RebootTablets(DEFAULT_TABLE_NAME);

        {
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetInt32(rows[0].at("level")), scanResult);
        }
    }

    Y_UNIT_TEST(TtlBorders) {
        TTieringTestHelper tieringHelper;
        auto& csController = tieringHelper.GetCsController();
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();
        NYdb::NTable::TTableClient tableClient = testHelper.GetKikimr().GetTableClient();

        olapHelper.CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        {
            const TDuration tsInterval = TDuration::Days(3650);
            const ui64 rows = 10000;
            WriteTestData(testHelper.GetKikimr(), DEFAULT_TABLE_NAME, 0, (TInstant::Now() - tsInterval).MicroSeconds(), rows, false,
                tsInterval.MicroSeconds() / rows);
        }

        //         {
        //             auto selectQuery = TString(R"(
        //                 SELECT MAX(timestamp) AS timestamp FROM `/Root/olapStore/olapTable`
        //             )");
        //
        //             auto rows = ExecuteScanQuery(tableClient, selectQuery);
        //             UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        //             UNIT_ASSERT_GT(GetTimestamp(rows[0].at(DEFAULT_COLUMN_NAME)), TInstant::Now() - TDuration::Days(100));
        //         }

        {
            auto selectQuery = TString(R"(
                SELECT COUNT(*) AS count FROM `/Root/olapStore/olapTable/.sys/primary_index_portion_stats`
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("count")), 1);
        }

        {
            const TString query = R"(ALTER TABLE `/Root/olapStore/olapTable` SET TTL Interval("P300D") ON timestamp)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));

        {
            auto selectQuery = TString(R"(
                SELECT COUNT(*) AS count FROM `/Root/olapStore/olapTable`
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
            UNIT_ASSERT_GT(GetUint64(rows[0].at("count")), 0);
        }
    }

    Y_UNIT_TEST(LocksInterference) {
        TTieringTestHelper tieringHelper;
        auto& csController = tieringHelper.GetCsController();
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();
        const auto& kikimr = testHelper.GetKikimr();
        NYdb::NTable::TTableClient tableClient = kikimr.GetTableClient();

        olapHelper.CreateTestOlapTable();
        const auto describeResult = kikimr.GetTestClient().Describe(kikimr.GetTestServer().GetRuntime(), "Root/olapStore/olapTable");
        const auto tablePathId = NColumnShard::TSchemeShardLocalPathId::FromRawValue(describeResult.GetPathId());

        tieringHelper.WriteSampleData();
        csController->WaitCompactions(TDuration::Seconds(5));
        THashSet<NColumnShard::TInternalPathId> pathsToLock;
        for (const auto& [_, pathIdTranslator] : csController->GetActiveTablets()) {
            if (auto internalPathId = pathIdTranslator->ResolveInternalPathId(tablePathId, false)) {
                pathsToLock.insert(*internalPathId);
            }
        };

        csController->RegisterLock("table",
            std::make_shared<NOlap::NDataLocks::TListTablesLock>("table", std::move(pathsToLock), NOlap::NDataLocks::ELockCategory::Compaction));
        {
            const TString query = R"(ALTER TABLE `/Root/olapStore/olapTable` SET TTL Interval("PT1S") ON timestamp)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier("__DEFAULT");

        csController->UnregisterLock("table");
        csController->EnableBackground(NYDBTest::ICSController::EBackground::TTL);
        csController->WaitActualization(TDuration::Seconds(5));
        csController->WaitTtl(TDuration::Seconds(5));
        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/olapTable/.sys/primary_index_portion_stats`
                WHERE Activity == 1
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 0);
        }
    }

    Y_UNIT_TEST(TieringGC) {
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->Clear();
        TTieringTestHelper tieringHelper;
        auto& csController = tieringHelper.GetCsController();
        csController->SetOverrideMaxReadStaleness(TDuration::Seconds(1));
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();

        olapHelper.CreateTestOlapTable();
        testHelper.CreateTier("tier1");
        tieringHelper.WriteSampleData();
        testHelper.SetTiering(DEFAULT_TABLE_NAME, DEFAULT_TIER_NAME, DEFAULT_COLUMN_NAME);
        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier(DEFAULT_TIER_NAME, false);
        UNIT_ASSERT_GT(Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucket("olap-tier1").GetSize(), 0);

        csController->DisableBackground(NYDBTest::ICSController::EBackground::GC);
        testHelper.ResetTiering(DEFAULT_TABLE_NAME);
        csController->WaitActualization(TDuration::Seconds(5));

        tieringHelper.CheckAllDataInTier("__DEFAULT", false);
        UNIT_ASSERT_GT(Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucket("olap-tier1").GetSize(), 0);

        csController->EnableBackground(NYDBTest::ICSController::EBackground::GC);
        csController->SetExternalStorageUnavailable(true);
        testHelper.ResetTiering(DEFAULT_TABLE_NAME);
        csController->WaitCleaning(TDuration::Seconds(5));
        UNIT_ASSERT_GT(Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucket("olap-tier1").GetSize(), 0);

        csController->SetExternalStorageUnavailable(false);
        testHelper.ResetTiering(DEFAULT_TABLE_NAME);
        csController->WaitCondition(TDuration::Seconds(60), []() {
            return Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucket("olap-tier1").GetSize() == 0;
        });
    }

    Y_UNIT_TEST(NoBackoffUnavailableS3) {
        TTieringTestHelper<TAbortedWriteCounterController> tieringHelper;
        auto& putController = tieringHelper.GetCsController();
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();

        olapHelper.CreateTestOlapTable();
        testHelper.CreateTier("tier1");
        tieringHelper.WriteSampleData();
        putController->WaitCompactions(TDuration::Seconds(5));

        putController->SetExternalStorageUnavailable(true);
        testHelper.SetTiering(DEFAULT_TABLE_NAME, DEFAULT_TIER_NAME, DEFAULT_COLUMN_NAME);

        putController->WaitActualization(TDuration::Seconds(5));
        Sleep(TDuration::Seconds(5));

        UNIT_ASSERT_C(putController->GetAbortedWrites() > 20,
            "Expected load spike, but only " << putController->GetAbortedWrites() << " PutObject requests recorded");   // comment after fix
        // UNIT_ASSERT_C(putController->GetAbortedWrites() < 10,
        //               "Expected load spike, but was "
        //               << putController->GetAbortedWrites() << " PutObject requests recorded"); // uncomment after fix
    }
}

}   // namespace NKikimr::NKqp
