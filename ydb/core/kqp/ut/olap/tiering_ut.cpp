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
#include <ydb/core/util/aws.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <library/cpp/testing/hook/hook.h>

namespace NKikimr::NKqp {

static const TString DEFAULT_TABLE_PATH = "/Root/olapStore/olapTable";
static const TString DEFAULT_TIER_NAME = "tier1";
static const TString DEFAULT_TIER_PATH = "/Root/tier1";
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

    YDB_ACCESSOR(TString, TablePath, DEFAULT_TABLE_PATH);

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
        for (ui64 i = 0; i < 400; ++i) {
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

Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
    NKikimr::InitAwsAPI();
}

Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
    NKikimr::ShutdownAwsAPI();
}

Y_UNIT_TEST_SUITE(KqpOlapTiering) {
    Y_UNIT_TEST(EvictionResetTiering) {
        TTieringTestHelper tieringHelper;
        auto& csController = tieringHelper.GetCsController();
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();

        olapHelper.CreateTestOlapTable();
        testHelper.CreateTier(DEFAULT_TIER_NAME);
        tieringHelper.WriteSampleData();
        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier("__DEFAULT");

        testHelper.SetTiering(DEFAULT_TABLE_PATH, DEFAULT_TIER_PATH, DEFAULT_COLUMN_NAME);
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier(DEFAULT_TIER_PATH);

        testHelper.ResetTiering(DEFAULT_TABLE_PATH);
        csController->WaitCompactions(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier("__DEFAULT");
    }

    Y_UNIT_TEST(EvictionIncreaseDuration) {
        TTieringTestHelper tieringHelper;
        auto& csController = tieringHelper.GetCsController();
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();

        olapHelper.CreateTestOlapTable();
        testHelper.CreateTier(DEFAULT_TIER_NAME);
        tieringHelper.WriteSampleData();
        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier("__DEFAULT");

        testHelper.SetTiering(DEFAULT_TABLE_PATH, DEFAULT_TIER_PATH, DEFAULT_COLUMN_NAME);
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier(DEFAULT_TIER_PATH);

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
        tieringHelper.GetTestHelper().GetKikimr().GetTestServer().GetRuntime()->GetAppData().ColumnShardConfig.SetBulkUpsertRequireAllColumns(false);
        olapHelper.CreateTestOlapStandaloneTable();
        testHelper.CreateTier(DEFAULT_TIER_NAME);
        testHelper.SetTiering("/Root/olapTable", DEFAULT_TIER_PATH, DEFAULT_COLUMN_NAME);

        {
            const TString query = R"(ALTER TABLE `/Root/olapTable` ADD COLUMN f Int32)";
            auto result = testHelper.GetSession().ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        for (ui64 i = 0; i < 100; ++i) {
            WriteTestData(testHelper.GetKikimr(), "/Root/olapTable", 0, 3600000000 + i * 10000, 1000);
            WriteTestData(testHelper.GetKikimr(), "/Root/olapTable", 0, 3600000000 + i * 10000, 1000);
        }

        testHelper.RebootTablets("/Root/olapTable");
        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));

        NYdb::NTable::TTableClient tableClient = testHelper.GetKikimr().GetTableClient();
        auto selectQuery = TStringBuilder();
        selectQuery << R"(
            SELECT
                TierName, SUM(ColumnRawBytes) AS RawBytes, SUM(Rows) AS Rows
            FROM `/Root/olapTable/.sys/primary_index_portion_stats`
            WHERE Activity == 1
            GROUP BY TierName)";

        auto rows = ExecuteScanQuery(tableClient, selectQuery);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("TierName")), DEFAULT_TIER_PATH);
    }

    Y_UNIT_TEST(EvictionWithStrippedEdsPath) {
        TTieringTestHelper tieringHelper;
        auto& csController = tieringHelper.GetCsController();
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();

        olapHelper.CreateTestOlapTable();
        testHelper.CreateTier(DEFAULT_TIER_NAME);
        tieringHelper.WriteSampleData();

        testHelper.SetTiering(DEFAULT_TABLE_PATH, DEFAULT_TIER_PATH, DEFAULT_COLUMN_NAME);
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier(DEFAULT_TIER_PATH);
    }

    Y_UNIT_TEST(TieringValidation) {
        TTieringTestHelper tieringHelper;
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();

        olapHelper.CreateTestOlapTable();
        testHelper.CreateTier(DEFAULT_TIER_NAME);

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

        testHelper.SetTiering(DEFAULT_TABLE_PATH, DEFAULT_TIER_PATH, DEFAULT_COLUMN_NAME);
    }

    Y_UNIT_TEST(DeletedTier) {
        TTieringTestHelper tieringHelper;
        auto& csController = tieringHelper.GetCsController();
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();
        NYdb::NTable::TTableClient tableClient = testHelper.GetKikimr().GetTableClient();

        olapHelper.CreateTestOlapTable();
        testHelper.CreateTier(DEFAULT_TIER_NAME);
        tieringHelper.WriteSampleData();
        testHelper.SetTiering(DEFAULT_TABLE_PATH, DEFAULT_TIER_PATH, DEFAULT_COLUMN_NAME);
        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));

        csController->DisableBackground(NYDBTest::ICSController::EBackground::TTL);
        testHelper.ResetTiering(DEFAULT_TABLE_PATH);
        testHelper.RebootTablets(DEFAULT_TABLE_PATH);
        tieringHelper.CheckAllDataInTier(DEFAULT_TIER_PATH);

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

        testHelper.RebootTablets(DEFAULT_TABLE_PATH);

        {
            auto it = tableClient.StreamExecuteScanQuery(selectQuery, NYdb::NTable::TStreamExecScanQuerySettings()).GetValueSync();
            auto streamPart = it.ReadNext().GetValueSync();
            UNIT_ASSERT(!streamPart.IsSuccess());
            UNIT_ASSERT_STRING_CONTAINS(streamPart.GetIssues().ToString(), "/Root/tier1");
        }

        testHelper.CreateTier(DEFAULT_TIER_NAME);
        testHelper.RebootTablets(DEFAULT_TABLE_PATH);

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
            WriteTestData(testHelper.GetKikimr(), DEFAULT_TABLE_PATH, 0, (TInstant::Now() - tsInterval).MicroSeconds(), rows, false,
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
        testHelper.CreateTier(DEFAULT_TIER_NAME);
        tieringHelper.WriteSampleData();
        testHelper.SetTiering(DEFAULT_TABLE_PATH, DEFAULT_TIER_PATH, DEFAULT_COLUMN_NAME);
        csController->WaitCompactions(TDuration::Seconds(5));
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier(DEFAULT_TIER_PATH, false);
        UNIT_ASSERT_GT(Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucket("olap-tier1").GetSize(), 0);

        csController->DisableBackground(NYDBTest::ICSController::EBackground::GC);
        testHelper.ResetTiering(DEFAULT_TABLE_PATH);
        csController->WaitActualization(TDuration::Seconds(5));

        tieringHelper.CheckAllDataInTier("__DEFAULT", false);
        UNIT_ASSERT_GT(Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucket("olap-tier1").GetSize(), 0);

        csController->EnableBackground(NYDBTest::ICSController::EBackground::GC);
        csController->SetExternalStorageUnavailable(true);
        testHelper.ResetTiering(DEFAULT_TABLE_PATH);
        csController->WaitCleaning(TDuration::Seconds(5));
        UNIT_ASSERT_GT(Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucket("olap-tier1").GetSize(), 0);

        csController->SetExternalStorageUnavailable(false);
        testHelper.ResetTiering(DEFAULT_TABLE_PATH);
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
        testHelper.CreateTier(DEFAULT_TIER_NAME);
        tieringHelper.WriteSampleData();
        putController->WaitCompactions(TDuration::Seconds(5));

        putController->SetExternalStorageUnavailable(true);
        testHelper.SetTiering(DEFAULT_TABLE_PATH, DEFAULT_TIER_PATH, DEFAULT_COLUMN_NAME);

        putController->WaitActualization(TDuration::Seconds(5));
        Sleep(TDuration::Seconds(5));

        UNIT_ASSERT_C(putController->GetAbortedWrites() > 20,
            "Expected load spike, but only " << putController->GetAbortedWrites() << " PutObject requests recorded");   // comment after fix
        // UNIT_ASSERT_C(putController->GetAbortedWrites() < 10,
        //               "Expected load spike, but was "
        //               << putController->GetAbortedWrites() << " PutObject requests recorded"); // uncomment after fix
    }

    Y_UNIT_TEST(ReconfigureTier) {
        TTieringTestHelper tieringHelper;
        auto& csController = tieringHelper.GetCsController();
        auto& olapHelper = tieringHelper.GetOlapHelper();
        auto& testHelper = tieringHelper.GetTestHelper();
        NYdb::NTable::TTableClient tableClient = testHelper.GetKikimr().GetTableClient();

        olapHelper.CreateTestOlapTable();
        testHelper.CreateTier(DEFAULT_TIER_NAME);
        tieringHelper.WriteSampleData();
        csController->WaitCompactions(TDuration::Seconds(5));
        testHelper.SetTiering(DEFAULT_TABLE_PATH, DEFAULT_TIER_PATH, DEFAULT_COLUMN_NAME);
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier(DEFAULT_TIER_PATH);

        testHelper.ResetTiering(DEFAULT_TABLE_PATH);
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier("__DEFAULT");

        {
            auto result = testHelper.GetSession().ExecuteSchemeQuery(R"(DROP EXTERNAL DATA SOURCE `/Root/tier1`)").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testHelper.GetSession().ExecuteSchemeQuery(R"(
                UPSERT OBJECT `accessKey` (TYPE SECRET) WITH (value = `secretAccessKey`);
                UPSERT OBJECT `secretKey` (TYPE SECRET) WITH (value = `fakeSecret`);
                CREATE EXTERNAL DATA SOURCE `)" + DEFAULT_TIER_PATH + R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="http://fake.fake/olap-another-bucket",
                    AUTH_METHOD="AWS",
                    AWS_ACCESS_KEY_ID_SECRET_NAME="accessKey",
                    AWS_SECRET_ACCESS_KEY_SECRET_NAME="secretKey",
                    AWS_REGION="ru-central1"
            );
            )").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        testHelper.SetTiering(DEFAULT_TABLE_PATH, DEFAULT_TIER_PATH, DEFAULT_COLUMN_NAME);
        csController->WaitActualization(TDuration::Seconds(5));
        tieringHelper.CheckAllDataInTier(DEFAULT_TIER_PATH);
        UNIT_ASSERT_GT(Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucket("olap-another-bucket").GetSize(), 0);
    }
}

}   // namespace NKikimr::NKqp
