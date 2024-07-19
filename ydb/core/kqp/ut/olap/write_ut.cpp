#include "helpers/local.h"
#include "helpers/writer.h"
#include "helpers/typed_local.h"
#include "helpers/query_executor.h"
#include "helpers/get_value.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/wrappers/fake_storage.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapWrite) {
    Y_UNIT_TEST(TierDraftsGC) {
        auto csController = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NYDBTest::NColumnShard::TController>();
        csController->SetIndexWriteControllerEnabled(false);
        csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->ResetWriteCounters();

        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable();
        Tests::NCommon::TLoggerInit(kikimr).SetComponents({NKikimrServices::TX_COLUMNSHARD}, "CS").SetPriority(NActors::NLog::PRI_DEBUG).Initialize();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }
        while (csController->GetInsertStartedCounter().Val() == 0) {
            Cout << "Wait indexation..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        while (!Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() || !csController->GetIndexWriteControllerBrokeCount().Val()) {
            Cout << "Wait errors on write... " << Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() << "/" << csController->GetIndexWriteControllerBrokeCount().Val() << Endl;
            Sleep(TDuration::Seconds(2));
        }
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        const auto startInstant = TMonotonic::Now();
        while (Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() && TMonotonic::Now() - startInstant < TDuration::Seconds(200)) {
            Cerr << "Waiting empty... " << Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() << Endl;
            Sleep(TDuration::Seconds(2));
        }

        AFL_VERIFY(!Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());
    }

    Y_UNIT_TEST(TierDraftsGCWithRestart) {
        auto csController = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NYDBTest::NColumnShard::TController>();
        csController->SetIndexWriteControllerEnabled(false);
        csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1000));
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::GC);
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->ResetWriteCounters();

        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable();
        Tests::NCommon::TLoggerInit(kikimr).SetComponents({NKikimrServices::TX_COLUMNSHARD}, "CS").SetPriority(NActors::NLog::PRI_DEBUG).Initialize();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }
        while (csController->GetInsertStartedCounter().Val() == 0) {
            Cout << "Wait indexation..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        while (Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() < 20 || !csController->GetIndexWriteControllerBrokeCount().Val()) {
            Cout << "Wait errors on write... " << Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() << "/" << csController->GetIndexWriteControllerBrokeCount().Val() << Endl;
            Sleep(TDuration::Seconds(2));
        }
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        AFL_VERIFY(Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());
        {
            const auto startInstant = TMonotonic::Now();
            AFL_VERIFY(Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetDeletesCount() == 0)
                ("count", Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetDeletesCount());
            while (Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() && TMonotonic::Now() - startInstant < TDuration::Seconds(200)) {
                for (auto&& i : csController->GetShardActualIds()) {
                    kikimr.GetTestServer().GetRuntime()->Send(MakePipePerNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                        new TEvents::TEvPoisonPill(), i, false));
                }
                csController->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::GC);
                Cerr << "Waiting empty... " << Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() << Endl;
                Sleep(TDuration::Seconds(2));
            }
        }

        AFL_VERIFY(!Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());
        const auto writesCountStart = Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount();
        const auto deletesCountStart = Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetDeletesCount();
        {
            const auto startInstant = TMonotonic::Now();
            while (TMonotonic::Now() - startInstant < TDuration::Seconds(10)) {
                for (auto&& i : csController->GetShardActualIds()) {
                    kikimr.GetTestServer().GetRuntime()->Send(MakePipePerNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                        new TEvents::TEvPoisonPill(), i, false));
                }
                Cerr << "Waiting empty... " << Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() << "/" << Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetDeletesCount() << Endl;
                Sleep(TDuration::MilliSeconds(500));
            }
        }
        AFL_VERIFY(writesCountStart == Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount())
            ("writes", writesCountStart)("count", Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount());
        AFL_VERIFY(deletesCountStart == Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetDeletesCount())
            ("deletes", deletesCountStart)("count", Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetDeletesCount());
    }

    Y_UNIT_TEST(DefaultValues) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("Utf8", kikimr);
        helper.CreateTestOlapTable();
        helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `ENCODING.DICTIONARY.ENABLED`=`true`, `DEFAULT_VALUE`=`abcde`);");
        helper.FillPKOnly(0, 800000);

        auto selectQuery = TString(R"(
                SELECT
                    count(*) as count,
                FROM `/Root/olapStore/olapTable`
                WHERE field = 'abcde'
            )");

        auto tableClient = kikimr.GetTableClient();
        auto rows = ExecuteScanQuery(tableClient, selectQuery);
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("count")), 800000);
    }

    Y_UNIT_TEST(WriteDeleteCleanGC) {
        auto csController = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NYDBTest::NColumnShard::TController>();
        csController->SetPeriodicWakeupActivationPeriod(TDuration::MilliSeconds(100));
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::GC);
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->ResetWriteCounters();

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);

        auto settings = TKikimrSettings().SetAppConfig(appConfig).SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable();
        Tests::NCommon::TLoggerInit(kikimr).SetComponents({ NKikimrServices::TX_COLUMNSHARD, NKikimrServices::TX_COLUMNSHARD_BLOBS }, "CS").SetPriority(NActors::NLog::PRI_DEBUG).Initialize();
        auto tableClient = kikimr.GetTableClient();

        auto client = kikimr.GetQueryClient();

        {
            auto it = client.ExecuteQuery(R"(
                INSERT INTO `/Root/olapStore/olapTable` (timestamp, uid, resource_id) VALUES (Timestamp('1970-01-01T00:00:00Z'), 'a', '0');
                INSERT INTO `/Root/olapStore/olapTable` (timestamp, uid, resource_id) VALUES (Timestamp('1970-01-01T00:00:01Z'), 'a', 'test');
                INSERT INTO `/Root/olapStore/olapTable` (timestamp, uid, resource_id) VALUES (Timestamp('1970-01-01T00:00:02Z'), 'a', 't');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }

        while (csController->GetInsertStartedCounter().Val() == 0) {
            Cerr << "Wait indexation..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        {
            const TInstant start = TInstant::Now();
            while (!Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() && TInstant::Now() - start < TDuration::Seconds(10)) {
                Cerr << "Wait size in memory... " << Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() << Endl;
                Sleep(TDuration::Seconds(2));
            }
            AFL_VERIFY(Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());
        }
        {
            auto it = client.ExecuteQuery(R"(
                DELETE FROM `/Root/olapStore/olapTable` ON SELECT CAST(0u AS Timestamp) AS timestamp, Unwrap(CAST('a' AS Utf8)) AS uid;
                DELETE FROM `/Root/olapStore/olapTable`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }
        csController->SetReadTimeoutClean(TDuration::Zero());
        csController->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::GC);
        {
            const TInstant start = TInstant::Now();
            while (Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() && TInstant::Now() - start < TDuration::Seconds(10)) {
                Cerr << "Wait empty... " << Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() << Endl;
                Sleep(TDuration::Seconds(2));
            }
            AFL_VERIFY(!Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());
        }
    }

}

} // namespace
