#include "combinatory/variator.h"
#include "helpers/get_value.h"
#include "helpers/local.h"
#include "helpers/query_executor.h"
#include "helpers/typed_local.h"
#include "helpers/writer.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapWrite) {
    Y_UNIT_TEST(WriteFails) {
        auto csController = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NOlap::TWaitCompactionController>();
        csController->SetSmallSizeDetector(1000000);
        csController->SetIndexWriteControllerEnabled(false);
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideBlobPutResultOnWriteValue(NKikimrProto::EReplyStatus::BLOCKED);
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->ResetWriteCounters();

        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        kikimr.GetTestServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableWritePortionsOnInsert(true);
        TLocalHelper(kikimr).CreateTestOlapTable();
        Tests::NCommon::TLoggerInit(kikimr)
            .SetComponents({ NKikimrServices::TX_COLUMNSHARD }, "CS")
            .SetPriority(NActors::NLog::PRI_DEBUG)
            .Initialize();
        {
            auto batch = TLocalHelper(kikimr).TestArrowBatch(30000, 1000000, 11000);
            TLocalHelper(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", batch, Ydb::StatusIds::INTERNAL_ERROR);
        }
    }

    Y_UNIT_TEST(TierDraftsGC) {
        auto csController = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NOlap::TWaitCompactionController>();
        csController->SetSmallSizeDetector(1000000);
        csController->SetIndexWriteControllerEnabled(false);
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->ResetWriteCounters();

        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto helper = TLocalHelper(kikimr);
        helper.CreateTestOlapTable();
        helper.SetForcedCompaction();

        Tests::NCommon::TLoggerInit(kikimr)
            .SetComponents({ NKikimrServices::TX_COLUMNSHARD }, "CS")
            .SetPriority(NActors::NLog::PRI_DEBUG)
            .Initialize();
        auto tableClient = kikimr.GetTableClient();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        while (csController->GetCompactionStartedCounter().Val() == 0) {
            Cout << "Wait compaction..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        while (!Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() ||
               !csController->GetIndexWriteControllerBrokeCount().Val()) {
            Cout << "Wait errors on write... " << Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() << "/"
                 << csController->GetIndexWriteControllerBrokeCount().Val() << Endl;
            Sleep(TDuration::Seconds(2));
        }
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        const auto startInstant = TMonotonic::Now();
        while (Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() &&
               TMonotonic::Now() - startInstant < TDuration::Seconds(200)) {
            Cerr << "Waiting empty... " << Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() << Endl;
            Sleep(TDuration::Seconds(2));
        }

        AFL_VERIFY(!Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());
    }

    Y_UNIT_TEST(TestRemoveTableBeforeIndexation) {
        auto csController = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NYDBTest::NColumnShard::TController>();
        csController->SetIndexWriteControllerEnabled(false);
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);

        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable();
        Tests::NCommon::TLoggerInit(kikimr)
            .SetComponents({ NKikimrServices::TX_COLUMNSHARD }, "CS")
            .SetPriority(NActors::NLog::PRI_DEBUG)
            .Initialize();
        auto tableClient = kikimr.GetTableClient();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        TTypedLocalHelper("Utf8", kikimr).ExecuteSchemeQuery("DROP TABLE `/Root/olapStore/olapTable`;");
        csController->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        csController->WaitCompactions(TDuration::Seconds(5));
    }

    TString scriptSimplificationWithWrite = R"(
        STOP_SCHEMAS_CLEANUP
        ------
        FAST_PORTIONS_CLEANUP
        ------
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1$$);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                {"levels" : [{"class_name" : "Zero", "expected_blobs_size" : 20, "portions_live_duration" : "1s", "portions_size_limit" : 40000000, "portions_count_available" : 1},
                             {"class_name" : "Zero", "expected_blobs_size" : 4000000}]}`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1) VALUES (1u)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN Col2 Uint32
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2) VALUES (2u, 2u)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN Col3 Uint32
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col3) VALUES (3u, 3u, 3u)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN Col4 Uint32 NOT NULL DEFAULT 0
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col3, Col4) VALUES (4u, 4u, 4u, 4u)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN Col5 Uint32
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col3, Col4, Col5) VALUES (5u, 5u, 5u, 5u, 5u)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` DROP COLUMN Col3
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col4, Col5) VALUES (6u, 6u, 6u, 6u)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN Col7 Uint32
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col4, Col5, Col7) VALUES (7u, 7u, 7u, 7u, 7u)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN Col8 Uint32
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col2, Col4, Col5, Col7, Col8) VALUES (8u, 8u, 8u, 8u, 8u, 8u)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` DROP COLUMN Col2
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (Col1, Col4, Col5, Col7, Col8) VALUES (9u, 9u, 9u, 9u, 9u)
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN Col9 Uint32
        ------
        ONE_SCHEMAS_CLEANUP:
        EXPECTED: true
        ------
        RESTART_TABLETS
        ------
        ONE_SCHEMAS_CLEANUP:
        EXPECTED: false
        ------
        READ: SELECT Col1, Col4, Col5, Col7, Col8 FROM `/Root/ColumnTable` ORDER BY Col1;
        EXPECTED: [[1u;#;#;#;#];[2u;#;#;#;#];[3u;#;#;#;#];[4u;[4u];#;#;#];[5u;[5u];[5u];#;#];[6u;[6u];[6u];#;#];[7u;[7u];[7u];[7u];#];[8u;[8u];[8u];[8u];[8u]];[9u;[9u];[9u];[9u];[9u]]]
        ------
        READ: SELECT SchemaVersion FROM `/Root/ColumnTable/.sys/primary_index_schema_stats` WHERE PresetId = 0 ORDER BY SchemaVersion;
        EXPECTED: [[[6u]];[[9u]];[[11u]]]
        ------
        DATA:
        DELETE FROM `/Root/ColumnTable`
        ------
        ONE_COMPACTION:
        ------
        ONE_SCHEMAS_CLEANUP:
        EXPECTED: true
        ------
        RESTART_TABLETS
        ------
        ONE_SCHEMAS_CLEANUP:
        EXPECTED: false
    )";
    Y_UNIT_TEST_STRING_VARIATOR(SimplificationWithWrite, scriptSimplificationWithWrite) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    TString scriptSimplificationEmptyTable = R"(
        STOP_SCHEMAS_CLEANUP
        ------
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            Col1 Uint64 NOT NULL,
            PRIMARY KEY (Col1)
        )
        PARTITION BY HASH(Col1)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = $$1$$);
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN Col2 Uint32
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN Col3 Uint32
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN Col4 Uint32 NOT NULL DEFAULT 0
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN Col5 Uint32
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` DROP COLUMN Col3
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN Col7 Uint32
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN Col8 Uint32
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` DROP COLUMN Col2
        ------
        SCHEMA:
        ALTER TABLE `/Root/ColumnTable` ADD COLUMN Col9 Uint32
        ------
        ONE_SCHEMAS_CLEANUP:
        EXPECTED: true
        ------
        RESTART_TABLETS
        ------
        ONE_SCHEMAS_CLEANUP:
        EXPECTED: false
    )";
    Y_UNIT_TEST_STRING_VARIATOR(SimplificationEmptyTable, scriptSimplificationEmptyTable) {
        Variator::ToExecutor(Variator::SingleScript(__SCRIPT_CONTENT)).Execute();
    }

    Y_UNIT_TEST(TierDraftsGCWithRestart) {
        auto csController = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NOlap::TWaitCompactionController>();
        csController->SetSmallSizeDetector(1000000);
        csController->SetIndexWriteControllerEnabled(false);
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(100));
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::GC);
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->ResetWriteCounters();

        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto helper = TLocalHelper(kikimr);
        helper.CreateTestOlapTable();
        helper.SetForcedCompaction();
        Tests::NCommon::TLoggerInit(kikimr)
            .SetComponents({ NKikimrServices::TX_COLUMNSHARD }, "CS")
            .SetPriority(NActors::NLog::PRI_DEBUG)
            .Initialize();
        auto tableClient = kikimr.GetTableClient();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);

        while (csController->GetCompactionStartedCounter().Val() == 0) {
            Cout << "Wait compaction..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        while (Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() < 20 ||
               !csController->GetIndexWriteControllerBrokeCount().Val()) {
            Cout << "Wait errors on write... " << Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() << "/"
                 << csController->GetIndexWriteControllerBrokeCount().Val() << Endl;
            Sleep(TDuration::Seconds(2));
        }
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        csController->WaitCompactions(TDuration::Seconds(5));
        AFL_VERIFY(Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());
        {
            const auto startInstant = TMonotonic::Now();
            AFL_VERIFY(Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetDeletesCount() == 0)
            ("count", Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetDeletesCount());
            while (Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() &&
                   TMonotonic::Now() - startInstant < TDuration::Seconds(200)) {
                for (auto&& i : csController->GetShardActualIds()) {
                    kikimr.GetTestServer().GetRuntime()->Send(MakePipePerNodeCacheID(false), NActors::TActorId(),
                        new TEvPipeCache::TEvForward(new TEvents::TEvPoisonPill(), i, false));
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
                    kikimr.GetTestServer().GetRuntime()->Send(MakePipePerNodeCacheID(false), NActors::TActorId(),
                        new TEvPipeCache::TEvForward(new TEvents::TEvPoisonPill(), i, false));
                }
                Cerr << "Waiting empty... " << Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetWritesCount() << "/"
                     << Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetDeletesCount() << Endl;
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
        helper.ExecuteSchemeQuery(
            "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `ENCODING.DICTIONARY.ENABLED`=`true`, "
            "`DEFAULT_VALUE`=`abcde`);");
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

    Y_UNIT_TEST(MultiWriteInTime) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableColumnShardConfig()->SetWritingBufferDurationMs(15000);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        auto csController = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NYDBTest::NColumnShard::TReadOnlyController>();
        TTypedLocalHelper helper("Utf8", kikimr);
        helper.CreateTestOlapTable();
        helper.SetForcedCompaction();

        auto writeSession = helper.StartWriting("/Root/olapStore/olapTable");
        writeSession.FillTable("field", NArrow::NConstruction::TStringPoolFiller(1, 1, "aaa", 1), 0, 800000);
        Sleep(TDuration::Seconds(1));
        writeSession.FillTable("field", NArrow::NConstruction::TStringPoolFiller(1, 1, "bbb", 1), 0.5, 800000);
        Sleep(TDuration::Seconds(1));
        writeSession.FillTable("field", NArrow::NConstruction::TStringPoolFiller(1, 1, "ccc", 1), 0.75, 800000);
        Sleep(TDuration::Seconds(1));
        writeSession.Finalize();
        {
            auto selectQuery = TString(R"(
                SELECT
                    field, count(*) as count,
                FROM `/Root/olapStore/olapTable`
                GROUP BY field
                ORDER BY field
            )");

            auto tableClient = kikimr.GetTableClient();
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("count")), 400000);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("field")), "aaa");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("count")), 200000);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[1].at("field")), "bbb");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("count")), 800000);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[2].at("field")), "ccc");
        }
        {
            auto selectQuery = TString(R"(
                SELECT COUNT(*) as count, MAX(PortionId) as portion_id, TabletId
                FROM `/Root/olapStore/olapTable/.sys/primary_index_portion_stats`
                GROUP BY TabletId
            )");

            auto tableClient = kikimr.GetTableClient();
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("count")), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("count")), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("count")), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("portion_id")), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("portion_id")), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("portion_id")), 1);
        }
        AFL_VERIFY(csController->GetCompactionStartedCounter().Val() == 0);
    }

    Y_UNIT_TEST(MultiWriteInTimeDiffSchemas) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableColumnShardConfig()->SetWritingBufferDurationMs(15000);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        auto csController = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NYDBTest::NColumnShard::TReadOnlyController>();
        TTypedLocalHelper helper("Utf8", "Utf8", kikimr);
        helper.CreateTestOlapTable();
        helper.SetForcedCompaction();
        auto writeGuard = helper.StartWriting("/Root/olapStore/olapTable");
        writeGuard.FillTable("field", NArrow::NConstruction::TStringPoolFiller(1, 1, "aaa", 1), 0, 800000);
        Sleep(TDuration::Seconds(1));
        writeGuard.FillTable("field1", NArrow::NConstruction::TStringPoolFiller(1, 1, "bbb", 1), 0.5, 800000);
        Sleep(TDuration::Seconds(1));
        writeGuard.FillTable("field", NArrow::NConstruction::TStringPoolFiller(1, 1, "ccc", 1), 0.75, 800000);
        Sleep(TDuration::Seconds(1));
        writeGuard.Finalize();
        {
            auto selectQuery = TString(R"(
                SELECT
                    field, count(*) as count,
                FROM `/Root/olapStore/olapTable`
                GROUP BY field
                ORDER BY field
            )");

            auto tableClient = kikimr.GetTableClient();
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("count")), 200000);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("field")), "");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("count")), 400000);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[1].at("field")), "aaa");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("count")), 800000);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[2].at("field")), "ccc");
        }
        {
            auto selectQuery = TString(R"(
                SELECT COUNT(*) as count, MAX(PortionId) as portion_id, TabletId
                FROM `/Root/olapStore/olapTable/.sys/primary_index_portion_stats`
                GROUP BY TabletId
            )");

            auto tableClient = kikimr.GetTableClient();
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("count")), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("count")), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("count")), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("portion_id")), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("portion_id")), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("portion_id")), 1);
        }
        AFL_VERIFY(csController->GetCompactionStartedCounter().Val() == 0);
    }

    Y_UNIT_TEST(WriteDeleteCleanGC) {
        auto csController = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NOlap::TWaitCompactionController>();
        csController->SetSmallSizeDetector(1000000);
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::MilliSeconds(100));
        csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::GC);
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->ResetWriteCounters();

        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);

        TKikimrRunner kikimr(settings);
        auto helper = TLocalHelper(kikimr);
        helper.CreateTestOlapTable();
        helper.SetForcedCompaction();
        Tests::NCommon::TLoggerInit(kikimr)
            .SetComponents({ NKikimrServices::TX_COLUMNSHARD, NKikimrServices::TX_COLUMNSHARD_BLOBS }, "CS")
            .SetPriority(NActors::NLog::PRI_DEBUG)
            .Initialize();
        auto tableClient = kikimr.GetTableClient();

        auto client = kikimr.GetQueryClient();

        {
            auto it = client
                          .ExecuteQuery(R"(
                INSERT INTO `/Root/olapStore/olapTable` (timestamp, uid, resource_id) VALUES (Timestamp('1970-01-01T00:00:00Z'), 'a', '0');
                INSERT INTO `/Root/olapStore/olapTable` (timestamp, uid, resource_id) VALUES (Timestamp('1970-01-01T00:00:01Z'), 'a', 'test');
                INSERT INTO `/Root/olapStore/olapTable` (timestamp, uid, resource_id) VALUES (Timestamp('1970-01-01T00:00:02Z'), 'a', 't');
            )",
                              NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                          .ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }

        while (csController->GetCompactionStartedCounter().Val() == 0) {
            Cerr << "Wait compaction..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        {
            const TInstant start = TInstant::Now();
            while (
                !Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() && TInstant::Now() - start < TDuration::Seconds(10)) {
                Cerr << "Wait size in memory... " << Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() << Endl;
                Sleep(TDuration::Seconds(2));
            }
            AFL_VERIFY(Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());
        }
        {
            auto it = client
                          .ExecuteQuery(R"(
                DELETE FROM `/Root/olapStore/olapTable` ON SELECT CAST(0u AS Timestamp) AS timestamp, Unwrap(CAST('a' AS Utf8)) AS uid;
                DELETE FROM `/Root/olapStore/olapTable`;
            )",
                              NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                          .ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }
        csController->SetOverrideMaxReadStaleness(TDuration::Zero());
        csController->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::GC);
        {
            const TInstant start = TInstant::Now();
            while (
                Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() && TInstant::Now() - start < TDuration::Seconds(10)) {
                Cerr << "Wait empty... " << Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize() << Endl;
                Sleep(TDuration::Seconds(2));
            }
            AFL_VERIFY(!Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());
        }
    }
}

}   // namespace NKikimr::NKqp
