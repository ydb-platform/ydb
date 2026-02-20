#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/olap/combinatory/variator.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/test_combinator.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

static void ExecSchemeQuery(TKikimrRunner& kikimr, bool useQueryService, const TString& query) {
    if (useQueryService) {
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    } else {
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

Y_UNIT_TEST_SUITE(KqpOlapIndexes) {
    Y_UNIT_TEST_DUO(CreateTableThenAddAndDropLocalBloomIndexesWithSqlSyntax, UseQueryService) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecSchemeQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableWithLocalIndexes`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        ExecSchemeQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTableWithLocalIndexes`
            ADD INDEX idx_bloom LOCAL USING bloom_filter
                ON (resource_id)
                WITH (false_positive_probability = 0.01);
        )");

        ExecSchemeQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTableWithLocalIndexes`
            ADD INDEX idx_ngram LOCAL USING bloom_ngram_filter
                ON (resource_id)
                WITH (ngram_size = 3, hashes_count = 2, filter_size_bytes = 512, records_count = 1024, case_sensitive = true);
        )");

        ExecSchemeQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTableWithLocalIndexes` DROP INDEX idx_bloom;
        )");

        ExecSchemeQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTableWithLocalIndexes` DROP INDEX idx_ngram;
        )");
    }

    Y_UNIT_TEST_DUO(AddAndDropLocalBloomIndexesWithSqlSyntax, UseQueryService) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapStandaloneTable();

        ExecSchemeQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTable`
            ADD INDEX idx_bloom LOCAL USING bloom_filter
                ON (uid)
                WITH (false_positive_probability = 0.01);
        )");

        ExecSchemeQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTable`
            ADD INDEX idx_ngram LOCAL USING bloom_ngram_filter
                ON (resource_id)
                WITH (ngram_size = 3, hashes_count = 2, filter_size_bytes = 512, records_count = 1024, case_sensitive = true);
        )");

        ExecSchemeQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTable` DROP INDEX idx_bloom;");
        ExecSchemeQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTable` DROP INDEX idx_ngram;");
    }

    Y_UNIT_TEST_DUO(CreateTableWithLocalBloomFilterIndexAndDropIsCorrect, UseQueryService) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecSchemeQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableCreateBloom`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid),
                INDEX idx_bloom LOCAL USING bloom_filter
                    ON (resource_id)
                    WITH (false_positive_probability = 0.01)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        ExecSchemeQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTableCreateBloom` DROP INDEX idx_bloom;");
    }

    Y_UNIT_TEST_DUO(CreateTableWithLocalBloomNgramFilterIndexAndDropIsCorrect, UseQueryService) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecSchemeQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableCreateNgram`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid),
                INDEX idx_ngram LOCAL USING bloom_ngram_filter
                    ON (resource_id)
                    WITH (ngram_size = 3, hashes_count = 2, filter_size_bytes = 512, records_count = 1024, case_sensitive = true)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        ExecSchemeQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTableCreateNgram` DROP INDEX idx_ngram;");
    }

    Y_UNIT_TEST_DUO(LocalBloomNgramIndexDefaultCaseSensitivePersisted, UseQueryService) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true).SetEnableShowCreate(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecSchemeQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableNgramDefault`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid),
                INDEX idx_ngram LOCAL USING bloom_ngram_filter
                    ON (resource_id)
                    WITH (ngram_size = 3, hashes_count = 2, filter_size_bytes = 512, records_count = 1024)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto showResult = session.ExecuteQuery(
            "SHOW CREATE TABLE `/Root/olapTableNgramDefault`;",
            NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(showResult.IsSuccess(), showResult.GetIssues().ToString());
        UNIT_ASSERT(!showResult.GetResultSets().empty());
        NYdb::TResultSetParser parser(showResult.GetResultSet(0));
        UNIT_ASSERT_C(parser.TryNextRow(), "SHOW CREATE must return at least one row");
        TString createText = parser.ColumnParser(0).GetOptionalUtf8().value_or("");
        bool hasDefault = createText.Contains("case_sensitive=true") ||
            createText.Contains("\"case_sensitive\":true") ||
            createText.Contains("\\\"case_sensitive\\\":true");
        UNIT_ASSERT_C(hasDefault, "SHOW CREATE should contain case_sensitive default true, got: " << createText);
    }

    Y_UNIT_TEST_DUO(LocalIndexCannotBeUsedInTableView, UseQueryService) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecSchemeQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableViewLocalIndex`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid),
                INDEX idx_ngram LOCAL USING bloom_ngram_filter ON (resource_id)
                    WITH (ngram_size = 3, hashes_count = 2, filter_size_bytes = 512, records_count = 1024)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(
            "SELECT * FROM `/Root/olapTableViewLocalIndex` VIEW idx_ngram;",
            NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT(!result.IsSuccess());
    }

    Y_UNIT_TEST(TablesInStore) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        auto tableClient = kikimr.GetTableClient();
        {
            auto alterQuery =
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_uid, TYPE=BLOOM_NGRAMM_FILTER,
                    FEATURES=`{"column_name" : "resource_id", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 512, "records_count" : 1024}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        {
            auto session = tableClient.CreateSession().GetValueSync().GetSession();

            auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapStore/olapTableTest`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                level Int32,
                message Utf8,
                new_column1 Uint64,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(IndexesActualization) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());

        auto helper = TLocalHelper(kikimr);
        helper.CreateTestOlapTable();
        helper.SetForcedCompaction();
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
                              R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`))";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());


        }
        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "uid", "false_positive_probability" : 0.01}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        {
            auto alterQuery =
                TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "resource_id", "false_positive_probability" : 0.05}`);
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
                WHERE uid = '222'
            )")
                          .GetValueSync();
            //                WHERE ((resource_id = '2' AND level = 222222) OR (resource_id = '1' AND level = 111111) OR (resource_id LIKE '%11dd%')) AND uid = '222'

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            CompareYson(result, R"([[0u;]])");
            AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0);
            AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() == 0);
            AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() < csController->GetIndexesSkippingOnSelect().Val())
            ("approve", csController->GetIndexesApprovedOnSelect().Val())("skip", csController->GetIndexesSkippingOnSelect().Val());
        }
    }

    Y_UNIT_TEST(CountMinSketchIndex) {
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());

        TLocalHelper(kikimr).CreateTestOlapStandaloneTable();
        auto tableClient = kikimr.GetTableClient();
        auto& client = kikimr.GetTestClient();

        Tests::NCommon::TLoggerInit(kikimr)
            .SetComponents({ NKikimrServices::TX_COLUMNSHARD }, "CS")
            .SetPriority(NActors::NLog::PRI_DEBUG)
            .Initialize();

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_ts, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ["timestamp"]}`);
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

        WriteTestData(kikimr, "/Root/olapTable", 1000000, 300000000, 40000);
        WriteTestData(kikimr, "/Root/olapTable", 1100000, 300100000, 40000);
        WriteTestData(kikimr, "/Root/olapTable", 1200000, 300200000, 40000);
        WriteTestData(kikimr, "/Root/olapTable", 1300000, 300300000, 40000);
        WriteTestData(kikimr, "/Root/olapTable", 1400000, 300400000, 40000);
        WriteTestData(kikimr, "/Root/olapTable", 2000000, 200000000, 280000);
        // At least 11 writes with intersecting ranges are necessary to perform at least one tiling compaction.
        for (int i = 0; i < 11; i++) {
            WriteTestData(kikimr, "/Root/olapTable", 3000000, 100000000, 440000);
        }

        csController->WaitActualization(TDuration::Seconds(10));

        {
            auto res = client.Ls("/Root/olapTable");
            auto description = res->Record.GetPathDescription().GetColumnTableDescription();
            auto indexes = description.GetSchema().GetIndexes();
            UNIT_ASSERT(indexes.size() == 5);

            std::unordered_set<TString> indexNames{ "cms_ts", "cms_res_id", "cms_uid", "cms_level", "cms_message" };
            for (const auto& i : indexes) {
                UNIT_ASSERT(i.GetClassName() == "COUNT_MIN_SKETCH");
                UNIT_ASSERT(indexNames.erase(i.GetName()));
            }
            UNIT_ASSERT(indexNames.empty());
        }

        {
            auto runtime = kikimr.GetTestServer().GetRuntime();
            auto sender = runtime->AllocateEdgeActor();

            TAutoPtr<IEventHandle> handle;

            std::optional<NColumnShard::TSchemeShardLocalPathId> schemeShardLocalPathId;
            for (auto&& i : csController->GetShardActualIds()) {
                const auto pathIds = csController->GetPathIdTranslator(i)->GetSchemeShardLocalPathIds();
                UNIT_ASSERT(pathIds.size() == 1);
                if (schemeShardLocalPathId.has_value()) {
                    UNIT_ASSERT(schemeShardLocalPathId == *pathIds.begin());
                } else {
                    schemeShardLocalPathId = *pathIds.begin();
                }
            }

            UNIT_ASSERT(schemeShardLocalPathId.has_value());

            size_t shard = 0;
            for (const auto& [tabletId, pathIdTranslator]: csController->GetActiveTablets()) {
                auto request = std::make_unique<NStat::TEvStatistics::TEvStatisticsRequest>();
                request->Record.MutableTable()->MutablePathId()->SetLocalId(schemeShardLocalPathId->GetRawValue());
                runtime->Send(MakePipePerNodeCacheID(false), sender, new TEvPipeCache::TEvForward(request.release(), static_cast<ui64>(tabletId), false));
                if (++shard == 3) {
                    break;
                }
            }

            auto sketch = std::unique_ptr<TCountMinSketch>(TCountMinSketch::Create());
            for (size_t shard = 0; shard < 3; ++shard) {
                auto event = runtime->GrabEdgeEvent<NStat::TEvStatistics::TEvStatisticsResponse>(handle);
                UNIT_ASSERT(event);

                auto& response = event->Record;
                UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS);
                UNIT_ASSERT(response.ColumnsSize() == 6);
                TString someData = response.GetColumns(0).GetStatistics(0).GetData();
                *sketch += *std::unique_ptr<TCountMinSketch>(TCountMinSketch::FromString(someData.data(), someData.size()));
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
            CompareYson(result, expectedResult);
        }

    public:
        TTestIndexesScenario& Initialize() {
            Settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
            Settings.AppConfig.MutableColumnShardConfig()->SetReaderClassName("SIMPLE");
            Kikimr = std::make_unique<TKikimrRunner>(Settings);
            return *this;
        }

        void ExecuteSkipIndexesScenario() {
            auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
            csController->SetOverrideMemoryLimitForPortionReading(1e+10);
            csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());
            TLocalHelper(*Kikimr).CreateTestOlapTable();
            auto tableClient = Kikimr->GetTableClient();


            Tests::NCommon::TLoggerInit(*Kikimr)
                .SetComponents({ NKikimrServices::KQP_RESOURCE_MANAGER, NKikimrServices::TX_COLUMNSHARD }, "CS")
                .SetPriority(NActors::NLog::PRI_ERROR)
                .Initialize();


            {
                auto alterQuery =
                    TStringBuilder() <<
                    R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                  {"levels" : [{"class_name" : "Zero", "portions_live_duration" : "10s", "expected_blobs_size" : 2048000, "portions_count_available" : 1},
                               {"class_name" : "Zero"}]}`);
                )";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery =
                    TStringBuilder() << Sprintf(
                        R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "uid", "false_positive_probability" : 0.01, "storage_id" : "%s"}`);
                )",
                        StorageId.data());
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery = R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_uid, TYPE=BLOOM_NGRAMM_FILTER,
                    FEATURES=`{"column_name" : "resource_id", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 512, "records_count" : 1024}`);
                )";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery =
                    TStringBuilder() << Sprintf(
                        R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "resource_id", "false_positive_probability" : 0.05, "storage_id" : "%s"}`);
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
                WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
                WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
                WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1200000, 300200000, 10000);
                WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1300000, 300300000, 10000);
                WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1400000, 300400000, 10000);
                WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 2000000, 200000000, 70000);
                WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 3000000, 100000000, 110000);

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

            ExecuteSQL(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";
                SELECT COUNT(*) FROM `/Root/olapStore/olapTable`)", "[[230000u;]]");

            AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0)("val", csController->GetIndexesSkippedNoData().Val());
            AFL_VERIFY(csController->GetIndexesSkippingOnSelect().Val() == 0);
            AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() == 0);
            csController->WaitCompactions(TDuration::Seconds(5));
            // important checker for control compactions (<=21) and control indexes constructed (>=21)
            AFL_VERIFY(csController->GetCompactionStartedCounter().Val() == 3)("count", csController->GetCompactionStartedCounter().Val());

            {
                ExecuteSQL(R"(
                    PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";
                    SELECT COUNT(*)
                    FROM `/Root/olapStore/olapTable`
                    WHERE resource_id LIKE '%110a151' AND resource_id LIKE '110a%' AND resource_id LIKE '%dd%')",
                    "[[0u;]]");
                AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0)("val", csController->GetIndexesSkippedNoData().Val());
                AFL_VERIFY(!csController->GetIndexesApprovedOnSelect().Val());
                AFL_VERIFY(csController->GetIndexesSkippingOnSelect().Val());
            }
            {
                ResetZeroLevel(csController);
                ExecuteSQL(R"(
                    PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";
                    SELECT COUNT(*)
                    FROM `/Root/olapStore/olapTable`
                    WHERE resource_id LIKE '%110a151%')",
                    "[[0u;]]");
                AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0)("val", csController->GetIndexesSkippedNoData().Val());
                AFL_VERIFY(!csController->GetIndexesApprovedOnSelect().Val());
                AFL_VERIFY(csController->GetIndexesSkippingOnSelect().Val() - SkipStart == 3);
            }
            {
                ResetZeroLevel(csController);
                ExecuteSQL(R"(
                    PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";
                    SELECT COUNT(*)
                    FROM `/Root/olapStore/olapTable`
                    WHERE ((resource_id = '2' AND level = 222222) OR (resource_id = '1' AND level = 111111) OR (resource_id LIKE '%11dd%')) AND uid = '222')",
                    "[[0u;]]");

                AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0)("val", csController->GetIndexesSkippedNoData().Val());
                AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() - ApproveStart < csController->GetIndexesSkippingOnSelect().Val() - SkipStart);
            }
            constexpr std::string_view enablePushdownOlapAggregation = R"(PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";)";
            {
                ResetZeroLevel(csController);
                ui32 requestsCount = 100;
                for (ui32 i = 0; i < requestsCount; ++i) {
                    const ui32 idx = RandomNumber<ui32>(uids.size());
                    const auto query = [&](const TString& res, const TString& uid, const ui32 level) {
                        TStringBuilder sb;
                        sb << enablePushdownOlapAggregation << Endl;
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
                AFL_VERIFY((csController->GetIndexesApprovedOnSelect().Val() - ApproveStart) * 0.3 < csController->GetIndexesSkippingOnSelect().Val() - SkipStart)
                ("approved", csController->GetIndexesApprovedOnSelect().Val() - ApproveStart)(
                    "skipped", csController->GetIndexesSkippingOnSelect().Val() - SkipStart);
            }
            {
                ResetZeroLevel(csController);
                ui32 requestsCount = 300;
                for (ui32 i = 0; i < requestsCount; ++i) {
                    const ui32 idx = RandomNumber<ui32>(uids.size());
                    const auto query = [&](const TString& res) {
                        TStringBuilder sb;
                        sb << enablePushdownOlapAggregation << Endl;
                        sb << "SELECT COUNT(*) FROM `/Root/olapStore/olapTable`" << Endl;
                        sb << "WHERE" << Endl;
                        sb << "resource_id LIKE '%" << res << "%'" << Endl;
                        return sb;
                    };
                    ExecuteSQL(query(resourceIds[idx]), "[[1u;]]");
                }
//                AFL_VERIFY(csController->GetIndexesSkippingOnSelect().Val() - SkipStart)(
//                    "approved", csController->GetIndexesApprovedOnSelect().Val() - ApproveStart)(
//                    "skipped", csController->GetIndexesSkippingOnSelect().Val() - SkipStart);
            }
            {
                ResetZeroLevel(csController);
                ui32 requestsCount = 300;
                for (ui32 i = 0; i < requestsCount; ++i) {
                    const ui32 idx = RandomNumber<ui32>(uids.size());
                    const auto query = [&](const TString& res) {
                        TStringBuilder sb;
                        sb << enablePushdownOlapAggregation << Endl;
                        sb << "SELECT COUNT(*) FROM `/Root/olapStore/olapTable`" << Endl;
                        sb << "WHERE" << Endl;
                        sb << "resource_id LIKE '" << res << "%'" << Endl;
                        return sb;
                    };
                    ExecuteSQL(query(resourceIds[idx]), "[[1u;]]");
                }
//                AFL_VERIFY(csController->GetIndexesSkippingOnSelect().Val() - SkipStart)(
//                    "approved", csController->GetIndexesApprovedOnSelect().Val() - ApproveStart)(
//                    "skipped", csController->GetIndexesSkippingOnSelect().Val() - SkipStart);
            }
            {
                ResetZeroLevel(csController);
                ui32 requestsCount = 300;
                for (ui32 i = 0; i < requestsCount; ++i) {
                    const ui32 idx = RandomNumber<ui32>(uids.size());
                    const auto query = [&](const TString& res) {
                        TStringBuilder sb;
                        sb << enablePushdownOlapAggregation << Endl;
                        sb << "SELECT COUNT(*) FROM `/Root/olapStore/olapTable`" << Endl;
                        sb << "WHERE" << Endl;
                        sb << "resource_id LIKE '%" << res << "'" << Endl;
                        return sb;
                    };
                    ExecuteSQL(query(resourceIds[idx]), "[[1u;]]");
                }
//                AFL_VERIFY(csController->GetIndexesSkippingOnSelect().Val() - SkipStart)(
//                    "approved", csController->GetIndexesApprovedOnSelect().Val() - ApproveStart)(
//                    "skipped", csController->GetIndexesSkippingOnSelect().Val() - SkipStart);
            }
        }

        void ExecuteAddColumnWithIndexesScenario() {
            auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
            csController->SetOverrideMemoryLimitForPortionReading(1e+10);
            csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());
            csController->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Disable);
            TLocalHelper(*Kikimr).CreateTestOlapStandaloneTable();
            auto tableClient = Kikimr->GetTableClient();

            WriteTestData(*Kikimr, "/Root/olapTable", 1000000, 300000000, 10000);
            WriteTestData(*Kikimr, "/Root/olapTable", 1100000, 300100000, 10000);
            WriteTestData(*Kikimr, "/Root/olapTable", 1200000, 300200000, 10000);
            WriteTestData(*Kikimr, "/Root/olapTable", 1300000, 300300000, 10000);
            WriteTestData(*Kikimr, "/Root/olapTable", 1400000, 300400000, 10000);
            WriteTestData(*Kikimr, "/Root/olapTable", 2000000, 200000000, 70000);
            WriteTestData(*Kikimr, "/Root/olapTable", 3000000, 100000000, 110000);

            {
                auto alterQuery =
                    TStringBuilder() <<
                    R"(ALTER TABLE `/Root/olapTable` ADD COLUMN checkIndexesColumn Utf8;)";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery =
                    TStringBuilder() <<
                    R"(
                    ALTER OBJECT `/Root/olapTable`
                    (TYPE TABLE)
                    SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_checkIndexesColumn, TYPE=BLOOM_NGRAMM_FILTER,
                        FEATURES=`{"column_name" : "checkIndexesColumn", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 512,
                                    "records_count" : 3000, "case_sensitive" : false,
                                    "data_extractor" : {"class_name" : "DEFAULT"}, "bits_storage_type": "SIMPLE_STRING"}`);
                    )";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }

            {
                auto alterQuery =
                    TStringBuilder() <<
                    R"(ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                  {"levels" : [{"class_name" : "Zero", "portions_live_duration" : "10s", "expected_blobs_size" : 2048000, "portions_count_available" : 1},
                               {"class_name" : "Zero"}]}`);
                )";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }

            csController->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Force);
            UNIT_ASSERT(csController->WaitCompactions(TDuration::Seconds(15)));

            {
                ExecuteSQL(R"(
                PRAGMA OptimizeSimpleILIKE;
                PRAGMA AnsiLike;
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";
                    SELECT COUNT(*)
                    FROM `/Root/olapTable`
                    WHERE checkIndexesColumn = "5")",
                    "[[0u;]]");
                UNIT_ASSERT_VALUES_EQUAL(csController->GetIndexesApprovedOnSelect().Val(), 0);
            }
        }


        void ExecuteDifferentConfigurationScenarios(const TString& indexesConfig, const TString& like) {
            auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
            csController->SetOverrideMemoryLimitForPortionReading(1e+10);
            csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());
            csController->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Disable);
            TLocalHelper(*Kikimr).CreateTestOlapTable();
            auto tableClient = Kikimr->GetTableClient();

            {
                auto alterQuery =
                    TStringBuilder() <<
                    R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                  {"levels" : [{"class_name" : "Zero", "portions_live_duration" : "10s", "expected_blobs_size" : 2048000, "portions_count_available" : 1},
                               {"class_name" : "Zero"}]}`);
                )";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery =
                    TStringBuilder() << Sprintf(
                        R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "uid", "false_positive_probability" : 0.01, "storage_id" : "%s"}`);
                )",
                        StorageId.data());
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery = indexesConfig;
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery =
                    TStringBuilder() << Sprintf(
                        R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "resource_id", "false_positive_probability" : 0.05, "storage_id" : "%s"}`);
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
                WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
                WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
                WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1200000, 300200000, 10000);
                WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1300000, 300300000, 10000);
                WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 1400000, 300400000, 10000);
                WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 2000000, 200000000, 70000);
                WriteTestData(*Kikimr, "/Root/olapStore/olapTable", 3000000, 100000000, 110000);

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

            ExecuteSQL(R"(
                PRAGMA OptimizeSimpleILIKE;
                PRAGMA AnsiLike;
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";
                SELECT COUNT(*) FROM `/Root/olapStore/olapTable`)", "[[230000u;]]");

            AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0)("val", csController->GetIndexesSkippedNoData().Val());
            csController->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Force);
            UNIT_ASSERT(csController->WaitCompactions(TDuration::Seconds(30), 3));

            {
                ExecuteSQL(R"(
                PRAGMA OptimizeSimpleILIKE;
                PRAGMA AnsiLike;
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";
                    SELECT COUNT(*)
                    FROM `/Root/olapStore/olapTable`
                    WHERE resource_id )" + like + R"( '%110a151' AND resource_id )" + like + R"( '110a%' AND resource_id )" + like + R"( '%dd%')",
                    "[[0u;]]");
                AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0)("val", csController->GetIndexesSkippedNoData().Val());
            }
            {
                ResetZeroLevel(csController);
                ExecuteSQL(R"(
                PRAGMA OptimizeSimpleILIKE;
                PRAGMA AnsiLike;
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";
                    SELECT COUNT(*)
                    FROM `/Root/olapStore/olapTable`
                    WHERE resource_id )" + like + R"( '%110a151%')",
                    "[[0u;]]");
                AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0)("val", csController->GetIndexesSkippedNoData().Val());
            }
            {
                ResetZeroLevel(csController);
                ExecuteSQL(R"(
                PRAGMA OptimizeSimpleILIKE;
                PRAGMA AnsiLike;
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";
                    SELECT COUNT(*)
                    FROM `/Root/olapStore/olapTable`
                    WHERE ((resource_id = '2' AND level = 222222) OR (resource_id = '1' AND level = 111111) OR (resource_id )" + like + R"( '%11dd%')) AND uid = '222')",
                    "[[0u;]]");

                AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0)("val", csController->GetIndexesSkippedNoData().Val());
                AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() - ApproveStart < csController->GetIndexesSkippingOnSelect().Val() - SkipStart);
            }
            constexpr std::string_view enablePushdownOlapAggregation = R"(
                PRAGMA OptimizeSimpleILIKE;
                PRAGMA AnsiLike;
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";)";
            {
                ResetZeroLevel(csController);
                ui32 requestsCount = 10;
                for (ui32 i = 0; i < requestsCount; ++i) {
                    const ui32 idx = RandomNumber<ui32>(uids.size());
                    const auto query = [&](const TString& res, const TString& uid, const ui32 level) {
                        TStringBuilder sb;
                        sb << enablePushdownOlapAggregation << Endl;
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
            }
            {
                ResetZeroLevel(csController);
                ui32 requestsCount = 10;
                for (ui32 i = 0; i < requestsCount; ++i) {
                    const ui32 idx = RandomNumber<ui32>(uids.size());
                    const auto query = [&](const TString& res) {
                        TStringBuilder sb;
                        sb << enablePushdownOlapAggregation << Endl;
                        sb << "SELECT COUNT(*) FROM `/Root/olapStore/olapTable`" << Endl;
                        sb << "WHERE" << Endl;
                        sb << "resource_id " << like << " '%" << res << "%'" << Endl;
                        return sb;
                    };
                    ExecuteSQL(query(resourceIds[idx]), "[[1u;]]");
                }
            }
            {
                ResetZeroLevel(csController);
                ui32 requestsCount = 10;
                for (ui32 i = 0; i < requestsCount; ++i) {
                    const ui32 idx = RandomNumber<ui32>(uids.size());
                    const auto query = [&](const TString& res) {
                        TStringBuilder sb;
                        sb << enablePushdownOlapAggregation << Endl;
                        sb << "SELECT COUNT(*) FROM `/Root/olapStore/olapTable`" << Endl;
                        sb << "WHERE" << Endl;
                        sb << "resource_id " << like << " '" << res << "%'" << Endl;
                        return sb;
                    };
                    ExecuteSQL(query(resourceIds[idx]), "[[1u;]]");
                }
            }
            {
                ResetZeroLevel(csController);
                ui32 requestsCount = 10;
                for (ui32 i = 0; i < requestsCount; ++i) {
                    const ui32 idx = RandomNumber<ui32>(uids.size());
                    const auto query = [&](const TString& res) {
                        TStringBuilder sb;
                        sb << enablePushdownOlapAggregation << Endl;
                        sb << "SELECT COUNT(*) FROM `/Root/olapStore/olapTable`" << Endl;
                        sb << "WHERE" << Endl;
                        sb << "resource_id " << like << " '%" << res << "'" << Endl;
                        return sb;
                    };
                    ExecuteSQL(query(resourceIds[idx]), "[[1u;]]");
                }
            }
        }
    };

    Y_UNIT_TEST(IndexesInBS) {
        TTestIndexesScenario().SetStorageId("__DEFAULT").Initialize().ExecuteSkipIndexesScenario();
    }

    Y_UNIT_TEST(IndexesInLocalMetadata) {
        TTestIndexesScenario().SetStorageId("__LOCAL_METADATA").Initialize().ExecuteSkipIndexesScenario();
    }

    Y_UNIT_TEST(CheckCompactionFailingOnIndexes) {
        TTestIndexesScenario().SetStorageId("__DEFAULT").Initialize().ExecuteAddColumnWithIndexesScenario();
    }

    TString scriptDifferentIndexesConfig = R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_resource_id, TYPE=BLOOM_NGRAMM_FILTER,
        FEATURES=`{"column_name" : "resource_id", "ngramm_size" : $$3|8$$, "hashes_count" : $$5|8$$,
                   "filter_size_bytes" : $$128|129|131|255|257$$,
                   "records_count" : $$331|1879$$, "case_sensitive": $$false|true$$,
                   "data_extractor" : {"class_name" : "DEFAULT"}, "bits_storage_type": "$$SIMPLE_STRING|BITSET$$"}`);
    )";

    Y_UNIT_TEST_STRING_VARIATOR(DifferentIndexesConfigDefaultLike, scriptDifferentIndexesConfig) {
        TTestIndexesScenario().SetStorageId("__DEFAULT").Initialize().ExecuteDifferentConfigurationScenarios(__SCRIPT_CONTENT, "LIKE");
    }
    Y_UNIT_TEST_STRING_VARIATOR(DifferentIndexesConfigLocalLike, scriptDifferentIndexesConfig) {
        TTestIndexesScenario().SetStorageId("__LOCAL_METADATA").Initialize().ExecuteDifferentConfigurationScenarios(__SCRIPT_CONTENT, "LIKE");
    }

    TString scriptDifferentIndexesConfigIlike = R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_resource_id, TYPE=BLOOM_NGRAMM_FILTER,
        FEATURES=`{"column_name" : "resource_id", "ngramm_size" : $$3|8$$, "hashes_count" : $$1|5|8$$,
                   "filter_size_bytes" : $$128|129|131|255|257$$,
                   "records_count" : $$331|1879$$, "case_sensitive": $$false$$,
                   "data_extractor" : {"class_name" : "DEFAULT"}, "bits_storage_type": "$$SIMPLE_STRING|BITSET$$"}`);
    )";

    Y_UNIT_TEST_STRING_VARIATOR(DifferentIndexesConfigDefaultIlike, scriptDifferentIndexesConfigIlike) {
        TTestIndexesScenario().SetStorageId("__DEFAULT").Initialize().ExecuteDifferentConfigurationScenarios(__SCRIPT_CONTENT, "ILIKE");
    }
    Y_UNIT_TEST_STRING_VARIATOR(DifferentIndexesConfigLocalIlike, scriptDifferentIndexesConfigIlike) {
        TTestIndexesScenario().SetStorageId("__LOCAL_METADATA").Initialize().ExecuteDifferentConfigurationScenarios(__SCRIPT_CONTENT, "ILIKE");
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
                    FEATURES=`{"column_name" : "uid", "false_positive_probability" : 0.05}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "resource_id", "false_positive_probability" : 0.05}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(alterResult.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "uid", "false_positive_probability" : 0.005}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(alterResult.GetStatus(), NYdb::EStatus::SUCCESS);
        }

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "uid", "false_positive_probability" : 0.01, "bits_storage_type": "BITSET"}`);
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
