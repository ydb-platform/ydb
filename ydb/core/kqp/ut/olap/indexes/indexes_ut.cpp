#include "indexes_test_enums.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/olap/combinatory/variator.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/with_appended.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom_ngramm/const.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/test_combinator.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>

namespace NKikimr::NKqp {
static void ExecQuery(TKikimrRunner& kikimr, bool useQueryService, const TString& query) {
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

static void ExecQueryExpectErrorContains(TKikimrRunner& kikimr, bool useQueryService, const TString& query, TStringBuf needle) {
    if (useQueryService) {
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetIssues().ToString().contains(needle),
            "Expected error containing '" << needle << "', got: " << result.GetIssues().ToString());
    } else {
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetIssues().ToString().contains(needle),
            "Expected error containing '" << needle << "', got: " << result.GetIssues().ToString());
    }
}

Y_UNIT_TEST_SUITE(KqpOlapIndexes) {
    Y_UNIT_TEST(CreateMinMaxIndex, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapStandaloneTable();

        ExecQuery(kikimr, UseQueryService, R"(ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_minmax_level, TYPE=MINMAX,
            FEATURES=`{"column_name" : "level"}`);
        )");
    }

    Y_UNIT_TEST(MinMaxIndexAppliedToDataAfterCompaction, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
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

        ExecQuery(kikimr, UseQueryService,
            TStringBuilder() << R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_level, TYPE=MINMAX,
                    FEATURES=`{"column_name" : "level"}`);
                )");
        ExecQuery(kikimr, UseQueryService,
            TStringBuilder() << R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id, TYPE=MINMAX,
                    FEATURES=`{"column_name" : "resource_id"}`);
                )");
        ExecQuery(kikimr, UseQueryService,
            TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
        csController->WaitActualization(TDuration::Seconds(10));
        {
            auto it = tableClient
                          .StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
                WHERE level = -1
            )")
                          .GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            CompareYson(result, R"([[0u;]])");
            UNIT_ASSERT_VALUES_EQUAL(csController->GetIndexesSkippedNoData().Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(csController->GetIndexesApprovedOnSelect().Val(), 0);
            UNIT_ASSERT_C(
                csController->GetIndexesApprovedOnSelect().Val() < csController->GetIndexesSkippingOnSelect().Val(),
                TStringBuilder()
                    << "approved: "
                    << csController->GetIndexesApprovedOnSelect().Val()
                    << ", skipped: "
                    << csController->GetIndexesSkippingOnSelect().Val());
        }
    }
    TString scriptChunkDetailsMinMax = R"(
        STOP_COMPACTION
        ------
        SCHEMA:
        CREATE TABLE `/Root/ColumnTable` (
            pk Uint64 NOT NULL,
            field Utf8,
            PRIMARY KEY (pk)
        )
        PARTITION BY HASH(pk)
        WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=field_mm, TYPE=MINMAX, FEATURES=`{"column_name" : "field"}`);
        ------
        SCHEMA:
        ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`)
        ------
        DATA:
        REPLACE INTO `/Root/ColumnTable` (pk, field) VALUES (1u, 'x');
        ------
        ONE_ACTUALIZATION
        ------
        READ: SELECT ChunkDetails FROM `/Root/ColumnTable/.sys/primary_index_stats` WHERE EntityName="field_mm";
        EXPECTED: [[["{\"min\":\"x\",\"max\":\"x\"}"]]]
    )";
    Y_UNIT_TEST(ChunkDetailsMinMax) {
        Variator::ToExecutor(Variator::SingleScript(scriptChunkDetailsMinMax)).Execute(TKikimrSettings().SetColumnShardAlterObjectEnabled(true));
    }


    Y_UNIT_TEST(MinMaxIndexUsedInQueries, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto helper = TLocalHelper(kikimr);
        helper.CreateTestOlapStandaloneTable();
        helper.SetForcedCompaction();
        auto tableClient = kikimr.GetTableClient();
        auto queryServiceCLient = kikimr.GetQueryClient();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());


        auto assertDDLQueryOk = [&](TString query) {
            if (UseQueryService) {
                auto result = queryServiceCLient.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            } else {
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto res = session.ExecuteSchemeQuery(query).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }
        };


        auto runDMLQuery = [&] (TString query) -> TString {
            auto result = queryServiceCLient.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            if (result.GetResultSets().empty()) {
                return "[]";
            }
            return FormatResultSetYson(result.GetResultSet(0));
        };

        assertDDLQueryOk(R"(
            CREATE TABLE `/Root/minmax_test_applied_applied` (
                `key` Int32 NOT NULL,
                `value` String NOT NULL,
                PRIMARY KEY (`key`)
            )
            PARTITION BY HASH (`key`)
            WITH (
                STORE = COLUMN
            );
        )");

        assertDDLQueryOk(R"(
            ALTER OBJECT `/Root/minmax_test_applied_applied` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=value_mm, TYPE=MINMAX, FEATURES=`{"column_name" : "value"}`);

        )");

        // Switch the table to tiling++ with settings that keep every portion
        // in the accumulator and never trigger compaction during the bulk inserts.
        assertDDLQueryOk(NKikimr::Tests::NCS::THelper::GetTilingNoCompactionAlter("/Root/minmax_test_applied_applied"));

        runDMLQuery(R"(
            $data1 = ListMap(ListFromRange(1, 1500001), ($x) -> { RETURN AsStruct($x AS item); });
            UPSERT INTO `/Root/minmax_test_applied_applied` (`key`, `value`)
            SELECT CAST(item AS Int32) AS `key`, "Value_" || CAST(item+1 AS String) AS `value` FROM AS_TABLE($data1);
        )");

        runDMLQuery(R"(
            $data2 = ListMap(ListFromRange(1, 1500001), ($x) -> { RETURN AsStruct($x AS item); });
            UPSERT INTO `/Root/minmax_test_applied_applied` (`key`, `value`)
            SELECT CAST(item AS Int32) AS `key`, "Value_" || CAST(item AS String) AS `value` FROM AS_TABLE($data2);
        )");
        // Re-route every portion to the last level and force compaction there.
        assertDDLQueryOk(NKikimr::Tests::NCS::THelper::GetTilingForceLastLevelCompactionAlter("/Root/minmax_test_applied_applied"));
        csController->WaitCompactions(TDuration::Seconds(5));

        auto skipped_and_approved = csController->GetIndexesSkippingOnSelect().Val() + csController->GetIndexesApprovedOnSelect().Val();


        CompareYson(runDMLQuery(R"(
            SELECT COUNT(*) FROM `/Root/minmax_test_applied_applied` WHERE `value` < "Value_500000";
        )"), "[[944450u]]");
        Cerr << "not built indexes: " << csController->GetIndexesSkippedNoData().Val() << '\n';

        UNIT_ASSERT_GT(csController->GetIndexesSkippingOnSelect().Val() + csController->GetIndexesApprovedOnSelect().Val(), skipped_and_approved);
        skipped_and_approved = csController->GetIndexesSkippingOnSelect().Val() + csController->GetIndexesApprovedOnSelect().Val();

        CompareYson(runDMLQuery(R"(
            SELECT COUNT(*) FROM `/Root/minmax_test_applied_applied` WHERE `value` > "Value_500000";
        )"), "[[555549u]]");

        UNIT_ASSERT_GT(csController->GetIndexesSkippingOnSelect().Val() + csController->GetIndexesApprovedOnSelect().Val(), skipped_and_approved);
        skipped_and_approved = csController->GetIndexesSkippingOnSelect().Val() + csController->GetIndexesApprovedOnSelect().Val();

        CompareYson(runDMLQuery(R"(
            SELECT COUNT(*) FROM `/Root/minmax_test_applied_applied` WHERE `value` <= "Value_500000";
        )"), "[[944451u]]");

        UNIT_ASSERT_GT(csController->GetIndexesSkippingOnSelect().Val() + csController->GetIndexesApprovedOnSelect().Val(), skipped_and_approved);
        skipped_and_approved = csController->GetIndexesSkippingOnSelect().Val() + csController->GetIndexesApprovedOnSelect().Val();

        CompareYson(runDMLQuery(R"(
            SELECT COUNT(*) FROM `/Root/minmax_test_applied_applied` WHERE `value` >= "Value_500000";
        )"), "[[555550u]]");

        UNIT_ASSERT_GT(csController->GetIndexesSkippingOnSelect().Val() + csController->GetIndexesApprovedOnSelect().Val(), skipped_and_approved);
    }

    Y_UNIT_TEST(MinMaxNulls, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto helper = TLocalHelper(kikimr);
        helper.CreateTestOlapStandaloneTable();
        helper.SetForcedCompaction();
        auto tableClient = kikimr.GetTableClient();
        auto queryServiceCLient = kikimr.GetQueryClient();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());


        auto assertDDLQueryOk = [&](TString query) {
            if (UseQueryService) {
                auto result = queryServiceCLient.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            } else {
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto res = session.ExecuteSchemeQuery(query).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }
        };


        auto runDMLQuery = [&] (TString query) -> TString {
            auto result = queryServiceCLient.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            if (result.GetResultSets().empty()) {
                return "[]";
            }
            return FormatResultSetYson(result.GetResultSet(0));
        };

        assertDDLQueryOk(R"(
            CREATE TABLE `/Root/minmax_nulls` (
                `key` Int32 NOT NULL,
                `null_value` String NULL,
                PRIMARY KEY (`key`)
            )
            PARTITION BY HASH (`key`)
            WITH (
                STORE = COLUMN
            );
        )");

        assertDDLQueryOk(R"(
            ALTER OBJECT `/Root/minmax_nulls` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=null_value_mm, TYPE=MINMAX, FEATURES=`{"column_name" : "null_value"}`);
        )");

        runDMLQuery(R"(
            $data1 = ListMap(ListFromRange(1, 1500001), ($x) -> { RETURN AsStruct($x AS item); });
            UPSERT INTO `/Root/minmax_nulls` (`key`, `null_value`)
            SELECT CAST(item AS Int32) AS `key`, CAST(Null AS String?) AS `null_value` FROM AS_TABLE($data1);
        )");

        runDMLQuery(R"(
            $data2 = ListMap(ListFromRange(1, 1500001), ($x) -> { RETURN AsStruct($x AS item); });
            UPSERT INTO `/Root/minmax_nulls` (`key`, `null_value`)
            SELECT CAST(item AS Int32) AS `key`, CAST(Null AS String?) AS `null_value` FROM AS_TABLE($data2);
        )");
        csController->WaitCompactions(TDuration::Seconds(5));

        auto init_approved = csController->GetIndexesApprovedOnSelect().Val();

        CompareYson(runDMLQuery(R"(
            SELECT COUNT(*) FROM `/Root/minmax_nulls` WHERE `null_value` < "Value_500000";
        )"), "[[0u]]");

        UNIT_ASSERT_VALUES_EQUAL(csController->GetIndexesApprovedOnSelect().Val(), init_approved);

        CompareYson(runDMLQuery(R"(
            SELECT COUNT(*) FROM `/Root/minmax_nulls` WHERE `null_value` > "Value_500000";
        )"), "[[0u]]");

        UNIT_ASSERT_VALUES_EQUAL(csController->GetIndexesApprovedOnSelect().Val(), init_approved);

    }

    Y_UNIT_TEST(CreateTableThenAddAndDropLocalBloomIndexesWithSqlSyntax, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecQuery(kikimr, UseQueryService, R"(
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

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTableWithLocalIndexes`
            ADD INDEX idx_bloom LOCAL USING bloom_filter
                ON (resource_id)
                WITH (false_positive_probability = 0.01);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTableWithLocalIndexes`
            ADD INDEX idx_ngram LOCAL USING bloom_ngram_filter
                ON (resource_id)
                WITH (ngram_size = 3, false_positive_probability = 0.01, case_sensitive = true);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTableWithLocalIndexes` DROP INDEX idx_bloom;
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTableWithLocalIndexes` DROP INDEX idx_ngram;
        )");
    }

    Y_UNIT_TEST(AddAndDropLocalBloomIndexesWithSqlSyntax, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapStandaloneTable();

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTable`
            ADD INDEX idx_bloom LOCAL USING bloom_filter
                ON (uid)
                WITH (false_positive_probability = 0.01);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTable`
            ADD INDEX idx_ngram LOCAL USING bloom_ngram_filter
                ON (resource_id)
                WITH (ngram_size = 3, false_positive_probability = 0.01, case_sensitive = true);
        )");

        ExecQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTable` DROP INDEX idx_bloom;");
        ExecQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTable` DROP INDEX idx_ngram;");
    }

    Y_UNIT_TEST(CreateTableWithLocalBloomFilterIndexAndDropIsCorrect, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecQuery(kikimr, UseQueryService, R"(
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

        ExecQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTableCreateBloom` DROP INDEX idx_bloom;");
    }

    Y_UNIT_TEST(CreateTableWithLocalBloomNgramFilterIndexAndDropIsCorrect, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableCreateNgram`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid),
                INDEX idx_ngram LOCAL USING bloom_ngram_filter
                    ON (resource_id)
                    WITH (ngram_size = 3, false_positive_probability = 0.01, case_sensitive = true)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        ExecQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTableCreateNgram` DROP INDEX idx_ngram;");
    }

    Y_UNIT_TEST(BloomNgramAddIndexThenUpsertIndexChangesFilterParams, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);
        auto& client = kikimr.GetTestClient();

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableBloomNgramAddThenUpsert`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTableBloomNgramAddThenUpsert`
            ADD INDEX idx_ngram LOCAL USING bloom_ngram_filter
                ON (resource_id)
                WITH (ngram_size = 3, false_positive_probability = 0.01, case_sensitive = true);
        )");

        auto readBloomNGramm = [&](double& fppOut, ui32& filterBytesOut) -> decltype(auto) {
            auto desc = client.Ls("/Root/olapTableBloomNgramAddThenUpsert");
            UNIT_ASSERT_C(desc->Record.GetPathDescription().HasColumnTableDescription(), "expected column table path");
            const auto& schema = desc->Record.GetPathDescription().GetColumnTableDescription().GetSchema();
            for (auto&& idx : schema.GetIndexes()) {
                if (idx.GetName() == "idx_ngram" && idx.HasBloomNGrammFilter()) {
                    const auto& f = idx.GetBloomNGrammFilter();
                    fppOut = f.GetFalsePositiveProbability();
                    filterBytesOut = f.GetFilterSizeBytes();
                    return;
                }
            }

            UNIT_ASSERT_C(false, "idx_ngram with BloomNGrammFilter not found in table schema");
        };

        double fppBefore = 0;
        ui32 filterBytesBefore = 0;
        readBloomNGramm(fppBefore, filterBytesBefore);

        ExecQueryExpectErrorContains(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapTableBloomNgramAddThenUpsert` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=idx_ngram, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "resource_id", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096, "records_count" : 50000, "case_sensitive" : true, "data_extractor" : {"class_name" : "DEFAULT"}, "bits_storage_type": "SIMPLE_STRING"}`);
        )", "cannot switch bloom ngram index from false_positive_probability mode to deprecated sizing");

        double fppAfter = 0;
        ui32 filterBytesAfter = 0;
        readBloomNGramm(fppAfter, filterBytesAfter);

        UNIT_ASSERT_DOUBLES_EQUAL_C(
            fppBefore, fppAfter, 1e-9,
            TStringBuilder() << "Rejected UPSERT_INDEX must not change false_positive_probability; before fpp="
                             << fppBefore << " after fpp=" << fppAfter);
        UNIT_ASSERT_VALUES_EQUAL_C(
            filterBytesBefore, filterBytesAfter,
            TStringBuilder() << "Rejected UPSERT_INDEX must not change filter_size_bytes; before="
                             << filterBytesBefore << " after=" << filterBytesAfter);
    }

    Y_UNIT_TEST(BloomNgramIndexCreatedViaAlterObjectWithFalsePositiveProbability, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);
        auto& client = kikimr.GetTestClient();

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableBloomNgramAlterObjectFpp`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapTableBloomNgramAlterObjectFpp` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=idx_ngram, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "resource_id", "ngramm_size" : 3, "false_positive_probability" : 0.05, "case_sensitive" : true, "data_extractor" : {"class_name" : "DEFAULT"}, "bits_storage_type": "SIMPLE_STRING"}`);
        )");

        auto desc = client.Ls("/Root/olapTableBloomNgramAlterObjectFpp");
        UNIT_ASSERT_C(desc->Record.GetPathDescription().HasColumnTableDescription(), "expected column table path");
        const auto& schema = desc->Record.GetPathDescription().GetColumnTableDescription().GetSchema();
        bool found = false;
        for (auto&& idx : schema.GetIndexes()) {
            if (idx.GetName() == "idx_ngram" && idx.HasBloomNGrammFilter()) {
                found = true;
                UNIT_ASSERT_DOUBLES_EQUAL_C(idx.GetBloomNGrammFilter().GetFalsePositiveProbability(), 0.05, 1e-9,
                    "false_positive_probability from ALTER OBJECT UPSERT_INDEX FEATURES");
                break;
            }
        }

        UNIT_ASSERT_C(found, "idx_ngram bloom ngram index should appear after UPSERT_INDEX with false_positive_probability");
    }

    Y_UNIT_TEST(BloomNgramAlterObjectUpsertRejectsMixingFppAndFilterSizeBytes, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableBloomNgramMixFppSize`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        const TString alterMixFilterSize = R"(
            ALTER OBJECT `/Root/olapTableBloomNgramMixFppSize` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=idx_ngram, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "resource_id", "ngramm_size" : 3, "false_positive_probability" : 0.05, "filter_size_bytes" : 512, "case_sensitive" : true, "data_extractor" : {"class_name" : "DEFAULT"}, "bits_storage_type": "SIMPLE_STRING"}`);
        )";
        const TString alterMixHashesCount = R"(
            ALTER OBJECT `/Root/olapTableBloomNgramMixFppSize` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=idx_ngram, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "resource_id", "ngramm_size" : 3, "false_positive_probability" : 0.05, "hashes_count" : 2, "case_sensitive" : true, "data_extractor" : {"class_name" : "DEFAULT"}, "bits_storage_type": "SIMPLE_STRING"}`);
        )";

        auto assertMixRejected = [&](const TString& alterQuery) {
            ExecQueryExpectErrorContains(kikimr, UseQueryService, alterQuery, "cannot mix");
        };

        assertMixRejected(alterMixFilterSize);
        assertMixRejected(alterMixHashesCount);
    }

    Y_UNIT_TEST(LocalBloomNgramIndexDefaultCaseSensitivePersisted, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true).SetEnableShowCreate(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableNgramDefault`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid),
                INDEX idx_ngram LOCAL USING bloom_ngram_filter
                    ON (resource_id)
                    WITH (ngram_size = 3, false_positive_probability = 0.01)
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

Y_UNIT_TEST(LocalBloomIndexHasNoCaseSensitiveInShowCreate, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true).SetEnableShowCreate(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableBloomCaseInsensitive`
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

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();
        auto showResult = session.ExecuteQuery(
            "SHOW CREATE TABLE `/Root/olapTableBloomCaseInsensitive`;",
            NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(showResult.IsSuccess(), showResult.GetIssues().ToString());
        UNIT_ASSERT(!showResult.GetResultSets().empty());
        NYdb::TResultSetParser parser(showResult.GetResultSet(0));
        UNIT_ASSERT_C(parser.TryNextRow(), "SHOW CREATE must return at least one row");
        TString createText = parser.ColumnParser(0).GetOptionalUtf8().value_or("");
        UNIT_ASSERT_C(!createText.Contains("case_sensitive"),
            "SHOW CREATE should not contain case_sensitive for bloom filter, got: " << createText);
}

Y_UNIT_TEST(RenameLocalBloomIndex, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true).SetEnableShowCreate(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableRenameBloom`
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

        ExecQuery(kikimr, UseQueryService,
            "ALTER TABLE `/Root/olapTableRenameBloom` RENAME INDEX idx_bloom TO idx_bloom_renamed;");

        {
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            auto showResult = session.ExecuteQuery(
                "SHOW CREATE TABLE `/Root/olapTableRenameBloom`;",
                NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(showResult.IsSuccess(), showResult.GetIssues().ToString());
            UNIT_ASSERT(!showResult.GetResultSets().empty());
            NYdb::TResultSetParser parser(showResult.GetResultSet(0));
            UNIT_ASSERT_C(parser.TryNextRow(), "SHOW CREATE must return at least one row");
            TString createText = parser.ColumnParser(0).GetOptionalUtf8().value_or("");
            UNIT_ASSERT_C(createText.Contains("idx_bloom_renamed"), "SHOW CREATE should contain renamed index idx_bloom_renamed, got: " << createText);
        }

        ExecQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTableRenameBloom` DROP INDEX idx_bloom_renamed;");
    }

    Y_UNIT_TEST(RenameLocalBloomNgramIndex, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true).SetEnableShowCreate(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableRenameNgram`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid),
                INDEX idx_ngram LOCAL USING bloom_ngram_filter
                    ON (resource_id)
                    WITH (ngram_size = 3, false_positive_probability = 0.01)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        ExecQuery(kikimr, UseQueryService,
            "ALTER TABLE `/Root/olapTableRenameNgram` RENAME INDEX idx_ngram TO idx_ngram_renamed;");

        {
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            auto showResult = session.ExecuteQuery(
                "SHOW CREATE TABLE `/Root/olapTableRenameNgram`;",
                NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(showResult.IsSuccess(), showResult.GetIssues().ToString());
            UNIT_ASSERT(!showResult.GetResultSets().empty());
            NYdb::TResultSetParser parser(showResult.GetResultSet(0));
            UNIT_ASSERT_C(parser.TryNextRow(), "SHOW CREATE must return at least one row");
            TString createText = parser.ColumnParser(0).GetOptionalUtf8().value_or("");
            UNIT_ASSERT_C(createText.Contains("idx_ngram_renamed"), "SHOW CREATE should contain renamed index idx_ngram_renamed, got: " << createText);
        }

        ExecQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTableRenameNgram` DROP INDEX idx_ngram_renamed;");
    }

    Y_UNIT_TEST(RenameLocalBloomAndBloomNgramIndexes, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true).SetEnableShowCreate(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableRenameBoth`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid),
                INDEX idx_bloom LOCAL USING bloom_filter ON (resource_id) WITH (false_positive_probability = 0.01),
                INDEX idx_ngram LOCAL USING bloom_ngram_filter ON (resource_id)
                    WITH (ngram_size = 3, false_positive_probability = 0.01)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        ExecQuery(kikimr, UseQueryService,
            "ALTER TABLE `/Root/olapTableRenameBoth` RENAME INDEX idx_bloom TO bloom_renamed;");
        ExecQuery(kikimr, UseQueryService,
            "ALTER TABLE `/Root/olapTableRenameBoth` RENAME INDEX idx_ngram TO ngram_renamed;");

        {
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            auto showResult = session.ExecuteQuery(
                "SHOW CREATE TABLE `/Root/olapTableRenameBoth`;",
                NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(showResult.IsSuccess(), showResult.GetIssues().ToString());
            UNIT_ASSERT(!showResult.GetResultSets().empty());
            NYdb::TResultSetParser parser(showResult.GetResultSet(0));
            UNIT_ASSERT_C(parser.TryNextRow(), "SHOW CREATE must return at least one row");
            TString createText = parser.ColumnParser(0).GetOptionalUtf8().value_or("");
            UNIT_ASSERT_C(createText.Contains("bloom_renamed"), "SHOW CREATE should contain bloom_renamed, got: " << createText);
            UNIT_ASSERT_C(createText.Contains("ngram_renamed"), "SHOW CREATE should contain ngram_renamed, got: " << createText);
        }

        ExecQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTableRenameBoth` DROP INDEX bloom_renamed;");
        ExecQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTableRenameBoth` DROP INDEX ngram_renamed;");
    }

    Y_UNIT_TEST(CheckSchemaChangesDuringTheTime, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableSchemaChanges`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                message Utf8,
                PRIMARY KEY (timestamp, uid),
                INDEX idx_bloom LOCAL USING bloom_filter ON (resource_id) WITH (false_positive_probability = 0.01),
                INDEX idx_ngram LOCAL USING bloom_ngram_filter ON (resource_id)
                    WITH (ngram_size = 3, false_positive_probability = 0.01)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        // Add column without index
        ExecQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTableSchemaChanges` ADD COLUMN extra Utf8;");

        // Add column with index
        ExecQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTableSchemaChanges` ADD COLUMN filtered_col Utf8;");
        ExecQuery(kikimr, UseQueryService, R"(
            ALTER TABLE `/Root/olapTableSchemaChanges` ADD INDEX idx_extra_bloom LOCAL USING bloom_filter
                ON (filtered_col) WITH (false_positive_probability = 0.01);)");

        // Drop column without index
        ExecQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTableSchemaChanges` DROP COLUMN message;");

        // Drop index then drop column
        ExecQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTableSchemaChanges` DROP INDEX idx_extra_bloom;");
        ExecQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTableSchemaChanges` DROP COLUMN filtered_col;");

        // Verify table is still usable and indexed columns remain
        {
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            auto result = session.ExecuteQuery(
                "SELECT COUNT(*) FROM `/Root/olapTableSchemaChanges` WHERE resource_id = 'x';",
                NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CheckForConflictsWithAlreadyExistingIndices, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableIndexConflicts`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid),
                INDEX idx_bloom LOCAL USING bloom_filter ON (resource_id) WITH (false_positive_probability = 0.01),
                INDEX idx_ngram LOCAL USING bloom_ngram_filter ON (resource_id)
                    WITH (ngram_size = 3, false_positive_probability = 0.01)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        {
            const TString addDuplicateQuery =
                "ALTER TABLE `/Root/olapTableIndexConflicts` ADD INDEX idx_bloom LOCAL USING bloom_filter "
                "ON (uid) WITH (false_positive_probability = 0.01);";
            if (UseQueryService) {
                auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
                auto result = session.ExecuteQuery(addDuplicateQuery, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
                const TString issues = result.GetIssues().ToString();
                const bool nameConflict = issues.Contains("idx_bloom") && issues.Contains("duplication");
                const bool incompatibleUpdate = issues.Contains("cannot modify index") || issues.Contains("columns set is different");
                UNIT_ASSERT_C(nameConflict || incompatibleUpdate,
                    "Expected conflict (duplicate name or incompatible update), got: " << issues);
            } else {
                auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(addDuplicateQuery).GetValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
                const TString issues = alterResult.GetIssues().ToString();
                const bool nameConflict = issues.Contains("idx_bloom") && issues.Contains("duplication");
                const bool incompatibleUpdate = issues.Contains("cannot modify index") || issues.Contains("columns set is different");
                UNIT_ASSERT_C(nameConflict || incompatibleUpdate,
                    "Expected conflict (duplicate name or incompatible update), got: " << issues);
            }
        }

        {
            const TString renameToExistingQuery =
                "ALTER TABLE `/Root/olapTableIndexConflicts` RENAME INDEX idx_ngram TO idx_bloom;";
            if (UseQueryService) {
                auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
                auto result = session.ExecuteQuery(renameToExistingQuery, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
                UNIT_ASSERT_C(
                    result.GetIssues().ToString().contains("already exists") || result.GetIssues().ToString().contains("idx_bloom"),
                    "Expected conflict message about existing index name, got: " << result.GetIssues().ToString());
            } else {
                auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(renameToExistingQuery).GetValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
                UNIT_ASSERT_C(
                    alterResult.GetIssues().ToString().contains("already exists") || alterResult.GetIssues().ToString().contains("idx_bloom"),
                    "Expected conflict message about existing index name, got: " << alterResult.GetIssues().ToString());
            }
        }
    }

    // DEPRECATED: old syntax
    Y_UNIT_TEST(LocalBloomNgramIndexDeprecatedSyntaxFilterSizeRecordsCount, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true).SetEnableShowCreate(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        {
            const TString createWithDeprecatedInNewSyntax = R"(
                --!syntax_v1
                CREATE TABLE `/Root/olapTableNgramDeprecatedInNewSyntax`
                (
                    timestamp Timestamp NOT NULL,
                    resource_id Utf8,
                    uid Utf8 NOT NULL,
                    PRIMARY KEY (timestamp, uid),
                    INDEX idx_ngram LOCAL USING bloom_ngram_filter
                        ON (resource_id)
                        WITH (ngram_size = 3, filter_size_bytes = 512, records_count = 1024, case_sensitive = true)
                )
                PARTITION BY HASH(timestamp, uid)
                WITH (STORE = COLUMN, PARTITION_COUNT = 1))";

            ExecQueryExpectErrorContains(kikimr, UseQueryService, createWithDeprecatedInNewSyntax, "filter_size_bytes");
        }

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableNgramDeprecatedSyntax`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapTableNgramDeprecatedSyntax` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=idx_ngram, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "resource_id", "ngramm_size" : 3, "filter_size_bytes" : 512, "records_count" : 1024, "case_sensitive" : true}`)
        )");

        ExecQuery(kikimr, UseQueryService, "ALTER TABLE `/Root/olapTableNgramDeprecatedSyntax` DROP INDEX idx_ngram;");
    }

    Y_UNIT_TEST(LocalIndexCannotBeUsedInTableView, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableViewLocalIndex`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid),
                INDEX idx_ngram LOCAL USING bloom_ngram_filter ON (resource_id)
                    WITH (ngram_size = 3, false_positive_probability = 0.01)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(
            "SELECT * FROM `/Root/olapTableViewLocalIndex` VIEW idx_ngram;",
            NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT(!result.IsSuccess());
    }

    Y_UNIT_TEST(TablesInStore, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        ExecQuery(kikimr, UseQueryService,
            R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_uid, TYPE=BLOOM_NGRAMM_FILTER,
                    FEATURES=`{"column_name" : "resource_id", "ngramm_size" : 3, "false_positive_probability" : 0.01}`);
                )");
        ExecQuery(kikimr, UseQueryService, TStringBuilder() << R"(
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
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");
    }

    Y_UNIT_TEST(IndexesActualization, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
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

        ExecQuery(kikimr, UseQueryService,
            TStringBuilder() << R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "uid", "false_positive_probability" : 0.01}`);
                )");
        ExecQuery(kikimr, UseQueryService,
            TStringBuilder() << R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "resource_id", "false_positive_probability" : 0.05}`);
                )");
        ExecQuery(kikimr, UseQueryService,
            TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
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

    Y_UNIT_TEST(CountMinSketchIndex, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
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

        ExecQuery(kikimr, UseQueryService,
            TStringBuilder() << R"(ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_ts, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ["timestamp"]}`);
                )");
        ExecQuery(kikimr, UseQueryService,
            TStringBuilder() << R"(ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_res_id, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['resource_id']}`);
                )");
        ExecQuery(kikimr, UseQueryService,
            TStringBuilder() << R"(ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_uid, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['uid']}`);
                )");
        ExecQuery(kikimr, UseQueryService,
            TStringBuilder() << R"(ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_level, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['level']}`);
                )");
        ExecQuery(kikimr, UseQueryService,
            TStringBuilder() << R"(ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_message, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['message']}`);
                )");

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

    Y_UNIT_TEST(SchemeActualizationOnceOnStart, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
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
            ExecQuery(kikimr, UseQueryService,
                TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
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
                    FEATURES=`{"column_name" : "resource_id", "ngramm_size" : 3, "false_positive_probability" : 0.01}`);
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
            // The dynamic ngram filter sizing may shift one extra control compaction depending on storage layout
            AFL_VERIFY(3 <= csController->GetCompactionStartedCounter().Val() &&
                csController->GetCompactionStartedCounter().Val() <= 5)("count", csController->GetCompactionStartedCounter().Val());

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
                AFL_VERIFY(csController->GetIndexesSkippingOnSelect().Val() - SkipStart >= 3);
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
                        FEATURES=`{"column_name" : "checkIndexesColumn", "ngramm_size" : 3, "false_positive_probability" : 0.01,
                                    "case_sensitive" : false,
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

    Y_UNIT_TEST(IndexesInBS, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        Y_UNUSED(UseQueryService);
        TTestIndexesScenario().SetStorageId("__DEFAULT").Initialize().ExecuteSkipIndexesScenario();
    }

    Y_UNIT_TEST(IndexesInLocalMetadata, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        Y_UNUSED(UseQueryService);
        TTestIndexesScenario().SetStorageId("__LOCAL_METADATA").Initialize().ExecuteSkipIndexesScenario();
    }

    Y_UNIT_TEST(CheckCompactionFailingOnIndexes, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        Y_UNUSED(UseQueryService);
        TTestIndexesScenario().SetStorageId("__DEFAULT").Initialize().ExecuteAddColumnWithIndexesScenario();
    }

    TString scriptDifferentIndexesConfig = R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_resource_id, TYPE=BLOOM_NGRAMM_FILTER,
        FEATURES=`{"column_name" : "resource_id", "ngramm_size" : $$3|8$$, "false_positive_probability" : $$0.01|0.05|0.1$$,
                   "case_sensitive": $$false|true$$,
                   "data_extractor" : {"class_name" : "DEFAULT"}, "bits_storage_type": "$$SIMPLE_STRING|BITSET$$"}`);
    )";

    Y_UNIT_TEST_STRING_VARIATOR(DifferentIndexesConfigDefaultLike, scriptDifferentIndexesConfig) {
        TTestIndexesScenario().SetStorageId("__DEFAULT").Initialize().ExecuteDifferentConfigurationScenarios(__SCRIPT_CONTENT, "LIKE");
    }
    Y_UNIT_TEST_STRING_VARIATOR(DifferentIndexesConfigLocalLike, scriptDifferentIndexesConfig) {
        TTestIndexesScenario().SetStorageId("__LOCAL_METADATA").Initialize().ExecuteDifferentConfigurationScenarios(__SCRIPT_CONTENT, "LIKE");
    }

    TString scriptDifferentIndexesConfigIlike = R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_resource_id, TYPE=BLOOM_NGRAMM_FILTER,
        FEATURES=`{"column_name" : "resource_id", "ngramm_size" : $$3|8$$, "false_positive_probability" : $$0.01|0.05|0.1$$,
                   "case_sensitive": $$false$$,
                   "data_extractor" : {"class_name" : "DEFAULT"}, "bits_storage_type": "$$SIMPLE_STRING|BITSET$$"}`);
    )";

    Y_UNIT_TEST_STRING_VARIATOR(DifferentIndexesConfigDefaultIlike, scriptDifferentIndexesConfigIlike) {
        TTestIndexesScenario().SetStorageId("__DEFAULT").Initialize().ExecuteDifferentConfigurationScenarios(__SCRIPT_CONTENT, "ILIKE");
    }
    Y_UNIT_TEST_STRING_VARIATOR(DifferentIndexesConfigLocalIlike, scriptDifferentIndexesConfigIlike) {
        TTestIndexesScenario().SetStorageId("__LOCAL_METADATA").Initialize().ExecuteDifferentConfigurationScenarios(__SCRIPT_CONTENT, "ILIKE");
    }

    Y_UNIT_TEST(CheckThatIndexIsUsedInQueries, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());

        auto helper = TLocalHelper(kikimr);
        helper.CreateTestOlapTable();
        helper.SetForcedCompaction();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1200000, 300200000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1300000, 300300000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1400000, 300400000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 2000000, 200000000, 70000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 3000000, 100000000, 110000);
        csController->WaitCompactions(TDuration::Seconds(5));

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id, TYPE=BLOOM_FILTER,
                FEATURES=`{"column_name" : "resource_id", "false_positive_probability" : 0.01}`);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);
        )");

        csController->WaitActualization(TDuration::Seconds(10));

        const ui64 skipBefore = csController->GetIndexesSkippingOnSelect().Val();

        auto it = kikimr.GetTableClient().StreamExecuteScanQuery(R"(
            --!syntax_v1
            SELECT COUNT(*)
            FROM `/Root/olapStore/olapTable`
            WHERE resource_id = 'nonexistent_value'
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(StreamResultToYson(it), R"([[0u;]])");

        const ui64 skipAfter = csController->GetIndexesSkippingOnSelect().Val();
        UNIT_ASSERT_C(skipAfter > skipBefore, "Expected bloom index to skip at least one portion");
    }

    Y_UNIT_TEST(CheckThatIndexIsNotUsedInQueries, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());

        auto helper = TLocalHelper(kikimr);
        helper.CreateTestOlapTable();
        helper.SetForcedCompaction();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1200000, 300200000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1300000, 300300000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1400000, 300400000, 10000);
        csController->WaitCompactions(TDuration::Seconds(5));

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id, TYPE=BLOOM_FILTER,
                FEATURES=`{"column_name" : "resource_id", "false_positive_probability" : 0.01}`);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);
        )");

        csController->WaitActualization(TDuration::Seconds(20));

        const ui64 skipBefore = csController->GetIndexesSkippingOnSelect().Val();
        const ui64 approveBefore = csController->GetIndexesApprovedOnSelect().Val();

        auto it = kikimr.GetTableClient().StreamExecuteScanQuery(R"(
            --!syntax_v1
            SELECT COUNT(*)
            FROM `/Root/olapStore/olapTable`
            WHERE level = -1
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(StreamResultToYson(it), R"([[0u;]])");

        const ui64 skipAfter = csController->GetIndexesSkippingOnSelect().Val();
        const ui64 approveAfter = csController->GetIndexesApprovedOnSelect().Val();

        UNIT_ASSERT_VALUES_EQUAL_C(approveAfter, approveBefore, TStringBuilder() << "Bloom index must not be approved for predicate on non-indexed column. before=" << approveBefore << ", after=" << approveAfter);
        UNIT_ASSERT_VALUES_EQUAL_C(skipAfter, skipBefore, TStringBuilder() << "Bloom index must not be checked for predicate on non-indexed column. before=" << skipBefore << ", after=" << skipAfter);
    }

    Y_UNIT_TEST(TestCompatibilityWithOtherIndices, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());
        auto& client = kikimr.GetTestClient();

        auto helper = TLocalHelper(kikimr);
        helper.CreateTestOlapTable();
        helper.SetForcedCompaction();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1200000, 300200000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1300000, 300300000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1400000, 300400000, 10000);
        csController->WaitCompactions(TDuration::Seconds(5));

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id_bf, TYPE=BLOOM_FILTER,
                FEATURES=`{"column_name" : "resource_id", "false_positive_probability" : 0.01}`);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid_bf, TYPE=BLOOM_FILTER,
                FEATURES=`{"column_name" : "uid", "false_positive_probability" : 0.01}`);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id_ngram, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "resource_id", "ngramm_size" : 3, "false_positive_probability" : 0.01, "case_sensitive" : false}`);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);
        )");

        csController->WaitActualization(TDuration::Seconds(20));

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 2100000, 500000000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 2200000, 500100000, 10000);
        csController->WaitCompactions(TDuration::Seconds(5));

        const ui64 skipBefore = csController->GetIndexesSkippingOnSelect().Val();

        auto it = kikimr.GetTableClient().StreamExecuteScanQuery(R"(
            --!syntax_v1
            SELECT COUNT(*)
            FROM `/Root/olapStore/olapTable`
            WHERE resource_id = 'nonexistent_value' AND uid = 'missing_uid'
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(StreamResultToYson(it), R"([[0u;]])");
        const ui64 skipAfter = csController->GetIndexesSkippingOnSelect().Val();
        UNIT_ASSERT_C(skipAfter > skipBefore, "Expected at least one skipped portion after indexes activation");

        auto tableDesc = client.Ls("/Root/olapStore/olapTable");
        auto indexes = tableDesc->Record.GetPathDescription().GetColumnTableDescription().GetSchema().GetIndexes();
        std::unordered_set<TString> expectedIndexNames{"index_resource_id_bf", "index_uid_bf", "index_resource_id_ngram"};
        UNIT_ASSERT_VALUES_EQUAL_C(indexes.size(), expectedIndexNames.size(), "Unexpected number of indexes on /Root/olapStore/olapTable");
        for (auto&& index : indexes) {
            UNIT_ASSERT_C(expectedIndexNames.erase(index.GetName()), TStringBuilder() << "Unexpected index in schema: " << index.GetName());
        }

        UNIT_ASSERT_C(expectedIndexNames.empty(), "Some expected indexes were not found in schema");
    }

    Y_UNIT_TEST(VerifyBloomFilterIndexSpeedsUpQueriesInAppropriateScenarios, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());

        auto helper = TLocalHelper(kikimr);
        helper.CreateTestOlapTable();
        helper.SetForcedCompaction();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1200000, 300200000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1300000, 300300000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 1400000, 300400000, 10000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 2000000, 200000000, 70000);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 3000000, 100000000, 110000);
        csController->WaitCompactions(TDuration::Seconds(5));

        const auto executeProbeQuery = [&]() -> TDuration {
            const TInstant start = TInstant::Now();
            auto it = kikimr.GetTableClient().StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT COUNT(*)
                FROM `/Root/olapStore/olapTable`
                WHERE resource_id = 'nonexistent_value'
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            CompareYson(StreamResultToYson(it), "[[0u;]]");
            return TInstant::Now() - start;
        };

        const ui64 skipBeforeNoIndex = csController->GetIndexesSkippingOnSelect().Val();
        const ui64 approveBeforeNoIndex = csController->GetIndexesApprovedOnSelect().Val();
        const TDuration noIndexDuration = executeProbeQuery();
        const ui64 skipAfterNoIndex = csController->GetIndexesSkippingOnSelect().Val();
        const ui64 approveAfterNoIndex = csController->GetIndexesApprovedOnSelect().Val();
        UNIT_ASSERT_VALUES_EQUAL_C(skipAfterNoIndex, skipBeforeNoIndex, "No index configured yet, skipping counter must not change");
        UNIT_ASSERT_VALUES_EQUAL_C(approveAfterNoIndex, approveBeforeNoIndex, "No index configured yet, approve counter must not change");

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id_bf_probe, TYPE=BLOOM_FILTER,
                FEATURES=`{"column_name" : "resource_id", "false_positive_probability" : 0.01}`);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);
        )");

        csController->WaitActualization(TDuration::Seconds(10));

        const ui64 skipBeforeWithIndex = csController->GetIndexesSkippingOnSelect().Val();
        const ui64 approveBeforeWithIndex = csController->GetIndexesApprovedOnSelect().Val();
        const TDuration withIndexDuration = executeProbeQuery();
        const ui64 skipAfterWithIndex = csController->GetIndexesSkippingOnSelect().Val();
        const ui64 approveAfterWithIndex = csController->GetIndexesApprovedOnSelect().Val();

        UNIT_ASSERT_C(skipAfterWithIndex > skipBeforeWithIndex, TStringBuilder() << "Expected bloom filter to skip portions in appropriate scenario. before=" << skipBeforeWithIndex << ", after=" << skipAfterWithIndex);
        UNIT_ASSERT_C(skipAfterWithIndex + approveAfterWithIndex > skipBeforeWithIndex + approveBeforeWithIndex, "Expected bloom filter index to be checked after creation");
        UNIT_ASSERT_C(
            withIndexDuration < noIndexDuration,
            TStringBuilder()
                << "Expected probe query to become faster with bloom index. no-index=" << noIndexDuration
                << ", with-index=" << withIndexDuration);
    }

    Y_UNIT_TEST(TestIndicesWorkOnSupportedDataTypes) {
        const bool UseQueryService = true;
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableColumnShardConfig()->SetDisabledOnSchemeShard(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());
        auto& client = kikimr.GetTestClient();

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/indexTypesTable`
            (
                id Int32 NOT NULL,
                uid Utf8,
                resource_id Utf8,
                message Utf8,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1)
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/indexTypesTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=idx_uid_bf, TYPE=BLOOM_FILTER,
                FEATURES=`{"column_name" : "uid", "false_positive_probability" : 0.01}`);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/indexTypesTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=idx_resource_bf, TYPE=BLOOM_FILTER,
                FEATURES=`{"column_name" : "resource_id", "false_positive_probability" : 0.01}`);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/indexTypesTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=idx_message_ngram, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "message", "ngramm_size" : 3, "false_positive_probability" : 0.01, "case_sensitive" : false}`);
        )");

        if (UseQueryService) {
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            auto result = session.ExecuteQuery(R"(
                --!syntax_v1
                UPSERT INTO `/Root/indexTypesTable` (id, uid, resource_id, message) VALUES
                    (1, "uid_1", "res_a", "alpha"),
                    (2, "uid_2", "res_b", "beta"),
                    (3, "uid_3", "res_c", "gamma"),
                    (4, "uid_4", "res_b", "delta");
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        } else {
            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1
                UPSERT INTO `/Root/indexTypesTable` (id, uid, resource_id, message) VALUES
                    (1, "uid_1", "res_a", "alpha"),
                    (2, "uid_2", "res_b", "beta"),
                    (3, "uid_3", "res_c", "gamma"),
                    (4, "uid_4", "res_b", "delta");
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        csController->WaitCompactions(TDuration::Seconds(5));
        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/indexTypesTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);
        )");
        csController->WaitActualization(TDuration::Seconds(10));

        auto checkCount = [&](const TString& query, TStringBuf expectedYson) {
            auto it = kikimr.GetTableClient().StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            CompareYson(TString(expectedYson), StreamResultToYson(it));
        };

        checkCount(R"(
            --!syntax_v1
            SELECT COUNT(*) FROM `/Root/indexTypesTable` WHERE uid = "uid_2"
        )", "[[1u;]]");

        checkCount(R"(
            --!syntax_v1
            SELECT COUNT(*) FROM `/Root/indexTypesTable` WHERE resource_id = "res_b"
        )", "[[2u;]]");

        checkCount(R"(
            --!syntax_v1
            SELECT COUNT(*) FROM `/Root/indexTypesTable` WHERE message LIKE "%ta%"
        )", "[[2u;]]");

        auto tableDesc = client.Ls("/Root/indexTypesTable");
        auto indexes = tableDesc->Record.GetPathDescription().GetColumnTableDescription().GetSchema().GetIndexes();
        std::unordered_set<TString> expectedIndexNames{"idx_uid_bf", "idx_resource_bf", "idx_message_ngram"};
        UNIT_ASSERT_VALUES_EQUAL(indexes.size(), expectedIndexNames.size());
        for (auto&& index : indexes) {
            UNIT_ASSERT(expectedIndexNames.erase(index.GetName()));
        }

        UNIT_ASSERT(expectedIndexNames.empty());
    }

    Y_UNIT_TEST(TestIndicesNotWorkingOnNotSupportedDataTypes) {
        const bool UseQueryService = true;
        auto settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/indexUnsupportedTypesTable`
            (
                id Int32 NOT NULL,
                level Int32,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1)
        )");

        ExecQueryExpectErrorContains(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/indexUnsupportedTypesTable` (TYPE TABLE) SET (
                ACTION=UPSERT_INDEX,
                NAME=idx_level_ngram,
                TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "level", "ngramm_size" : 3, "false_positive_probability" : 0.01}`
            );
        )", "column type");

    }

    Y_UNIT_TEST(IndexesModificationError, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        ExecQuery(kikimr, UseQueryService,
            TStringBuilder() << R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "uid", "false_positive_probability" : 0.05}`);
                )");

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "resource_id", "false_positive_probability" : 0}`);
                )";
            if (UseQueryService) {
                auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
                auto result = session.ExecuteQuery(alterQuery, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            } else {
                auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL(alterResult.GetStatus(), NYdb::EStatus::SUCCESS);
            }
        }

        {
            auto alterQuery = TStringBuilder() <<
                              R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "uid", "false_positive_probability" : 0}`);
                )";
            if (UseQueryService) {
                auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
                auto result = session.ExecuteQuery(alterQuery, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            } else {
                auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL(alterResult.GetStatus(), NYdb::EStatus::SUCCESS);
            }
        }

        ExecQuery(kikimr, UseQueryService,
            TStringBuilder() << R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "uid", "false_positive_probability" : 0.01, "bits_storage_type": "BITSET"}`);
                )");
        ExecQuery(kikimr, UseQueryService,
            TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=DROP_INDEX, NAME=index_uid);");
    }

    Y_UNIT_TEST(BloomNgramFilterSizeBasedOnUniqueNgrams) {
        class TIndexSizeCapturingController : public NOlap::TWaitCompactionController {
        private:
            using TBase = NOlap::TWaitCompactionController;
            mutable TMutex SizeMutex;
            std::vector<ui32> CompactionIndexBlobSizes;

            bool DoOnWriteIndexComplete(
                const NOlap::TColumnEngineChanges& change, const ::NKikimr::NColumnShard::TColumnShard& shard) override
            {
                if (change.TypeString() == NOlap::TCompactColumnEngineChanges::StaticTypeName()) {
                    if (auto* compaction = dynamic_cast<const NOlap::TChangesWithAppend*>(&change)) {
                        TGuard<TMutex> g(SizeMutex);
                        for (auto&& portion : compaction->GetAppendedPortions()) {
                            for (auto&& idx : portion.GetPortionResult().GetIndexesVerified()) {
                                CompactionIndexBlobSizes.push_back(idx.GetRawBytes());
                            }
                        }
                    }
                }
                return TBase::DoOnWriteIndexComplete(change, shard);
            }

        public:
            std::vector<ui32> GetCompactionIndexBlobSizes() const {
                TGuard<TMutex> g(SizeMutex);
                return CompactionIndexBlobSizes;
            }
        };

        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableColumnShardConfig()->SetReaderClassName("SIMPLE");
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<TIndexSizeCapturingController>();
        csController->SetOverrideMemoryLimitForPortionReading(1e+10);
        csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());
        csController->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Disable);
        TLocalHelper(kikimr).CreateTestOlapStandaloneTable();

        {
            auto tableClient = kikimr.GetTableClient();
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(R"(
                ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_resource_id, TYPE=BLOOM_NGRAMM_FILTER,
                    FEATURES=`{"column_name" : "resource_id", "ngramm_size" : 3, "false_positive_probability" : 0.01,
                               "data_extractor" : {"class_name" : "DEFAULT"}, "bits_storage_type": "SIMPLE_STRING"}`);
            )").GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto tableClient = kikimr.GetTableClient();
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(R"(
                ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS,
                    `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`,
                    `COMPACTION_PLANNER.FEATURES`=`
                    {"levels" : [{"class_name" : "Zero", "portions_live_duration" : "10s",
                                  "expected_blobs_size" : 2048000, "portions_count_available" : 1},
                                 {"class_name" : "Zero"}]}`);
            )").GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        constexpr ui32 UniqueResourceIds = 100;
        for (ui32 batch = 0; batch < 10; ++batch) {
            WriteTestData(kikimr, "/Root/olapTable", 1000000, 300000000 + batch * UniqueResourceIds, UniqueResourceIds);
        }

        csController->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Force);
        UNIT_ASSERT(csController->WaitCompactions(TDuration::Seconds(15)));

        constexpr ui64 MaxFilterSizeBytes = NOlap::NIndexes::NBloomNGramm::TConstants::MaxFilterSizeBytes;

        auto sizes = csController->GetCompactionIndexBlobSizes();
        UNIT_ASSERT_C(!sizes.empty(), "Compaction should produce at least one index blob");

        for (auto&& blobSize : sizes) {
            UNIT_ASSERT_C(blobSize > 0, "Index blob size must be positive");
            UNIT_ASSERT_C((blobSize & (blobSize - 1)) == 0, "Index blob size must be a power of 2, got: " << blobSize);
            UNIT_ASSERT_C(blobSize < MaxFilterSizeBytes / 4, "With only " << UniqueResourceIds << " unique resource_ids the filter should be much smaller than max (" << MaxFilterSizeBytes << "), got: " << blobSize);
        }

        auto tableClient = kikimr.GetTableClient();
        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";
                SELECT COUNT(*) FROM `/Root/olapTable`
                WHERE resource_id = '1000050'
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            CompareYson(result, "[[10u;]]");
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";
                SELECT COUNT(*) FROM `/Root/olapTable`
                WHERE resource_id = 'nonexistent_value'
            )").GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            CompareYson(result, "[[0u;]]");
        }

        UNIT_ASSERT_C(csController->GetIndexesSkippingOnSelect().Val() > 0,
            "Bloom ngram filter should skip at least some portions for a nonexistent value");
    }

    Y_UNIT_TEST(BloomNgramAlterObjectNgramAndFilterBytesThenSqlAlterFpp, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);
        auto& client = kikimr.GetTestClient();

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapAlterObjectNgramFilterThenSqlFpp`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        ExecQuery(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapAlterObjectNgramFilterThenSqlFpp` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=idx_ngram, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "resource_id", "ngramm_size" : 5, "filter_size_bytes" : 2048, "records_count" : 10000, "case_sensitive" : true}`);
        )");

        auto readNgram = [&]() {
            auto desc = client.Ls("/Root/olapAlterObjectNgramFilterThenSqlFpp");
            UNIT_ASSERT_C(desc->Record.GetPathDescription().HasColumnTableDescription(), "expected column table path");
            const auto& schema = desc->Record.GetPathDescription().GetColumnTableDescription().GetSchema();
            for (auto&& idx : schema.GetIndexes()) {
                if (idx.GetName() == "idx_ngram" && idx.HasBloomNGrammFilter()) {
                    const auto& f = idx.GetBloomNGrammFilter();
                    return std::make_tuple(
                        f.GetNGrammSize(),
                        f.GetCaseSensitive(),
                        f.GetFilterSizeBytes());
                }
            }

            UNIT_ASSERT_C(false, "idx_ngram with BloomNGrammFilter not found");
            return std::make_tuple(0u, false, 0u);
        };

        {
            const auto [ngram, cs, filterBytes] = readNgram();
            UNIT_ASSERT_VALUES_EQUAL_C(ngram, 5u, "ngramm_size from ALTER OBJECT FEATURES");
            UNIT_ASSERT_C(filterBytes > 0 && filterBytes <= 2048u,
                TStringBuilder() << "filter_size_bytes after UPSERT (normalized from FEATURES), got " << filterBytes);
            UNIT_ASSERT_VALUES_EQUAL_C(cs, true, "case_sensitive after UPSERT");
        }

        ExecQueryExpectErrorContains(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapAlterObjectNgramFilterThenSqlFpp` ALTER INDEX idx_ngram SET (false_positive_probability = 0.07);
        )", "cannot change false_positive_probability on a bloom ngram index created with deprecated sizing");

        {
            const auto [ngram, cs, filterBytes] = readNgram();
            UNIT_ASSERT_VALUES_EQUAL_C(ngram, 5u, "ngram_size unchanged after rejected ALTER");
            UNIT_ASSERT_VALUES_EQUAL_C(cs, true, "case_sensitive unchanged after rejected ALTER");
            UNIT_ASSERT_C(filterBytes > 0 && filterBytes <= 2048u,
                TStringBuilder() << "filter_size_bytes unchanged after rejected ALTER, got " << filterBytes);
        }
    }

    Y_UNIT_TEST(BloomNgramFppIndexRejectsDeprecatedSizingUpsert, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapFppThenOldSizing`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapFppThenOldSizing`
            ADD INDEX idx_ngram LOCAL USING bloom_ngram_filter
                ON (resource_id)
                WITH (ngram_size = 3, false_positive_probability = 0.05, case_sensitive = true);
        )");

        ExecQueryExpectErrorContains(kikimr, UseQueryService, R"(
            ALTER OBJECT `/Root/olapFppThenOldSizing` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=idx_ngram, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "resource_id", "ngramm_size" : 3, "filter_size_bytes" : 1024, "records_count" : 5000}`);
        )", "cannot switch bloom ngram index from false_positive_probability mode to deprecated sizing");
    }

    Y_UNIT_TEST(BloomNgramAlterIndexBasic, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapAlterIdx`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapAlterIdx`
            ADD INDEX idx_ngram LOCAL USING bloom_ngram_filter
                ON (resource_id)
                WITH (ngram_size = 3, false_positive_probability = 0.01, case_sensitive = true);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapAlterIdx` ALTER INDEX idx_ngram SET (false_positive_probability = 0.05);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapAlterIdx` ALTER INDEX idx_ngram SET (ngram_size = 4);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapAlterIdx` ALTER INDEX idx_ngram SET (case_sensitive = false);
        )");
    }

    Y_UNIT_TEST(BloomNgramAlterIndexKeepsSameSchemeIndexId, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        TKikimrRunner kikimr(settings);
        auto& client = kikimr.GetTestClient();

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapAlterIdxSameSchemeId`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapAlterIdxSameSchemeId`
            ADD INDEX idx_ngram LOCAL USING bloom_ngram_filter
                ON (resource_id)
                WITH (ngram_size = 3, false_positive_probability = 0.02, case_sensitive = true);
        )");

        auto readBloomNgramIndexIdAndFpp = [&]() -> std::tuple<ui32, double> {
            auto desc = client.Ls("/Root/olapAlterIdxSameSchemeId");
            UNIT_ASSERT_C(desc->Record.GetPathDescription().HasColumnTableDescription(), "expected column table path");
            const auto& schema = desc->Record.GetPathDescription().GetColumnTableDescription().GetSchema();
            for (auto&& idx : schema.GetIndexes()) {
                if (idx.GetName() == "idx_ngram" && idx.HasBloomNGrammFilter()) {
                    return std::make_tuple(idx.GetId(), idx.GetBloomNGrammFilter().GetFalsePositiveProbability());
                }
            }

            UNIT_ASSERT_C(false, "idx_ngram with BloomNGrammFilter not found");
            return std::make_tuple(0, 0.0);
        };

        const auto [idBefore, fppBefore] = readBloomNgramIndexIdAndFpp();
        UNIT_ASSERT_C(idBefore != 0, "index id must be assigned by schemeshard");

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapAlterIdxSameSchemeId` ALTER INDEX idx_ngram SET (false_positive_probability = 0.06);
        )");

        const auto [idAfter, fppAfter] = readBloomNgramIndexIdAndFpp();
        UNIT_ASSERT_VALUES_EQUAL_C(idBefore, idAfter, "ALTER INDEX must update the same local OLAP index (stable scheme id), not drop+add a new index");
        UNIT_ASSERT_DOUBLES_EQUAL_C(fppAfter, 0.06, 1e-9, "fpp should change after ALTER INDEX");
        UNIT_ASSERT_C(fppBefore != fppAfter, "sanity: fpp actually changed");
    }

    Y_UNIT_TEST(LocalBloomIndexesCompatibleWithDictionaryEncoding, EUseQueryService) {
        const bool UseQueryService = (Arg<0>() == EUseQueryService::QueryService);
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true).SetEnableShowCreate(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableCsDictionaryEncoding(true);
        TKikimrRunner kikimr(settings);
        auto& client = kikimr.GetTestClient();

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            CREATE TABLE `/Root/olapTableBloomWithDict`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8 ENCODING(DICT),
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1))");

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTableBloomWithDict`
            ADD INDEX idx_bloom LOCAL USING bloom_filter
                ON (resource_id)
                WITH (false_positive_probability = 0.01);
        )");

        ExecQuery(kikimr, UseQueryService, R"(
            --!syntax_v1
            ALTER TABLE `/Root/olapTableBloomWithDict`
            ADD INDEX idx_ngram LOCAL USING bloom_ngram_filter
                ON (resource_id)
                WITH (ngram_size = 3, false_positive_probability = 0.01, case_sensitive = true);
        )");

        {
            auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            auto showResult = session.ExecuteQuery(
                "SHOW CREATE TABLE `/Root/olapTableBloomWithDict`;",
                NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(showResult.IsSuccess(), showResult.GetIssues().ToString());
            UNIT_ASSERT(!showResult.GetResultSets().empty());
            NYdb::TResultSetParser parser(showResult.GetResultSet(0));
            UNIT_ASSERT_C(parser.TryNextRow(), "SHOW CREATE must return at least one row");
            TString createText = parser.ColumnParser(0).GetOptionalUtf8().value_or("");
            UNIT_ASSERT_C(createText.Contains("`resource_id` Utf8 ENCODING (DICT)") ||
                createText.Contains("resource_id Utf8 ENCODING (DICT)") ||
                createText.Contains("`resource_id` Utf8 ENCODING(DICT)") ||
                createText.Contains("resource_id Utf8 ENCODING(DICT)"),
                "SHOW CREATE should contain dictionary encoding for resource_id, got: " << createText);
            UNIT_ASSERT_C(
                createText.Contains("NAME = idx_bloom") && createText.Contains("TYPE = BLOOM_FILTER"),
                "SHOW CREATE should contain idx_bloom bloom filter definition, got: " << createText);
            UNIT_ASSERT_C(
                createText.Contains("NAME = idx_ngram") && createText.Contains("TYPE = BLOOM_NGRAMM_FILTER"),
                "SHOW CREATE should contain idx_ngram bloom ngram definition, got: " << createText);
        }

        {
            auto desc = client.Ls("/Root/olapTableBloomWithDict");
            UNIT_ASSERT_C(desc->Record.GetPathDescription().HasColumnTableDescription(), "expected column table path");
            const auto& schema = desc->Record.GetPathDescription().GetColumnTableDescription().GetSchema();

            bool hasBloom = false;
            bool hasNgram = false;
            for (auto&& idx : schema.GetIndexes()) {
                if (idx.GetName() == "idx_bloom" && idx.HasBloomFilter()) {
                    hasBloom = true;
                } else if (idx.GetName() == "idx_ngram" && idx.HasBloomNGrammFilter()) {
                    hasNgram = true;
                }
            }

            UNIT_ASSERT_C(hasBloom, "idx_bloom should be present in table schema");
            UNIT_ASSERT_C(hasNgram, "idx_ngram should be present in table schema");
        }

        auto runDataQuery = [&](const TString& query) {
            auto result = kikimr.GetQueryClient().ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            if (!result.GetResultSets().empty()) {
                return FormatResultSetYson(result.GetResultSet(0));
            }

            return TString("[]");
        };

        runDataQuery(R"(
            --!syntax_v1
            UPSERT INTO `/Root/olapTableBloomWithDict` (timestamp, resource_id, uid) VALUES
                (Timestamp("2024-01-01T00:00:00Z"), "alpha", "u1"),
                (Timestamp("2024-01-01T00:00:01Z"), "beta", "u2"),
                (Timestamp("2024-01-01T00:00:02Z"), "alpha", "u3"),
                (Timestamp("2024-01-01T00:00:03Z"), "gamma", "u4");
        )");

        CompareYson(runDataQuery(R"(
            --!syntax_v1
            SELECT COUNT(*) FROM `/Root/olapTableBloomWithDict` WHERE resource_id = "alpha";
        )"), "[[2u]]");

        CompareYson(runDataQuery(R"(
            --!syntax_v1
            SELECT COUNT(*) FROM `/Root/olapTableBloomWithDict` WHERE resource_id LIKE "alp%";
        )"), "[[2u]]");
    }
}

}   // namespace NKikimr::NKqp
