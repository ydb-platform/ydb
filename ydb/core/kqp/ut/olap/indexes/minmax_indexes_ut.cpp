#include "indexes_test_enums.h"
#include "indexes_ut_helper.h"

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

Y_UNIT_TEST_SUITE(KqpOlapMinMaxIndexes) {
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
}
}