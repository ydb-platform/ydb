#include "helpers/local.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapOptions) {
    NKikimrSchemeOp::TColumnTableSchemeOptions DescribeStoreOptions(TKikimrRunner& kikimr, const TString& storePath) {
        auto* runtime = kikimr.GetTestServer().GetRuntime();
        const auto describe = DescribeTable(&kikimr.GetTestServer(), runtime->AllocateEdgeActor(), storePath);
        const auto& presets = describe.GetPathDescription().GetColumnStoreDescription().GetSchemaPresets();
        UNIT_ASSERT_VALUES_EQUAL(presets.size(), 1);
        return presets[0].GetSchema().GetOptions();
    }

    void ExecuteScheme(TKikimrRunner& kikimr, const TString& query, const NYdb::EStatus expected = NYdb::EStatus::SUCCESS) {
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expected, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ResetOptions) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        const TString store = "/Root/olapStore";

        ExecuteScheme(kikimr, R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS,
            SCHEME_NEED_ACTUALIZATION=`true`, SCAN_READER_POLICY_NAME=`SIMPLE`, DEDUPLICATION_ENABLED=`false`, CACHE_BLOBS_AFTER_WRITE=`true`,
            `INSERT_OPTIONS.BUILD_INDEXES_ENABLED`=`true`, `INSERT_OPTIONS.BUILD_INDEXES_MIN_BLOB_BYTES`=`1024`,
            `COMPACTION_PLANNER.CLASS_NAME`=`tiling`, `METADATA_MEMORY_MANAGER.CLASS_NAME`=`local_db`);)");
        {
            const auto options = DescribeStoreOptions(kikimr, store);
            UNIT_ASSERT(options.GetSchemeNeedActualization());
            UNIT_ASSERT_VALUES_EQUAL(options.GetScanReaderPolicyName(), "SIMPLE");
            UNIT_ASSERT(options.HasDeduplicationEnabled());
            UNIT_ASSERT(!options.GetDeduplicationEnabled());
            UNIT_ASSERT(options.GetCacheBlobsAfterWrite());
            UNIT_ASSERT(options.GetInsertOptions().GetBuildIndexesEnabled());
            UNIT_ASSERT_VALUES_EQUAL(options.GetInsertOptions().GetBuildIndexesMinBlobBytes(), 1024u);
            UNIT_ASSERT(options.HasCompactionPlannerConstructor());
            UNIT_ASSERT(options.HasMetadataManagerConstructor());
        }

        // options not mentioned in UPSERT_OPTIONS keep their values
        ExecuteScheme(kikimr, R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCAN_READER_POLICY_NAME=`PLAIN`);)");
        {
            const auto options = DescribeStoreOptions(kikimr, store);
            UNIT_ASSERT(options.GetSchemeNeedActualization());
            UNIT_ASSERT_VALUES_EQUAL(options.GetScanReaderPolicyName(), "PLAIN");
        }

        ExecuteScheme(kikimr, R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=RESET_OPTIONS, NAME=`SCHEME_NEED_ACTUALIZATION, DEDUPLICATION_ENABLED`);)");
        {
            const auto options = DescribeStoreOptions(kikimr, store);
            UNIT_ASSERT(!options.GetSchemeNeedActualization());
            UNIT_ASSERT(!options.HasDeduplicationEnabled());
            UNIT_ASSERT_VALUES_EQUAL(options.GetScanReaderPolicyName(), "PLAIN");
            UNIT_ASSERT(options.HasCompactionPlannerConstructor());
        }

        ExecuteScheme(kikimr, R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=RESET_OPTIONS,
            NAME=`SCAN_READER_POLICY_NAME, INSERT_OPTIONS, COMPACTION_PLANNER, METADATA_MEMORY_MANAGER, CACHE_BLOBS_AFTER_WRITE`);)");
        {
            const auto options = DescribeStoreOptions(kikimr, store);
            UNIT_ASSERT(!options.HasScanReaderPolicyName());
            UNIT_ASSERT(!options.HasInsertOptions());
            UNIT_ASSERT(!options.HasCompactionPlannerConstructor());
            UNIT_ASSERT(!options.HasMetadataManagerConstructor());
            UNIT_ASSERT(!options.HasCacheBlobsAfterWrite());
        }

        ExecuteScheme(kikimr, R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=RESET_OPTIONS, NAME=`UNKNOWN`);)",
            NYdb::EStatus::GENERIC_ERROR);
        ExecuteScheme(kikimr, R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=RESET_OPTIONS);)",
            NYdb::EStatus::GENERIC_ERROR);
    }
}

}
