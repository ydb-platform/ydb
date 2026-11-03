#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

namespace {

// Schema versions of the active portions of `/Root/ColumnTable`.
std::vector<ui64> GetPortionSchemaVersions(TTestHelper& helper) {
    auto result = helper.GetKikimr().GetQueryClient()
                      .ExecuteQuery(R"(
                          SELECT Details FROM `/Root/ColumnTable/.sys/primary_index_portion_stats` WHERE Activity = 1 ORDER BY PortionId
                      )", NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                      .GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    std::vector<ui64> versions;
    NYdb::TResultSetParser parser(result.GetResultSet(0));
    while (parser.TryNextRow()) {
        NJson::TJsonValue details;
        UNIT_ASSERT(NJson::ReadJsonTree(TString(parser.ColumnParser("Details").GetUtf8()), &details));
        versions.push_back(details["schema_version"].GetUIntegerRobust());
    }
    return versions;
}

void Compact(TTestHelper& helper, const bool cascade) {
    auto result = helper.GetKikimr().GetQueryClient()
                      .ExecuteQuery(TStringBuilder() << "ALTER TABLE `/Root/ColumnTable` COMPACT WITH (CASCADE = " << (cascade ? "true" : "false") << ")",
                          NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(120)))
                      .GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

}   // namespace

Y_UNIT_TEST_SUITE(KqpOlapCompact) {
    // ALTER TABLE ... COMPACT WITH (CASCADE = true) on a column table also rewrites every portion to the latest schema
    // (as if SCHEME_NEED_ACTUALIZATION had been set) and completes only once that is done.
    Y_UNIT_TEST(CascadeActualizesSchema) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetOverrideTasksActualizationLag(TDuration::Zero());

        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.FeatureFlags.SetEnableForcedColumnCompactions(true);
        TTestHelper testHelper(settings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("Value").SetType(NScheme::NTypeIds::Utf8),
        };
        TTestHelper::TColumnTable table;
        table.SetName("/Root/ColumnTable").SetPrimaryKey({ "Key" }).SetSchema(schema);
        testHelper.CreateTable(table);
        testHelper.ExecuteQuery(R"(
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (
                ACTION=UPSERT_OPTIONS,
                `COMPACTION_PLANNER.CLASS_NAME`=`tiling++`,
                `COMPACTION_PLANNER.FEATURES`=`{"accumulator_portion_size_limit": 1, "last_level_compaction_portions": 2, "aging_enabled": false}`
            );
        )");

        for (ui32 i = 0; i < 4; ++i) {
            TTestHelper::TUpdatesBuilder updates(table.GetArrowSchema(schema));
            for (ui64 key = 0; key < 1000; ++key) {
                updates.AddRow().Add(key).Add("value");
            }
            testHelper.BulkUpsert(table, updates);
        }
        Compact(testHelper, /*cascade=*/false);
        const auto versionsBefore = GetPortionSchemaVersions(testHelper);
        UNIT_ASSERT(!versionsBefore.empty());
        const ui64 oldVersion = *std::max_element(versionsBefore.begin(), versionsBefore.end());

        // A new schema version: without CASCADE the existing portions are left as they are.
        testHelper.ExecuteQuery("ALTER TABLE `/Root/ColumnTable` ADD COLUMN Extra Uint64;");
        Compact(testHelper, /*cascade=*/false);
        UNIT_ASSERT_EQUAL(GetPortionSchemaVersions(testHelper), versionsBefore);

        Compact(testHelper, /*cascade=*/true);
        const auto versionsAfter = GetPortionSchemaVersions(testHelper);
        UNIT_ASSERT(!versionsAfter.empty());
        for (const ui64 version : versionsAfter) {
            UNIT_ASSERT_GT(version, oldVersion);
        }
        testHelper.ReadData("SELECT COUNT(*) FROM `/Root/ColumnTable`", "[[1000u]]");
    }
}

}   // namespace NKikimr::NKqp
