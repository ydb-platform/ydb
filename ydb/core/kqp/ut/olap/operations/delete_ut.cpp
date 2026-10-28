#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

namespace {

// The default test controller forces an intermediate merge after every two inputs, i.e. the sequential compaction path.
// With singlePassMerge the production memory limit is used instead, so the small compactions of these tests finish in one merge.
class TCompactionController: public NYDBTest::NColumnShard::TController {
private:
    const bool SinglePassMerge;

public:
    explicit TCompactionController(const bool singlePassMerge)
        : SinglePassMerge(singlePassMerge) {
    }

    bool CheckPortionsToMergeOnCompaction(const ui64 memoryAfterAdd, const ui32 currentSubsetsCount) override {
        if (SinglePassMerge) {
            return NYDBTest::ICSController::CheckPortionsToMergeOnCompaction(memoryAfterAdd, currentSubsetsCount);
        }
        return NYDBTest::NColumnShard::TController::CheckPortionsToMergeOnCompaction(memoryAfterAdd, currentSubsetsCount);
    }
};

// A single-shard column table `/Root/ColumnTable` (Key Uint64, Value Utf8) with the tiling++ planner, compaction disabled until
// CompactUntilDone() is called.
class TCompactAfterDeleteTest {
private:
    NYDBTest::TControllers::TGuard<TCompactionController> CsController;
    TTestHelper TestHelper;
    TVector<TTestHelper::TColumnSchema> Schema;
    TTestHelper::TColumnTable Table;

    static NYDBTest::TControllers::TGuard<TCompactionController> MakeController(const bool singlePassMerge) {
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<TCompactionController>(singlePassMerge);
        controller->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        controller->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        return controller;
    }

    static TKikimrSettings BuildSettings() {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        settings.FeatureFlags.SetEnableForcedColumnCompactions(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        return settings;
    }

public:
    explicit TCompactAfterDeleteTest(const bool singlePassMerge)
        : CsController(MakeController(singlePassMerge))
        , TestHelper(BuildSettings())
        , Schema({
            TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("Value").SetType(NScheme::NTypeIds::Utf8),
        }) {
        Table.SetName("/Root/ColumnTable").SetPrimaryKey({ "Key" }).SetSchema(Schema);
        TestHelper.CreateTable(Table);
        // Keep compaction tasks small so the delete markers are merged with the data in several steps.
        TestHelper.ExecuteQuery(R"(
            ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (
                ACTION=UPSERT_OPTIONS,
                `COMPACTION_PLANNER.CLASS_NAME`=`tiling++`,
                `COMPACTION_PLANNER.FEATURES`=`{
                    "accumulator_portion_size_limit": 1,
                    "last_level_compaction_portions": 2,
                    "aging_enabled": false
                }`
            );
        )");
    }

    TTestHelper& GetTestHelper() {
        return TestHelper;
    }

    // Writes one portion with the given keys.
    void BulkUpsert(const std::vector<ui64>& keys) {
        TTestHelper::TUpdatesBuilder updates(Table.GetArrowSchema(Schema));
        for (const ui64 key : keys) {
            updates.AddRow().Add(key).Add("value");
        }
        TestHelper.BulkUpsert(Table, updates);
    }

    void ExecuteWrite(const TString& query) {
        auto result = TestHelper.GetKikimr().GetQueryClient()
                          .ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                          .GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // Splits the compaction output so delete-only portions can become isolated from the remaining data, then runs a forced
    // compaction and checks that it finished with no intersecting portions left.
    void CompactUntilDone() {
        CsController->SetOverrideBlobSplitSettings(
            NOlap::NSplitter::TSplitSettings::BuildForTests().SetMaxPortionSize(4096).SetMinRecordsCount(64));
        CsController->EnableBackground(NYDBTest::ICSController::EBackground::Compaction);
        auto compactResult = TestHelper.GetKikimr().GetQueryClient()
                                 .ExecuteQuery("ALTER TABLE `/Root/ColumnTable` COMPACT WITH (CASCADE = false)",
                                     NYdb::NQuery::TTxControl::NoTx(),
                                     NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(60)))
                                 .GetValueSync();
        UNIT_ASSERT_C(compactResult.IsSuccess(), compactResult.GetIssues().ToString());

        NYdb::NOperation::TOperationClient operationClient(TestHelper.GetKikimr().GetDriver());
        auto operations = operationClient.List<NYdb::NTable::TCompactionOperation>().GetValueSync();
        UNIT_ASSERT_C(operations.IsSuccess(), operations.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(operations.GetList().size(), 1);
        const auto& operation = operations.GetList().front();
        UNIT_ASSERT(operation.Ready());
        UNIT_ASSERT_C(operation.Status().IsSuccess(), operation.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(operation.Metadata().State, NYdb::NTable::ECompactState::Done);
    }

    // An empty SELECT alone misses retained delete markers: they are counted in Rows of the active portions.
    // Obsolete portions awaiting GC are ignored.
    void CheckActivePortions(const TString& expected) {
        TestHelper.ReadData(R"(
            SELECT PortionId, Rows, CompactionLevel
            FROM `/Root/ColumnTable/.sys/primary_index_portion_stats`
            WHERE Activity = 1
            ORDER BY PortionId
        )", expected);
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(KqpOlapDelete) {
    Y_UNIT_TEST_TWIN(CompactAfterDeleteRemovesAllRows, SinglePassMerge) {
        TCompactAfterDeleteTest test(SinglePassMerge);
        auto& testHelper = test.GetTestHelper();

        constexpr ui64 batches = 8;
        constexpr ui64 rowsPerBatch = 1024;
        // Interleave keys so the loaded portions have overlapping primary-key ranges.
        for (ui64 batch = 0; batch < batches; ++batch) {
            std::vector<ui64> keys;
            for (ui64 row = 0; row < rowsPerBatch; ++row) {
                keys.push_back(row * batches + batch);
            }
            test.BulkUpsert(keys);
        }
        testHelper.ReadData("SELECT COUNT(*) FROM `/Root/ColumnTable`", "[[8192u]]");
        testHelper.ReadData(R"(
            SELECT COUNT(*)
            FROM `/Root/ColumnTable/.sys/primary_index_portion_stats`
            WHERE Activity = 1
        )", "[[8u]]");

        test.ExecuteWrite("DELETE FROM `/Root/ColumnTable`");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTable`", "[]");

        test.CompactUntilDone();

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTable`", "[]");
        test.CheckActivePortions("[]");
    }

    Y_UNIT_TEST_TWIN(CompactAfterDeleteOnSparseKeys, SinglePassMerge) {
        // Three data portions with gaps between them and delete markers for the existing and the absent keys 0..849.
        // tiling++ merges the markers with the two leftmost data portions first while the third one stays outside of that task:
        // only the markers of the keys 800..849 are still required after it, all the others have to be dropped at once.
        // Keeping them all would leave a marker-only portion below key 800 that intersects nothing and is never compacted again.
        TCompactAfterDeleteTest test(SinglePassMerge);
        auto& testHelper = test.GetTestHelper();

        for (const ui64 first : { 0, 200, 800 }) {
            std::vector<ui64> keys;
            for (ui64 key = first; key < first + 100; ++key) {
                keys.push_back(key);
            }
            test.BulkUpsert(keys);
        }
        testHelper.ReadData("SELECT COUNT(*) FROM `/Root/ColumnTable`", "[[300u]]");

        test.ExecuteWrite(R"(
            DELETE FROM `/Root/ColumnTable` ON
            SELECT Key FROM AS_TABLE(ListMap(ListFromRange(0ul, 850ul), ($key) -> (AsStruct($key AS Key))))
        )");
        testHelper.ReadData("SELECT COUNT(*) FROM `/Root/ColumnTable`", "[[50u]]");

        test.CompactUntilDone();

        // Nothing is lost, nothing is resurrected and no delete markers are left.
        testHelper.ReadData("SELECT COUNT(*) FROM `/Root/ColumnTable`", "[[50u]]");
        testHelper.ReadData("SELECT COUNT(*) FROM `/Root/ColumnTable` WHERE Key >= 850", "[[50u]]");
        testHelper.ReadData(R"(
            SELECT SUM(Rows)
            FROM `/Root/ColumnTable/.sys/primary_index_portion_stats`
            WHERE Activity = 1
        )", "[[[50u]]]");
    }

    Y_UNIT_TEST_TWIN(DeleteWithDiffrentTypesPKColumns, isStream) {
        auto runnerSettings = TKikimrSettings().SetWithSampleTables(true);
        runnerSettings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);

        TTestHelper testHelper(runnerSettings);
        auto client = testHelper.GetKikimr().GetQueryClient();

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("time").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("class").SetType(NScheme::NTypeIds::Utf8).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("uniq").SetType(NScheme::NTypeIds::Utf8).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "time", "class", "uniq" }).SetSchema(schema);
        testHelper.CreateTable(testTable);

        auto ts = TInstant::Now();
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(ts.MicroSeconds()).Add("test").Add("test");
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        
        if (isStream) {
            auto deleteQuery = "DELETE FROM `/Root/ColumnTableTest` ON SELECT * FROM `/Root/ColumnTableTest`";
            auto deleteQueryResult = client.ExecuteQuery(deleteQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(deleteQueryResult.IsSuccess(), deleteQueryResult.GetIssues().ToString());
        } else {
            auto deleteQuery = TStringBuilder() << "DELETE FROM `/Root/ColumnTableTest` WHERE Cast(DateTime::MakeDate(DateTime::StartOfDay(time)) as String) == \""
                             << ts.FormatLocalTime("%Y-%m-%d")
                             << "\" and class == \"test\" and uniq = \"test\";";
            auto deleteQueryResult = client.ExecuteQuery(deleteQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(deleteQueryResult.IsSuccess(), deleteQueryResult.GetIssues().ToString());
        }

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest`", "[]");
    }
}
}
