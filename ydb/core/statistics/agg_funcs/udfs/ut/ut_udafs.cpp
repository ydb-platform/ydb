#include <ydb/core/statistics/ut_common/ut_common.h>

namespace NKikimr::NStat {

Y_UNIT_TEST_SUITE(StatisticUDAFs) {
    Y_UNIT_TEST(CountMinSketch) {
        TTestEnv env(1, 1, /*useRealThreads=*/true);
        CreateDatabase(env, "Database");
        CreateColumnStoreTable(env, "Database", "Table", 4);

        // Execute a query calculating CMS on the Value column.

        const ui64 width = 1024;
        const ui64 depth = 8;
        TString query = std::format(R"(
            $cms_factory = AggregationFactory(
                "UDAF",
                ($item, $parent) -> {{ return Udf(StatisticsInternal::CountMinSketchCreate, $parent as Depends)($item, {}, {}) }},
                ($state, $item, $parent) -> {{ return Udf(StatisticsInternal::CountMinSketchAddValue, $parent as Depends)($state, $item) }},
                StatisticsInternal::CountMinSketchMerge,
                StatisticsInternal::CountMinSketchFinalize,
                StatisticsInternal::CountMinSketchSerialize,
                StatisticsInternal::CountMinSketchDeserialize,
            );

            SELECT AGGREGATE_BY(Value, $cms_factory) FROM `/Root/Database/Table`;
        )", width, depth);

        NYdb::NTable::TTableClient client(env.GetDriver());
        auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto streamPart = it.ReadNext().GetValueSync();
        UNIT_ASSERT_C(streamPart.IsSuccess(), streamPart.GetIssues());
        UNIT_ASSERT(streamPart.HasResultSet());
        NYdb::TResultSetParser parser(streamPart.GetResultSet());
        UNIT_ASSERT(parser.TryNextRow());

        // Load the result and do some basic sanity checks.

        auto value = NYdb::TValueParser(parser.GetValue(0)).GetOptionalString();
        UNIT_ASSERT(value);
        std::unique_ptr<TCountMinSketch> cms(TCountMinSketch::FromString(value->data(), value->size()));
        UNIT_ASSERT_VALUES_EQUAL(cms->GetElementCount(), ColumnTableRowsNumber);
        UNIT_ASSERT_VALUES_EQUAL(cms->GetWidth(), width);
        UNIT_ASSERT_VALUES_EQUAL(cms->GetDepth(), depth);
    }
}

}