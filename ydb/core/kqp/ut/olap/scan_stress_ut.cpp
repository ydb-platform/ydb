#include "helpers/local.h"
#include "helpers/writer.h"

#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <library/cpp/testing/unittest/registar.h>

#include <thread>

namespace NKikimr::NKqp {

// Reproduction for https://github.com/ydb-platform/ydb/issues/47942:
// SIGSEGV in TAccessorsCollection::RemainOnly / TProjectionProcessor on scans with
// aggregation over many overlapping portions (duplicate filtering active) under
// concurrent writes. Run under TSAN/ASAN to detect the underlying race.
Y_UNIT_TEST_SUITE(KqpOlapScanStress) {
    Y_UNIT_TEST(AggregationWithOverlapsAndConcurrentWrites) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        for (ui32 i = 0; i < 20; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
        }

        const TString query = R"(
            --!syntax_v1
            SELECT SUM(`level`), COUNT(*), MAX(`timestamp`)
            FROM `/Root/olapStore/olapTable`
            WHERE `timestamp` >= CAST(3000000 AS Timestamp) AND `level` >= 0
        )";

        std::atomic<bool> stop{ false };
        std::atomic<ui64> errors{ 0 };
        std::vector<std::thread> readers;
        for (ui32 t = 0; t < 4; ++t) {
            readers.emplace_back([&]() {
                while (!stop.load()) {
                    auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();
                    if (!it.IsSuccess()) {
                        ++errors;
                        continue;
                    }
                    try {
                        StreamResultToYson(it);
                    } catch (...) {
                        ++errors;
                    }
                }
            });
        }

        for (ui32 i = 0; i < 50; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
        }

        stop = true;
        for (auto& th : readers) {
            th.join();
        }
        UNIT_ASSERT_VALUES_EQUAL(errors.load(), 0);
    }
}

}   // namespace NKikimr::NKqp
