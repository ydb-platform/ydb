#include <ydb/core/tx/columnshard/find_empty_history_intervals/portions.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals {

Y_UNIT_TEST_SUITE(TFindEmptyHistoryIntervalsPortionFilter) {
    Y_UNIT_TEST(PortionStorageSummary) {
        TTestBasicRuntime runtime;
        NTxUT::TTester::Setup(runtime);
        const auto pathId = TInternalPathId::FromRawValue(1);
        const NOlap::TSnapshot snapshot(1, 1);
        const auto source = NOlap::NTest::MakeTestCompactedPortion(pathId, 1, 1, 2, 2, snapshot, std::nullopt);
        const NOlap::TUnifiedBlobId blob(1, TLogoBlobID(123, 1, 1, 2, 100, 0));
        const auto indexInfo = NOlap::NTest::MakePortionTestIndexInfo();

        struct TCase {
            TString Tier;
            std::optional<ui32> BsIndexBytes;
            bool Scan;
        };

        const std::vector<TCase> cases = {
            { "", std::nullopt, true },
            { "", 0, true },
            { "", 100, true },
            { NOlap::NBlobOperations::TGlobal::DefaultStorageId, 0, true },
            { "warm", std::nullopt, true },
            { "warm", 100, true },
            { "warm", 0, false },
        };
        for (const auto& test : cases) {
            auto meta = source->GetMeta().SerializeToProto({ blob }, NOlap::NPortion::EProduced::COMPACTED);
            meta.SetTierName(test.Tier);
            if (test.BsIndexBytes) {
                meta.SetBsIndexBlobBytes(*test.BsIndexBytes);
                meta.SetIndexBlobBytes(*test.BsIndexBytes);
                meta.SetIndexRawBytes(*test.BsIndexBytes);
            } else {
                meta.ClearBsIndexBlobBytes();
            }
            NOlap::TCompactedPortionInfoConstructor constructor(pathId, 1);
            constructor.SetSchemaVersion(1);
            constructor.SetAppearanceSnapshot(snapshot);
            NOlap::TFakeGroupSelector selector;
            UNIT_ASSERT(constructor.MutableMeta().LoadMetadata(meta, indexInfo, selector));
            const auto portion = constructor.Build();
            UNIT_ASSERT_VALUES_EQUAL(ShouldScanPortion(*portion), test.Scan);
        }
    }
}

}   // namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals
