#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
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
        constexpr ui32 pkColumnId = 1;
        constexpr ui32 indexId = 2;
        NKikimrSchemeOp::TColumnTableSchema schema;
        *schema.AddColumns() = NArrow::NTest::TTestColumn("pk", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)).CreateColumn(pkColumnId);
        schema.AddKeyColumnNames("pk");
        schema.SetVersion(1);

        struct TCase {
            TString Tier;
            std::optional<ui32> BsIndexBytes;
            bool HasBlobs;
            std::optional<TString> IndexStorageId = {};
            bool InheritPortionStorage = false;
        };

        const std::vector<TCase> cases = {
            { "", 0, true },
            { NOlap::NBlobOperations::TGlobal::DefaultStorageId, 0, true },
            { "warm", std::nullopt, false },
            { "warm", std::nullopt, true, NOlap::NBlobOperations::TGlobal::DefaultStorageId },
            { "warm", std::nullopt, false, NOlap::NBlobOperations::TGlobal::DefaultStorageId, true },
            { "warm", std::nullopt, false, NOlap::NBlobOperations::TGlobal::LocalMetadataStorageId },
            { "warm", 0, false, NOlap::NBlobOperations::TGlobal::DefaultStorageId },
        };
        for (const auto& test : cases) {
            schema.ClearIndexes();
            if (test.IndexStorageId) {
                *schema.AddIndexes() =
                    NOlap::NIndexes::TIndexMetaContainer(std::make_shared<NOlap::NIndexes::NMax::TIndexMeta>(
                                                             indexId, "pk_max", *test.IndexStorageId, test.InheritPortionStorage, pkColumnId))
                        .SerializeToProto();
            }
            const auto indexInfo = NOlap::TIndexInfo::BuildFromProto(
                1, schema, NOlap::TTestStoragesManager::GetInstance(), std::make_shared<NOlap::TSchemaObjectsCache>());
            UNIT_ASSERT(indexInfo);
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
            UNIT_ASSERT(constructor.MutableMeta().LoadMetadata(meta, *indexInfo, selector));
            const auto portion = constructor.Build();
            UNIT_ASSERT_VALUES_EQUAL(portion->HasBlobsInBlobStorage(*indexInfo), test.HasBlobs);
        }
    }
}

}   // namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals
