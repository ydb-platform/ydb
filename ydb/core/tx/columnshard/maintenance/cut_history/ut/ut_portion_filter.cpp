#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>

#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals {
namespace {

class TFixture {
    static constexpr ui32 PkColumnId = 1;
    static constexpr ui32 IndexId = 2;
    TTestBasicRuntime Runtime;
    NKikimrSchemeOp::TColumnTableSchema Schema;

public:
    TString Tier = "warm";
    std::optional<ui32> BsIndexBlobBytes;
    std::optional<NOlap::TIndexInfo> IndexInfo;

    TFixture() {
        NTxUT::TTester::Setup(Runtime);
        *Schema.AddColumns() = NArrow::NTest::TTestColumn("pk", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)).CreateColumn(PkColumnId);
        Schema.AddKeyColumnNames("pk");
        Schema.SetVersion(1);
    }

    void AddBlobStorageIndex(bool inheritPortionStorage = false) {
        *Schema.AddIndexes() =
            NOlap::NIndexes::TIndexMetaContainer(std::make_shared<NOlap::NIndexes::NMax::TIndexMeta>(IndexId, "pk_max",
                                                     NOlap::NBlobOperations::TGlobal::DefaultStorageId, inheritPortionStorage, PkColumnId))
                .SerializeToProto();
    }

    auto BuildPortion() {
        const auto pathId = TInternalPathId::FromRawValue(1);
        const NOlap::TSnapshot snapshot(1, 1);
        const auto source = NOlap::NTest::MakeTestCompactedPortion(pathId, 1, 1, 2, 2, snapshot, std::nullopt);
        const NOlap::TUnifiedBlobId blob(1, TLogoBlobID(123, 1, 1, 2, 100, 0));
        auto meta = source->GetMeta().SerializeToProto({ blob }, NOlap::NPortion::EProduced::COMPACTED);
        meta.SetTierName(Tier);
        if (BsIndexBlobBytes) {
            meta.SetBsIndexBlobBytes(*BsIndexBlobBytes);
            meta.SetIndexBlobBytes(*BsIndexBlobBytes);
            meta.SetIndexRawBytes(*BsIndexBlobBytes);
        } else {
            meta.ClearBsIndexBlobBytes();
        }
        IndexInfo = NOlap::TIndexInfo::BuildFromProto(
            1, Schema, NOlap::TTestStoragesManager::GetInstance(), std::make_shared<NOlap::TSchemaObjectsCache>());
        UNIT_ASSERT(IndexInfo);
        NOlap::TCompactedPortionInfoConstructor constructor(pathId, 1);
        constructor.SetSchemaVersion(1);
        constructor.SetAppearanceSnapshot(snapshot);
        NOlap::TFakeGroupSelector selector;
        UNIT_ASSERT(constructor.MutableMeta().LoadMetadata(meta, *IndexInfo, selector));
        return constructor.Build();
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TFindEmptyHistoryIntervalsPortionFilter) {
    Y_UNIT_TEST_TWIN(DefaultTierHasBlobsWithoutIndexBlobs, explicitDefaultTier) {
        TFixture f;
        f.Tier = explicitDefaultTier ? NOlap::NBlobOperations::TGlobal::DefaultStorageId : TString();
        f.BsIndexBlobBytes = 0;
        const auto portion = f.BuildPortion();
        UNIT_ASSERT(portion->HasBlobsInBlobStorage(*f.IndexInfo));
    }

    Y_UNIT_TEST(ExternalTierWithoutSummaryOrIndexHasNoBlobs) {
        TFixture f;
        const auto portion = f.BuildPortion();
        UNIT_ASSERT(!portion->HasBlobsInBlobStorage(*f.IndexInfo));
    }

    Y_UNIT_TEST(MissingSummaryFallsBackToIndexStorage) {
        TFixture f;
        f.AddBlobStorageIndex();
        const auto portion = f.BuildPortion();
        UNIT_ASSERT(portion->HasBlobsInBlobStorage(*f.IndexInfo));
    }

    Y_UNIT_TEST(MissingSummaryRespectsInheritedIndexStorage) {
        TFixture f;
        f.AddBlobStorageIndex(true);
        const auto portion = f.BuildPortion();
        UNIT_ASSERT(!portion->HasBlobsInBlobStorage(*f.IndexInfo));
    }

    Y_UNIT_TEST(ZeroSummaryOverridesIndexStorage) {
        TFixture f;
        f.AddBlobStorageIndex();
        f.BsIndexBlobBytes = 0;
        const auto portion = f.BuildPortion();
        UNIT_ASSERT(!portion->HasBlobsInBlobStorage(*f.IndexInfo));
    }

    Y_UNIT_TEST(PositiveSummaryHasBlobsOnExternalTier) {
        TFixture f;
        f.AddBlobStorageIndex();
        f.BsIndexBlobBytes = 100;
        const auto portion = f.BuildPortion();
        UNIT_ASSERT(portion->HasBlobsInBlobStorage(*f.IndexInfo));
    }
}

}   // namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals
