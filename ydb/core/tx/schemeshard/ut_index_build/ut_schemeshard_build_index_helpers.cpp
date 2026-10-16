#include <ydb/core/tx/schemeshard/index/build_index_helpers.h>
#include <ydb/core/tx/schemeshard/index/index_utils.h>

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_type_order.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSchemeShard {

Y_UNIT_TEST_SUITE(HnswPostingPartitioning) {
    TTableInfo::TPtr MakeBase(ui64 rows, ui32 partitions = 4) {
        auto table = MakeIntrusive<TTableInfo>();
        TVector<TTableShardInfo> shards;
        for (ui32 i = 0; i < partitions; ++i) {
            const ui64 end = (i + 1) * 100;
            shards.emplace_back(TShardIdx(1, i), i + 1 == partitions ? TString{} :
                TSerializedCellVec::Serialize({TCell::Make(end)}));
        }
        table->SetPartitioning(std::move(shards));
        TPartitionStats stats;
        stats.SeqNo = TMessageSeqNo{1, 1};
        stats.RowCount = rows;
        TDiskSpaceUsageDelta delta;
        table->UpdateShardStats(&delta, TShardIdx(1, 0), stats, TInstant::Zero());
        return table;
    }

    Ydb::Table::KMeansTreeSettings Settings(ui32 clusters = 10, ui32 levels = 1) {
        Ydb::Table::KMeansTreeSettings settings;
        settings.set_clusters(clusters);
        settings.set_levels(levels);
        settings.mutable_settings()->set_vector_dimension(200);
        return settings;
    }

    Y_UNIT_TEST(PlansConstructionMemoryAndSplitsLargeLeavesByBaseKey) {
        NKikimrSchemeOp::TTableDescription posting;
        NTableIndex::ConfigureHnswPostingPartitions(MakeBase(10000000), Settings(), 100, posting);
        UNIT_ASSERT_VALUES_EQUAL(posting.SplitBoundarySize(), 14);
        UNIT_ASSERT_VALUES_EQUAL(posting.GetPartitionConfig().GetPartitioningPolicy().GetMinPartitionsCount(), 15);
        ui32 leafBoundaries = 0;
        ui32 baseKeyBoundaries = 0;
        for (const auto& boundary : posting.GetSplitBoundary()) {
            TSerializedCellVec key(boundary.GetSerializedKeyPrefix());
            const auto cells = key.GetCells();
            const ui64 parent = cells[0].AsValue<ui64>();
            UNIT_ASSERT(parent >= NTableIndex::NKMeans::SetPostingParentFlag(1));
            UNIT_ASSERT(parent <= NTableIndex::NKMeans::SetPostingParentFlag(10));
            leafBoundaries += cells.size() == 1;
            baseKeyBoundaries += cells.size() == 2;
        }
        UNIT_ASSERT_VALUES_EQUAL(leafBoundaries, 9);
        UNIT_ASSERT_VALUES_EQUAL(baseKeyBoundaries, 5);
    }

    Y_UNIT_TEST(UsesFinalLevelIdsAndRespectsQuota) {
        NKikimrSchemeOp::TTableDescription posting;
        NTableIndex::ConfigureHnswPostingPartitions(MakeBase(10000000), Settings(3, 2), 3, posting);
        UNIT_ASSERT_VALUES_EQUAL(posting.SplitBoundarySize(), 2);
        for (ui32 i = 0; i < 2; ++i) {
            TSerializedCellVec key(posting.GetSplitBoundary(i).GetSerializedKeyPrefix());
            UNIT_ASSERT_VALUES_EQUAL(key.GetCells()[0].AsValue<ui64>(),
                NTableIndex::NKMeans::SetPostingParentFlag(7 + 3 * i));
        }
    }

    Y_UNIT_TEST(SingleClusterUsesBasePrimaryKeyBoundaries) {
        NKikimrSchemeOp::TTableDescription posting;
        NTableIndex::ConfigureHnswPostingPartitions(MakeBase(10000000), Settings(1), 100, posting);
        UNIT_ASSERT_VALUES_EQUAL(posting.SplitBoundarySize(), 3);
        for (ui32 i = 0; i < 3; ++i) {
            TSerializedCellVec key(posting.GetSplitBoundary(i).GetSerializedKeyPrefix());
            UNIT_ASSERT_VALUES_EQUAL(key.GetCells().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(key.GetCells()[0].AsValue<ui64>(), NTableIndex::NKMeans::SetPostingParentFlag(1));
            UNIT_ASSERT_VALUES_EQUAL(key.GetCells()[1].AsValue<ui64>(), (i + 1) * 100);
        }
    }

    Y_UNIT_TEST(PreservesExplicitBoundariesAndUniformPartitioning) {
        for (bool uniform : {false, true}) {
            NKikimrSchemeOp::TTableDescription posting;
            if (uniform) {
                posting.SetUniformPartitionsCount(2);
            } else {
                const ui64 parent = NTableIndex::NKMeans::SetPostingParentFlag(5);
                posting.AddSplitBoundary()->SetSerializedKeyPrefix(TSerializedCellVec::Serialize({TCell::Make(parent)}));
            }
            const auto before = posting.SerializeAsString();
            NTableIndex::ConfigureHnswPostingPartitions(MakeBase(10000000), Settings(), 100, posting);
            UNIT_ASSERT_VALUES_EQUAL(posting.SerializeAsString(), before);
        }
    }

    Y_UNIT_TEST(SmallOrUnknownTableDoesNotCreateEmptyPartitions) {
        for (ui64 rows : {0ull, 3ull}) {
            for (ui32 parts : {1u, 4u}) {
                NKikimrSchemeOp::TTableDescription posting;
                NTableIndex::ConfigureHnswPostingPartitions(MakeBase(rows, parts), Settings(), 100, posting);
                UNIT_ASSERT_VALUES_EQUAL(posting.SplitBoundarySize(), 0);
            }
        }
    }
}

using TCols = std::pair<std::optional<ui32>, std::optional<TString>>;
using TRange = std::pair<TCols, TCols>;

TCell MakeCell(std::optional<ui32> x) {
    if (x) {
        return TCell::Make<ui32>(*x);
    } else {
        return TCell();
    }
}

TCell MakeCell(std::optional<TString> x) {
    if (x) {
        return TCell(x->data(), x->size());
    } else {
        return TCell();
    }
}

bool RunValidation(std::initializer_list<TRange> ranges, TString& errorDescription) {
    std::vector<TString> indexColumns = {
        "index_key_1",
        "index_key_2",
    };
    std::vector<NScheme::TTypeInfoOrder> typeInfos = {
        NScheme::TTypeInfoOrder(NScheme::NTypeIds::Uint32),
        NScheme::TTypeInfoOrder(NScheme::NTypeIds::String),
    };
    std::vector<TSerializedTableRange> serializedRanges;
    std::vector<const TSerializedTableRange*> serializedRangesPtrs;
    serializedRanges.reserve(ranges.size());
    serializedRangesPtrs.reserve(serializedRanges.size());
    for (auto i = std::begin(ranges); i != std::end(ranges); ++i) {
        auto& r = serializedRanges.emplace_back();
        r.From = TSerializedCellVec(TSerializedCellVec::Serialize({MakeCell(i->first.first), MakeCell(i->first.second)}));
        r.To = TSerializedCellVec(TSerializedCellVec::Serialize({MakeCell(i->second.first), MakeCell(i->second.second)}));
        serializedRangesPtrs.push_back(&r);
    }

    return PerformCrossShardUniqIndexValidation(typeInfos, indexColumns, serializedRangesPtrs, errorDescription);
}

#define UNIT_ASSERT_VALIDATION_TRUE(...) do {                            \
    std::initializer_list<TRange> r = { __VA_ARGS__ };                   \
    TString errorDescription;                                            \
    UNIT_ASSERT_C(RunValidation(r, errorDescription), errorDescription); \
} while (false);                                                         \
/**/

#define UNIT_ASSERT_VALIDATION_FALSE(expectedError, ...) do {            \
    std::initializer_list<TRange> r = { __VA_ARGS__ };                   \
    TString errorDescription;                                            \
    UNIT_ASSERT(!RunValidation(r, errorDescription));                    \
    UNIT_ASSERT_VALUES_EQUAL(errorDescription, (expectedError));         \
} while (false);                                                         \
/**/

Y_UNIT_TEST_SUITE(CrossShardUniqIndexValidationTest) {
    Y_UNIT_TEST(Validation) {
        UNIT_ASSERT_VALIDATION_TRUE(
            TRange(TCols(1, "1"), TCols(1, "1")),
            TRange(TCols(1, "2"), TCols(10, "10")),
        );

        UNIT_ASSERT_VALIDATION_TRUE(
            TRange(TCols(1, "1"), TCols(1, std::nullopt)),
            TRange(TCols(1, std::nullopt), TCols(1, std::nullopt)),
        );

        UNIT_ASSERT_VALIDATION_TRUE(
            TRange(TCols(1, "1"), TCols(1, "1")),
        );

        UNIT_ASSERT_VALIDATION_TRUE(
            TRange(TCols(std::nullopt, std::nullopt), TCols(std::nullopt, std::nullopt)),
            TRange(TCols(std::nullopt, std::nullopt), TCols(std::nullopt, std::nullopt)),
        );

        UNIT_ASSERT_VALIDATION_FALSE("Duplicate key found: (index_key_1=1, index_key_2=~1)",
            TRange(TCols(1, "~1"), TCols(1, "~1")),
            TRange(TCols(1, "~1"), TCols(1, "~1")),
        );

        UNIT_ASSERT_VALIDATION_FALSE("Duplicate key found: (index_key_1=6, index_key_2=~6)",
            TRange(TCols(1, "~1"), TCols(2, "~2")),
            TRange(TCols(3, "~3"), TCols(4, "~4")),
            TRange(TCols(5, "~5"), TCols(6, "~6")),
            TRange(TCols(6, "~6"), TCols(8, std::nullopt)),
        );
    }
}

} // namespace NKikimr::NSchemeShard
