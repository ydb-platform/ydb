#include "partition_key_range.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NPQ;

namespace {

TMaybe<TSerializedCellVec> MakeBound(std::initializer_list<ui64> values) {
    TVector<TCell> cells;
    for (const ui64 value : values) {
        cells.push_back(TCell::Make<ui64>(value));
    }
    return TSerializedCellVec(TSerializedCellVec::Serialize(cells));
}

TMaybe<TSerializedCellVec> MakeNullPaddedBound(ui64 value) {
    const TVector<TCell> cells = {TCell::Make<ui64>(value), TCell()};
    return TSerializedCellVec(TSerializedCellVec::Serialize(cells));
}

TPartitionKeyRange MakeKeyRange(TMaybe<TSerializedCellVec> fromBound, TMaybe<TSerializedCellVec> toBound) {
    TPartitionKeyRange range;
    range.FromBound = std::move(fromBound);
    range.ToBound = std::move(toBound);
    return range;
}

const TVector<NScheme::TTypeInfo> Uint64Key = {NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)};
const TVector<NScheme::TTypeInfo> Uint64PairKey = {
    NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),
    NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),
};
const TVector<NScheme::TTypeInfo> Uint32Utf8Key = {
    NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
    NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
};

TString MakeUint32Utf8Key(ui32 id, TStringBuf name) {
    const TVector<TCell> cells = {TCell::Make<ui32>(id), TCell(name.data(), name.size())};
    return TSerializedCellVec::Serialize(cells);
}

TString MakeNullPaddedUint32Key(ui32 id) {
    const TVector<TCell> cells = {TCell::Make<ui32>(id), TCell()};
    return TSerializedCellVec::Serialize(cells);
}

TMaybe<TSerializedCellVec> MakeKeyBound(const TString& key) {
    return TSerializedCellVec(key);
}

TWeightedKeyRange MakeSource(TMaybe<TSerializedCellVec> fromBound, TMaybe<TSerializedCellVec> toBound, ui64 weight) {
    return {MakeKeyRange(std::move(fromBound), std::move(toBound)), weight};
}

} // anonymous

Y_UNIT_TEST_SUITE(TPartitionKeyRangeIntersectsTest) {
    Y_UNIT_TEST(ShouldIntersectWhenCompositeKeyRangesOverlapInUtf8Column) {
        // given
        const auto lhs = MakeKeyRange(MakeKeyBound(MakeUint32Utf8Key(1, "a")), MakeKeyBound(MakeUint32Utf8Key(1, "m")));
        const auto rhs = MakeKeyRange(MakeKeyBound(MakeUint32Utf8Key(1, "k")), MakeKeyBound(MakeNullPaddedUint32Key(2)));

        // when
        const bool intersects = Intersects(lhs, rhs, Uint32Utf8Key);

        // then
        UNIT_ASSERT(intersects);
    }

    Y_UNIT_TEST(ShouldNotIntersectWhenCompositeKeyRangeStartsAtNullPaddedBound) {
        // given
        const auto lhs = MakeKeyRange(MakeKeyBound(MakeUint32Utf8Key(1, "z")), MakeKeyBound(MakeNullPaddedUint32Key(256)));
        const auto rhs = MakeKeyRange(MakeKeyBound(MakeUint32Utf8Key(256, "")), Nothing());

        // when
        const bool intersects = Intersects(lhs, rhs, Uint32Utf8Key);

        // then
        UNIT_ASSERT(!intersects);
    }

    Y_UNIT_TEST(ShouldCompareFirstCompositeKeyColumnByTypeWhenBytesOrderDiffers) {
        // given
        const auto lhs = MakeKeyRange(Nothing(), MakeKeyBound(MakeNullPaddedUint32Key(256)));
        const auto rhs = MakeKeyRange(MakeKeyBound(MakeUint32Utf8Key(2, "a")), MakeKeyBound(MakeUint32Utf8Key(3, "a")));

        // when
        const bool intersects = Intersects(lhs, rhs, Uint32Utf8Key);

        // then
        UNIT_ASSERT(intersects);
    }

    Y_UNIT_TEST(ShouldIntersectWhenRangesOverlap) {
        // given
        const auto lhs = MakeKeyRange(MakeBound({100}), MakeBound({200}));
        const auto rhs = MakeKeyRange(MakeBound({150}), MakeBound({300}));

        // when
        const bool intersects = Intersects(lhs, rhs, Uint64Key);

        // then
        UNIT_ASSERT(intersects);
    }

    Y_UNIT_TEST(ShouldNotIntersectWhenRangesTouch) {
        // given
        const auto lhs = MakeKeyRange(MakeBound({100}), MakeBound({200}));
        const auto rhs = MakeKeyRange(MakeBound({200}), MakeBound({300}));

        // when
        const bool intersects = Intersects(lhs, rhs, Uint64Key);

        // then
        UNIT_ASSERT(!intersects);
    }

    Y_UNIT_TEST(ShouldIntersectWhenRangeIsUnbounded) {
        // given
        const auto lhs = MakeKeyRange(Nothing(), Nothing());
        const auto rhs = MakeKeyRange(MakeBound({300}), Nothing());

        // when
        const bool intersects = Intersects(lhs, rhs, Uint64Key);

        // then
        UNIT_ASSERT(intersects);
    }

    Y_UNIT_TEST(ShouldNotIntersectWhenLeftRangeEndsBeforeRightUnboundedRange) {
        // given
        const auto lhs = MakeKeyRange(Nothing(), MakeBound({100}));
        const auto rhs = MakeKeyRange(MakeBound({300}), Nothing());

        // when
        const bool intersects = Intersects(lhs, rhs, Uint64Key);

        // then
        UNIT_ASSERT(!intersects);
    }

    Y_UNIT_TEST(ShouldCompareKeysByTypeWhenBytesOrderDiffers) {
        // given
        const auto lhs = MakeKeyRange(MakeBound({1}), MakeBound({2}));
        const auto rhs = MakeKeyRange(MakeBound({256}), MakeBound({300}));

        // when
        const bool intersects = Intersects(lhs, rhs, Uint64Key);

        // then
        UNIT_ASSERT(!intersects);
    }

    Y_UNIT_TEST(ShouldNotIntersectWhenKeyStartsWithExclusiveBoundPrefix) {
        // given
        const auto lhs = MakeKeyRange(MakeBound({100}), MakeBound({200}));
        const auto rhs = MakeKeyRange(MakeBound({200, 5}), Nothing());

        // when
        const bool intersects = Intersects(lhs, rhs, Uint64PairKey);

        // then
        UNIT_ASSERT(!intersects);
    }

    Y_UNIT_TEST(ShouldIntersectWhenKeyIsBelowExclusiveBoundPrefix) {
        // given
        const auto lhs = MakeKeyRange(MakeBound({100}), MakeBound({200}));
        const auto rhs = MakeKeyRange(MakeBound({199, 5}), Nothing());

        // when
        const bool intersects = Intersects(lhs, rhs, Uint64PairKey);

        // then
        UNIT_ASSERT(intersects);
    }

    Y_UNIT_TEST(ShouldNotIntersectWhenNullPaddedBoundStartsAtExclusiveBoundPrefix) {
        // given
        const auto shardRange = MakeKeyRange(MakeBound({100}), MakeBound({200}));
        const auto partitionRange = MakeKeyRange(MakeNullPaddedBound(200), Nothing());

        // when
        const bool intersects = Intersects(shardRange, partitionRange, Uint64PairKey);

        // then
        UNIT_ASSERT(!intersects);
    }

    Y_UNIT_TEST(ShouldIntersectWhenNullPaddedRangeHasSameBoundsAsPrefixRange) {
        // given
        const auto shardRange = MakeKeyRange(MakeBound({100}), MakeBound({200}));
        const auto partitionRange = MakeKeyRange(MakeNullPaddedBound(100), MakeNullPaddedBound(200));

        // when
        const bool intersects = Intersects(shardRange, partitionRange, Uint64PairKey);

        // then
        UNIT_ASSERT(intersects);
    }
}

Y_UNIT_TEST_SUITE(TPartitionKeyRangeChooseSplitBoundaryTest) {
    Y_UNIT_TEST(ShouldChooseBoundaryThatSplitsLoadInHalfWhenSourcesAreAdjacent) {
        // given
        const TVector<TWeightedKeyRange> sources = {
            MakeSource(Nothing(), MakeBound({30}), 200),
            MakeSource(MakeBound({30}), MakeBound({60}), 500),
            MakeSource(MakeBound({60}), MakeBound({100}), 300),
        };

        // when
        const auto boundary = ChooseSplitBoundary(sources, MakeKeyRange(Nothing(), MakeBound({100})), Uint64Key);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeBound({60})->GetBuffer());
    }

    Y_UNIT_TEST(ShouldNotChooseBoundaryWhenPartitionHasOneSource) {
        // given
        const TVector<TWeightedKeyRange> sources = {
            MakeSource(MakeBound({10}), MakeBound({50}), 1000),
        };

        // when
        const auto boundary = ChooseSplitBoundary(sources, MakeKeyRange(Nothing(), MakeBound({100})), Uint64Key);

        // then
        UNIT_ASSERT(!boundary);
    }

    Y_UNIT_TEST(ShouldChooseInnerBoundaryWhenAnotherBoundaryIsOnPartitionEdge) {
        // given
        const TVector<TWeightedKeyRange> sources = {
            MakeSource(Nothing(), MakeBound({30}), 500),
            MakeSource(MakeBound({30}), MakeBound({60}), 200),
            MakeSource(MakeBound({60}), MakeBound({100}), 300),
        };

        // when
        const auto boundary = ChooseSplitBoundary(sources, MakeKeyRange(MakeBound({30}), MakeBound({100})), Uint64Key);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeBound({60})->GetBuffer());
    }

    Y_UNIT_TEST(ShouldNotChooseBoundaryWhenSourcesAreOutsidePartition) {
        // given
        const TVector<TWeightedKeyRange> sources = {
            MakeSource(Nothing(), MakeBound({30}), 500),
            MakeSource(MakeBound({30}), MakeBound({60}), 500),
        };

        // when
        const auto boundary = ChooseSplitBoundary(sources, MakeKeyRange(MakeBound({100}), Nothing()), Uint64Key);

        // then
        UNIT_ASSERT(!boundary);
    }

    Y_UNIT_TEST(ShouldChooseMiddleBoundaryWhenLoadIsEqual) {
        // given
        const TVector<TWeightedKeyRange> sources = {
            MakeSource(Nothing(), MakeBound({10}), 100),
            MakeSource(MakeBound({10}), MakeBound({20}), 100),
            MakeSource(MakeBound({20}), MakeBound({30}), 100),
            MakeSource(MakeBound({30}), Nothing(), 100),
        };

        // when
        const auto boundary = ChooseSplitBoundary(sources, MakeKeyRange(Nothing(), Nothing()), Uint64Key);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeBound({20})->GetBuffer());
    }

    Y_UNIT_TEST(ShouldOrderSourcesByTypedKeyWhenBytesOrderDiffers) {
        // given
        const TVector<TWeightedKeyRange> sources = {
            MakeSource(MakeBound({256}), Nothing(), 100),
            MakeSource(Nothing(), MakeBound({2}), 300),
            MakeSource(MakeBound({2}), MakeBound({256}), 100),
        };

        // when
        const auto boundary = ChooseSplitBoundary(sources, MakeKeyRange(Nothing(), Nothing()), Uint64Key);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeBound({2})->GetBuffer());
    }

    Y_UNIT_TEST(ShouldPadBoundaryWithNullsWhenSourceBoundIsKeyPrefix) {
        // given
        const TVector<TWeightedKeyRange> sources = {
            MakeSource(Nothing(), MakeBound({5}), 100),
            MakeSource(MakeBound({5}), Nothing(), 100),
        };

        // when
        const auto boundary = ChooseSplitBoundary(sources, MakeKeyRange(Nothing(), Nothing()), Uint64PairKey);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeNullPaddedBound(5)->GetBuffer());
    }

    Y_UNIT_TEST(ShouldChooseChildBoundaryWhenIdleParentIsListedBeforeChildren) {
        // given
        const TVector<TWeightedKeyRange> sources = {
            MakeSource(Nothing(), Nothing(), 0),
            MakeSource(Nothing(), MakeBound({50}), 100),
            MakeSource(MakeBound({50}), Nothing(), 100),
        };

        // when
        const auto boundary = ChooseSplitBoundary(sources, MakeKeyRange(Nothing(), Nothing()), Uint64Key);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeBound({50})->GetBuffer());
    }

    Y_UNIT_TEST(ShouldChooseChildBoundaryWhenIdleParentIsListedAfterFirstChild) {
        // given
        const TVector<TWeightedKeyRange> sources = {
            MakeSource(Nothing(), MakeBound({50}), 100),
            MakeSource(Nothing(), Nothing(), 0),
            MakeSource(MakeBound({50}), Nothing(), 100),
        };

        // when
        const auto boundary = ChooseSplitBoundary(sources, MakeKeyRange(Nothing(), Nothing()), Uint64Key);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeBound({50})->GetBuffer());
    }

    Y_UNIT_TEST(ShouldNotChooseBoundaryWhenActiveSourceCoversIt) {
        // given
        const TVector<TWeightedKeyRange> sources = {
            MakeSource(MakeBound({50}), Nothing(), 100),
            MakeSource(Nothing(), Nothing(), 100),
            MakeSource(Nothing(), MakeBound({50}), 100),
        };

        // when
        const auto boundary = ChooseSplitBoundary(sources, MakeKeyRange(Nothing(), Nothing()), Uint64Key);

        // then
        UNIT_ASSERT(!boundary);
    }
}
