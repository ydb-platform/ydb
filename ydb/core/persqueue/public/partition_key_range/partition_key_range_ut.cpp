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

TString MakeKey(std::initializer_list<ui64> values) {
    TVector<TCell> cells;
    for (const ui64 value : values) {
        cells.push_back(TCell::Make<ui64>(value));
    }
    return TSerializedCellVec::Serialize(cells);
}

TString MakeNullPaddedKey(ui64 value) {
    const TVector<TCell> cells = {TCell::Make<ui64>(value), TCell()};
    return TSerializedCellVec::Serialize(cells);
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
    Y_UNIT_TEST(ShouldChooseWeightedMedianKeyWhenKeysAreDistinct) {
        // given
        const TVector<TWeightedKey> keys = {
            {MakeKey({4}), 1},
            {MakeKey({1}), 1},
            {MakeKey({3}), 1},
            {MakeKey({2}), 1},
        };

        // when
        const auto boundary = ChooseSplitBoundary(keys, MakeKeyRange(Nothing(), Nothing()), Uint64Key);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeKey({3}));
    }

    Y_UNIT_TEST(ShouldCompareKeysByTypeWhenBytesOrderDiffers) {
        // given
        const TVector<TWeightedKey> keys = {
            {MakeKey({1}), 1},
            {MakeKey({256}), 1},
            {MakeKey({2}), 1},
        };

        // when
        const auto boundary = ChooseSplitBoundary(keys, MakeKeyRange(Nothing(), Nothing()), Uint64Key);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeKey({256}));
    }

    Y_UNIT_TEST(ShouldNotChooseBoundaryWhenAllKeysAreEqual) {
        // given
        const TVector<TWeightedKey> keys = {
            {MakeKey({5}), 10},
            {MakeKey({5}), 20},
        };

        // when
        const auto boundary = ChooseSplitBoundary(keys, MakeKeyRange(Nothing(), Nothing()), Uint64Key);

        // then
        UNIT_ASSERT(!boundary);
    }

    Y_UNIT_TEST(ShouldChooseKeyAboveFromBoundWhenLowestKeyIsHeaviest) {
        // given
        const TVector<TWeightedKey> keys = {
            {MakeKey({10}), 100},
            {MakeKey({11}), 1},
        };

        // when
        const auto boundary = ChooseSplitBoundary(keys, MakeKeyRange(MakeBound({10}), Nothing()), Uint64Key);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeKey({11}));
    }

    Y_UNIT_TEST(ShouldIgnoreKeyWhenItIsOutsideRangeOrHasWrongSize) {
        // given
        const TVector<TWeightedKey> keys = {
            {MakeKey({5}), 100},
            {MakeKey({12}), 1},
            {MakeKey({15}), 1},
            {MakeKey({20}), 100},
            {MakeKey({13, 1}), 100},
            {TSerializedCellVec::Serialize(TVector<TCell>{TCell("abc", 3)}), 100},
            {"not a key", 100},
        };

        // when
        const auto boundary = ChooseSplitBoundary(keys, MakeKeyRange(MakeBound({10}), MakeBound({20})), Uint64Key);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeKey({15}));
    }

    Y_UNIT_TEST(ShouldShortenBoundaryToKeyPrefixWhenPrefixSeparatesKeys) {
        // given
        const TVector<TWeightedKey> keys = {
            {MakeKey({1, 5}), 1},
            {MakeKey({1, 7}), 1},
            {MakeKey({2, 3}), 1},
            {MakeKey({2, 9}), 1},
        };

        // when
        const auto boundary = ChooseSplitBoundary(keys, MakeKeyRange(Nothing(), Nothing()), Uint64PairKey);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeNullPaddedKey(2));
    }

    Y_UNIT_TEST(ShouldKeepFullKeyWhenPrefixDoesNotSeparateKeys) {
        // given
        const TVector<TWeightedKey> keys = {
            {MakeKey({1, 5}), 1},
            {MakeKey({1, 7}), 1},
        };

        // when
        const auto boundary = ChooseSplitBoundary(keys, MakeKeyRange(Nothing(), Nothing()), Uint64PairKey);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeKey({1, 7}));
    }

    Y_UNIT_TEST(ShouldShortenCompositeBoundaryToTypedPrefixWhenFirstColumnSeparatesKeys) {
        // given
        const TVector<TWeightedKey> keys = {
            {MakeUint32Utf8Key(256, "a"), 1},
            {MakeUint32Utf8Key(1, "x"), 1},
            {MakeUint32Utf8Key(256, "b"), 1},
            {MakeUint32Utf8Key(1, "y"), 1},
        };

        // when
        const auto boundary = ChooseSplitBoundary(keys, MakeKeyRange(Nothing(), Nothing()), Uint32Utf8Key);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeNullPaddedUint32Key(256));
    }

    Y_UNIT_TEST(ShouldKeepFullCompositeKeyWhenOnlyUtf8ColumnSeparatesKeys) {
        // given
        const TVector<TWeightedKey> keys = {
            {MakeUint32Utf8Key(7, "b"), 1},
            {MakeUint32Utf8Key(7, "a"), 1},
        };

        // when
        const auto boundary = ChooseSplitBoundary(keys, MakeKeyRange(Nothing(), Nothing()), Uint32Utf8Key);

        // then
        UNIT_ASSERT(boundary);
        UNIT_ASSERT_VALUES_EQUAL(*boundary, MakeUint32Utf8Key(7, "b"));
    }
}
