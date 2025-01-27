#include <ydb/core/scheme/scheme_tabledefs.h>

#include <ydb/core/scheme_types/scheme_types.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(SchemeRanges) {

    TVector<NScheme::TTypeInfo> MakeTypes(size_t keysCount) {
        TVector<NScheme::TTypeInfo> types;
        types.reserve(keysCount);
        for (size_t i = 0; i < keysCount; ++i) {
            types.push_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32));
        }
        return types;
    }

    TCell MakeUi32(ui32 key) {
        return TCell::Make(ui32(key));
    }

    TCell MakeNull() {
        return TCell();
    }

    Y_UNIT_TEST(RangesBorders) {
        auto types = MakeTypes(1);

        for (ui32 flags = 0; flags < (1 << 4); ++flags) {
            TVector<TCell> firstLeft = {MakeUi32(1)};
            TVector<TCell> firstRight = {MakeUi32(10)};
            TVector<TCell> secondLeft = {MakeUi32(1)};
            TVector<TCell> sedondRight = {MakeUi32(10)};
            TTableRange first(firstLeft, ((flags >> 0) & 1), firstRight, ((flags >> 1) & 1));
            TTableRange second(secondLeft, ((flags >> 2) & 1), sedondRight, ((flags >> 3) & 1));
            UNIT_ASSERT_EQUAL(CompareRanges(first, second, types), 0);
        }

        TVector<TCell> firstLeft = {MakeUi32(1)};
        TVector<TCell> firstRight = {MakeUi32(10)};
        TVector<TCell> secondLeft = {MakeUi32(10)};
        TVector<TCell> sedondRight = {MakeUi32(100)};

        {
            TTableRange first(firstLeft, true, firstRight, true);
            TTableRange second(secondLeft, true, sedondRight, true);
            UNIT_ASSERT_EQUAL(CompareRanges(first, second, types), 0);
            UNIT_ASSERT_EQUAL(CompareRanges(second, first, types), 0);
        }

        {
            TTableRange first(firstLeft, true, firstRight, false);
            TTableRange second(secondLeft, true, sedondRight, true);
            UNIT_ASSERT_EQUAL(CompareRanges(first, second, types), -1);
            UNIT_ASSERT_EQUAL(CompareRanges(second, first, types), 1);
        }

        {
            TTableRange first(firstLeft, true, firstRight, true);
            TTableRange second(secondLeft, false, sedondRight, true);
            UNIT_ASSERT_EQUAL(CompareRanges(first, second, types), -1);
            UNIT_ASSERT_EQUAL(CompareRanges(second, first, types), 1);
        }

        {
            TTableRange first(firstLeft, true, firstRight, false);
            TTableRange second(secondLeft, false, sedondRight, true);
            UNIT_ASSERT_EQUAL(CompareRanges(first, second, types), -1);
            UNIT_ASSERT_EQUAL(CompareRanges(second, first, types), 1);
        }

        {
            TTableRange first(firstLeft, false, firstRight, false);
            TTableRange second(secondLeft, false, sedondRight, false);
            UNIT_ASSERT_EQUAL(CompareRanges(first, second, types), -1);
            UNIT_ASSERT_EQUAL(CompareRanges(second, first, types), 1);
        }

        {
            TTableRange first(firstLeft, false, firstRight, true);
            TTableRange second(secondLeft, false, sedondRight, false);
            UNIT_ASSERT_EQUAL(CompareRanges(first, second, types), -1);
            UNIT_ASSERT_EQUAL(CompareRanges(second, first, types), 1);
        }

        {
            TTableRange first(firstLeft, false, firstRight, false);
            TTableRange second(secondLeft, true, sedondRight, false);
            UNIT_ASSERT_EQUAL(CompareRanges(first, second, types), -1);
            UNIT_ASSERT_EQUAL(CompareRanges(second, first, types), 1);
        }

        {
            TTableRange first(firstLeft, false, firstRight, true);
            TTableRange second(secondLeft, true, sedondRight, false);
            UNIT_ASSERT_EQUAL(CompareRanges(first, second, types), 0);
            UNIT_ASSERT_EQUAL(CompareRanges(second, first, types), 0);
        }
    }

    Y_UNIT_TEST(CmpBorders) {
        auto types = MakeTypes(1);

        TVector<TCell> b1 = {MakeUi32(1)};
        TVector<TCell> b10 = {MakeUi32(10)};
        TVector<TCell> b100 = {MakeUi32(100)};

        for (ui32 flags = 0; flags < (1 << 2); ++flags) {
            UNIT_ASSERT_EQUAL((CompareBorders<false, true>(b1, b10, ((flags >> 0) & 1), ((flags >> 1) & 1), types)), -1);
            UNIT_ASSERT_EQUAL((CompareBorders<true, false>(b100, b10, ((flags >> 0) & 1), ((flags >> 1) & 1), types)), 1);
        }

        UNIT_ASSERT_EQUAL((CompareBorders<true, true>(b10, b10, true, true, types)), 0);
        UNIT_ASSERT_EQUAL((CompareBorders<true, true>(b10, b10, false, true, types)), -1);
        UNIT_ASSERT_EQUAL((CompareBorders<true, true>(b10, b10, true, false, types)), 1);
        UNIT_ASSERT_EQUAL((CompareBorders<true, true>(b10, b10, false, false, types)), 0);

        UNIT_ASSERT_EQUAL((CompareBorders<false, false>(b10, b10, true, true, types)), 0);
        UNIT_ASSERT_EQUAL((CompareBorders<false, false>(b10, b10, false, true, types)), 1);
        UNIT_ASSERT_EQUAL((CompareBorders<false, false>(b10, b10, true, false, types)), -1);
        UNIT_ASSERT_EQUAL((CompareBorders<false, false>(b10, b10, false, false, types)), 0);

        UNIT_ASSERT_EQUAL((CompareBorders<true, false>(b10, b10, true, true, types)), 0);
        UNIT_ASSERT_EQUAL((CompareBorders<true, false>(b10, b10, false, true, types)), -1);
        UNIT_ASSERT_EQUAL((CompareBorders<true, false>(b10, b10, true, false, types)), -1);
        UNIT_ASSERT_EQUAL((CompareBorders<true, false>(b10, b10, false, false, types)), -1);

        UNIT_ASSERT_EQUAL((CompareBorders<false, true>(b10, b10, true, true, types)), 0);
        UNIT_ASSERT_EQUAL((CompareBorders<false, true>(b10, b10, false, true, types)), 1);
        UNIT_ASSERT_EQUAL((CompareBorders<false, true>(b10, b10, true, false, types)), 1);
        UNIT_ASSERT_EQUAL((CompareBorders<false, true>(b10, b10, false, false, types)), 1);
    }
}

}
