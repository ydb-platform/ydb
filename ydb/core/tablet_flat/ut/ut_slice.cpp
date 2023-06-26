#include <ydb/core/tablet_flat/flat_part_slice.h>
#include <ydb/core/tablet_flat/flat_part_overlay.h>
#include <ydb/core/tablet_flat/util_fmt_desc.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

namespace {

    class TRowSliceBuilder {
    public:
        TRowSliceBuilder()
            : Run(new TSlices)
        {
        }

        TRowSliceBuilder&& Add(
                TRowId firstRowId,
                bool firstInclusive,
                TRowId lastRowId,
                bool lastInclusive) &&
        {
            TVector<TCell> firstKey{ TCell::Make(firstRowId) };
            TVector<TCell> lastKey{ TCell::Make(lastRowId) };
            Run->emplace_back(
                TSerializedCellVec(firstKey),
                TSerializedCellVec(lastKey),
                firstRowId,
                lastRowId,
                firstInclusive,
                lastInclusive);
            if (Run->size() > 1) {
                auto& a = Run->at(Run->size() - 2);
                auto& b = Run->at(Run->size() - 1);
                UNIT_ASSERT_C(TSlice::LessByRowId(a, b), "Strictly increasing slices expected");
            }
            return std::move(*this);
        }

        TIntrusiveConstPtr<TSlices> Build() &&
        {
            return std::move(Run);
        }

    private:
        TIntrusivePtr<TSlices> Run;
    };

    bool EqualByRowId(const TSlice& a, const TSlice& b)
    {
        return a.FirstRowId == b.FirstRowId
            && a.LastRowId == b.LastRowId
            && a.FirstInclusive == b.FirstInclusive
            && a.LastInclusive == b.LastInclusive;
    }

    bool EqualByRowId(const TVector<TSlice>& a, const TVector<TSlice>& b)
    {
        auto ait = a.begin();
        auto bit = b.begin();
        while (ait != a.end() && bit != b.end()) {
            if (!EqualByRowId(*ait, *bit)) {
                return false;
            }
            ++ait;
            ++bit;
        }
        return (ait == a.end()) == (bit == b.end());
    }

    bool EqualByRowId(const TIntrusiveConstPtr<TSlices>& a, const TIntrusiveConstPtr<TSlices>& b)
    {
        return EqualByRowId(*a, *b);
    }

    void VerifyEqual(const TIntrusiveConstPtr<TSlices>& value, const TIntrusiveConstPtr<TSlices>& expected)
    {
        UNIT_ASSERT_C(
            EqualByRowId(value, expected),
            "Got " << NFmt::If(value.Get()) << " expected " << NFmt::If(expected.Get()));
    }

    void VerifyMerge(
            const TIntrusiveConstPtr<TSlices>& a,
            const TIntrusiveConstPtr<TSlices>& b,
            const TIntrusiveConstPtr<TSlices>& expected)
    {
        VerifyEqual(TSlices::Merge(a, b), expected);
        VerifyEqual(TSlices::Merge(b, a), expected);
    }

    void VerifyEqualCall(
            const TIntrusiveConstPtr<TSlices>& a,
            const TIntrusiveConstPtr<TSlices>& b,
            bool expected)
    {
        UNIT_ASSERT_C(
            TSlices::EqualByRowId(a, b) == expected,
            "Expected " << NFmt::If(a.Get())
            << (expected ? " to be equal " : " to not be equal ")
            << "to " << NFmt::If(b.Get()));
    }

    void VerifySupersetCall(
            const TIntrusiveConstPtr<TSlices>& a,
            const TIntrusiveConstPtr<TSlices>& b,
            bool expected)
    {
        UNIT_ASSERT_C(
            TSlices::SupersetByRowId(a, b) == expected,
            "Expected " << NFmt::If(a.Get())
            << (expected ? " to be" : " to not be")
            << " a superset of " << NFmt::If(b.Get()));
    }

    TIntrusiveConstPtr<TSlices> VerifySubtract(
            const TIntrusiveConstPtr<TSlices>& a,
            const TIntrusiveConstPtr<TSlices>& b,
            const TIntrusiveConstPtr<TSlices>& expected)
    {
        auto result = TSlices::Subtract(a, b);
        UNIT_ASSERT_C(
            EqualByRowId(result, expected),
            "Got " << NFmt::If(result.Get())
            << " for " << NFmt::If(a.Get()) << " - " << NFmt::If(b.Get())
            << " expected " << NFmt::If(expected.Get()));
        return result;
    }
};

Y_UNIT_TEST_SUITE(TPartSlice) {

    Y_UNIT_TEST(TrivialMerge)
    {
        auto x = TRowSliceBuilder().Add(0, true, 10, false).Build();
        auto e = TRowSliceBuilder().Build();

        UNIT_ASSERT(TSlices::Merge(x, e) == x.Get());
        UNIT_ASSERT(TSlices::Merge(e, x) == x.Get());
        UNIT_ASSERT(TSlices::Merge(x, nullptr) == x.Get());
        UNIT_ASSERT(TSlices::Merge(nullptr, x) == x.Get());
    }

    Y_UNIT_TEST(SimpleMerge)
    {
        auto left = TRowSliceBuilder().Add(0, true, 3, false).Build();
        auto leftExt = TRowSliceBuilder().Add(0, true, 3, true).Build();
        auto leftBigger = TRowSliceBuilder().Add(0, true, 6, false).Build();

        VerifyMerge(left, leftExt, TRowSliceBuilder().Add(0, true, 3, true).Build());
        VerifyMerge(left, leftBigger, TRowSliceBuilder().Add(0, true, 6, false).Build());
        VerifyMerge(leftExt, leftBigger, TRowSliceBuilder().Add(0, true, 6, false).Build());

        auto right = TRowSliceBuilder().Add(6, false, 9, true).Build();
        auto rightExt = TRowSliceBuilder().Add(6, true, 9, true).Build();
        auto rightBigger = TRowSliceBuilder().Add(3, false, 9, true).Build();

        VerifyMerge(right, rightExt, TRowSliceBuilder().Add(6, true, 9, true).Build());
        VerifyMerge(right, rightBigger, TRowSliceBuilder().Add(3, false, 9, true).Build());
        VerifyMerge(rightExt, rightBigger, TRowSliceBuilder().Add(3, false, 9, true).Build());

        auto mid = TRowSliceBuilder().Add(3, false, 6, false).Build();
        auto midExt = TRowSliceBuilder().Add(3, true, 6, true).Build();
        auto midBigger = TRowSliceBuilder().Add(0, true, 9, true).Build();

        VerifyMerge(mid, midExt, TRowSliceBuilder().Add(3, true, 6, true).Build());
        VerifyMerge(mid, midBigger, TRowSliceBuilder().Add(0, true, 9, true).Build());
        VerifyMerge(midExt, midBigger, TRowSliceBuilder().Add(0, true, 9, true).Build());

        VerifyMerge(left, right, TRowSliceBuilder()
            .Add(0, true, 3, false)
            .Add(6, false, 9, true)
            .Build());

        VerifyMerge(leftBigger, rightExt, TRowSliceBuilder()
            .Add(0, true, 6, false)
            .Add(6, true, 9, true)
            .Build());

        VerifyMerge(leftExt, rightBigger, TRowSliceBuilder()
            .Add(0, true, 3, true)
            .Add(3, false, 9, true)
            .Build());

        // Full intersection does not happen in practice, data size is incorrect
        VerifyMerge(leftBigger, rightBigger, TRowSliceBuilder()
            .Add(0, true, 9, true)
            .Build());
    }

    Y_UNIT_TEST(ComplexMerge)
    {
        auto a = TRowSliceBuilder()
            .Add(0, true, 4, false)
            .Add(8, true, 12, false)
            .Add(16, true, 20, false)
            .Build();
        auto b = TRowSliceBuilder()
            .Add(2, true, 6, false)
            .Add(8, false, 13, false)
            .Add(15, false, 18, true)
            .Build();
        auto expected = TRowSliceBuilder()
            .Add(0, true, 6, false)
            .Add(8, true, 13, false)
            .Add(15, false, 20, false)
            .Build();
        VerifyMerge(a, b, expected);
    }

    Y_UNIT_TEST(LongTailMerge)
    {
        auto a = TRowSliceBuilder()
            .Add(2, true, 5, false)
            .Build();
        auto b = TRowSliceBuilder()
            .Add(0, true, 4, false)
            .Add(4, true, 8, false)
            .Add(8, true, 12, false)
            .Add(12, true, 16, false)
            .Build();
        VerifyMerge(a, b, TRowSliceBuilder()
            .Add(0, true, 8, false)
            .Add(8, true, 12, false)
            .Add(12, true, 16, false)
            .Build());
    }

    Y_UNIT_TEST(CutSingle)
    {
        auto x = TRowSliceBuilder()
            .Add(2, true, 8, false)
            .Build();

        UNIT_ASSERT(TSlices::Cut(x, 0, Max<TRowId>(), {}, {}) == x.Get());
        UNIT_ASSERT(TSlices::Cut(x, 2, 8, {}, {}) == x.Get());

        VerifyEqual(
            TSlices::Cut(x, 4, Max<TRowId>(), {}, {}),
            TRowSliceBuilder()
                .Add(4, true, 8, false)
                .Build());

        VerifyEqual(
            TSlices::Cut(x, 0, 6, {}, {}),
            TRowSliceBuilder()
                .Add(2, true, 6, false)
                .Build());

        VerifyEqual(
            TSlices::Cut(x, 4, 6, {}, {}),
            TRowSliceBuilder()
                .Add(4, true, 6, false)
                .Build());

        x = TRowSliceBuilder()
            .Add(2, true, 7, true)
            .Build();
        VerifyEqual(
            TSlices::Cut(x, 2, 7, {}, {}),
            TRowSliceBuilder()
                .Add(2, true, 7, false)
                .Build());
    }

    Y_UNIT_TEST(CutMulti)
    {
        auto x = TRowSliceBuilder()
            .Add(4, true, 10, false)
            .Add(14, false, 20, true)
            .Add(24, true, 29, true)
            .Build();

        UNIT_ASSERT(TSlices::Cut(x, 0, Max<TRowId>(), {}, {}) == x.Get());
        UNIT_ASSERT(TSlices::Cut(x, 4, 30, {}, {}) == x.Get());

        VerifyEqual(
            TSlices::Cut(x, 10, 15, {}, {}),
            TRowSliceBuilder().Build());

        VerifyEqual(
            TSlices::Cut(x, 4, 21, {}, {}),
            TRowSliceBuilder()
                .Add(4, true, 10, false)
                .Add(14, false, 20, true)
                .Build());

        VerifyEqual(
            TSlices::Cut(x, 14, 30, {}, {}),
            TRowSliceBuilder()
                .Add(14, false, 20, true)
                .Add(24, true, 29, true)
                .Build());

        VerifyEqual(
            TSlices::Cut(x, 14, 21, {}, {}),
            TRowSliceBuilder()
                .Add(14, false, 20, true)
                .Build());

        VerifyEqual(
            TSlices::Cut(x, 4, 29, {}, {}),
            TRowSliceBuilder()
                .Add(4, true, 10, false)
                .Add(14, false, 20, true)
                .Add(24, true, 29, false)
                .Build());

        VerifyEqual(
            TSlices::Cut(x, 5, 20, {}, {}),
            TRowSliceBuilder()
                .Add(5, true, 10, false)
                .Add(14, false, 20, false)
                .Build());

        // N.B.: slice start inclusive flag changed
        VerifyEqual(
            TSlices::Cut(x, 15, 20, {}, {}),
            TRowSliceBuilder()
                .Add(15, true, 20, false)
                .Build());

        // N.B.: slice was cut without changing its number of rows
        VerifyEqual(
            TSlices::Cut(x, 15, 30, {}, {}),
            TRowSliceBuilder()
                .Add(15, true, 20, true)
                .Add(24, true, 29, true)
                .Build());
    }

    Y_UNIT_TEST(LookupBasics)
    {
        auto x = TRowSliceBuilder()
            .Add(4, true, 10, false)
            .Add(14, true, 20, false)
            .Add(24, true, 29, true)
            .Build();

        auto start = x->begin();
        while (true) {
            // Test some well known rows
            UNIT_ASSERT_VALUES_EQUAL(x->Lookup(start, 0) - x->begin(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(x->Lookup(start, 8) - x->begin(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(x->Lookup(start, 10) - x->begin(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(x->Lookup(start, 14) - x->begin(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(x->Lookup(start, 19) - x->begin(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(x->Lookup(start, 20) - x->begin(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(x->Lookup(start, 29) - x->begin(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(x->Lookup(start, 30) - x->begin(), 3u);

            // Test some well known ranges
            UNIT_ASSERT_VALUES_EQUAL(x->Lookup(start, 0, 4) - x->begin(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(x->Lookup(start, 0, 8) - x->begin(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(x->Lookup(start, 8, 12) - x->begin(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(x->Lookup(start, 12, 16) - x->begin(), 1u);

            if (start == x->end()) {
                break;
            }

            ++start;
        }
    }

    Y_UNIT_TEST(LookupFull)
    {
        // Build run with 16 slices, enough to test both slow and fast path
        TIntrusivePtr<TSlices> x = new TSlices;
        for (int i = 0; i < 16; ++i) {
            x->emplace_back(
                TSerializedCellVec(), // key not important
                TSerializedCellVec(), // key not important
                i * 4,
                i * 4 + 2,
                true,
                false);
        }
        const TRowId maxRowId = x->back().EndRowId() + 1;

        auto start = x->begin();
        while (true) {
            for (TRowId rowBegin = 0; rowBegin < maxRowId; ++rowBegin) {
                // Test individual rows
                {
                    bool slowHave = false;
                    for (const auto& bounds : *x) {
                        if (bounds.Has(rowBegin)) {
                            slowHave = true;
                            break;
                        }
                    }
                    auto it = x->Lookup(start, rowBegin);
                    bool fastHave = it != x->end() && it->Has(rowBegin);
                    UNIT_ASSERT_C(fastHave == slowHave,
                        "For row " << rowBegin
                        << " found position " << (it - x->begin())
                        << " slow=" << slowHave
                        << " fast=" << fastHave);
                }

                // Test all possible ranges
                for (TRowId rowEnd = rowBegin + 1; rowEnd < maxRowId; ++rowEnd) {
                    bool slowHave = false;
                    for (const auto& bounds : *x) {
                        if (bounds.Has(rowBegin, rowEnd)) {
                            slowHave = true;
                            break;
                        }
                    }
                    auto it = x->Lookup(start, rowBegin, rowEnd);
                    bool fastHave = it != x->end() && it->Has(rowBegin, rowEnd);
                    UNIT_ASSERT_C(fastHave == slowHave,
                        "For range [" << rowBegin << "," << rowEnd << ")"
                        << " found position " << (it - x->begin())
                        << " slow=" << slowHave
                        << " fast=" << fastHave);
                }
            }

            if (start == x->end()) {
                break;
            }

            ++start;
        }
    }

    Y_UNIT_TEST(EqualByRowId)
    {
        auto x = TRowSliceBuilder()
            .Add(4, true, 8, false)
            .Add(8, true, 10, false)
            .Add(14, false, 20, true)
            .Add(24, true, 27, true)
            .Add(27, false, 29, true)
            .Build();

        VerifyEqualCall(
            x,
            TRowSliceBuilder()
                .Add(3, false, 9, true)
                .Add(15, true, 21, false)
                .Add(23, false, 30, false)
                .Build(),
            true);

        VerifyEqualCall(
            x,
            TRowSliceBuilder()
                .Add(3, false, 9, true)
                .Add(14, true, 21, true)
                .Add(23, false, 30, false)
                .Build(),
            false);

        VerifyEqualCall(
            x,
            TRowSliceBuilder()
                .Add(3, false, 9, true)
                .Add(15, true, 21, false)
                .Build(),
            false);

        VerifyEqualCall(
            x,
            TRowSliceBuilder()
                .Add(3, false, 9, true)
                .Add(15, true, 21, false)
                .Add(23, false, 30, false)
                .Add(35, true, 40, false)
                .Build(),
            false);
    }

    Y_UNIT_TEST(SupersetByRowId)
    {
        auto x = TRowSliceBuilder()
            .Add(4, true, 8, false)
            .Add(8, true, 10, false)
            .Add(14, false, 20, true)
            .Add(24, true, 27, true)
            .Add(27, false, 29, true)
            .Build();

        auto empty = TRowSliceBuilder().Build();

        auto a = TRowSliceBuilder()
            .Add(5, true, 9, false)
            .Add(14, false, 20, true)
            .Add(25, true, 28, true)
            .Build();

        auto b = TRowSliceBuilder()
            .Add(5, true, 10, true)
            .Build();

        auto c = TRowSliceBuilder()
            .Add(4, true, 10, false)
            .Build();

        auto d = TRowSliceBuilder()
            .Add(4, true, 10, false)
            .Add(14, false, 20, true)
            .Add(24, true, 29, true)
            .Add(35, true, 40, false)
            .Build();

        VerifySupersetCall(empty, empty, true);
        VerifySupersetCall(x, empty, true);
        VerifySupersetCall(empty, x, false);
        VerifySupersetCall(x, x, true);

        VerifySupersetCall(x, a, true);
        VerifySupersetCall(x, b, false);
        VerifySupersetCall(x, c, true);
        VerifySupersetCall(x, d, false);
    }

    Y_UNIT_TEST(Subtract)
    {
        auto x = TRowSliceBuilder()
            .Add(4, true, 10, false)
            .Add(14, false, 20, true)
            .Add(24, true, 29, true)
            .Build();

        auto empty = TRowSliceBuilder().Build();

        UNIT_ASSERT(TSlices::Subtract(x, empty) == x);
        UNIT_ASSERT(TSlices::Subtract(empty, x) == empty);

        // Check removal of everything

        VerifySubtract(
            x,
            TRowSliceBuilder().Add(0, true, 40, false).Build(),
            empty);

        VerifySubtract(
            x,
            TRowSliceBuilder().Add(4, true, 29, true).Build(),
            empty);

        VerifySubtract(
            x,
            TRowSliceBuilder().Add(3, false, 30, false).Build(),
            empty);

        // Check removal of every hole possible

        VerifySubtract(
            x,
            TRowSliceBuilder()
                .Add(0, true, 4, false)
                .Add(10, true, 14, true)
                .Add(20, false, 24, false)
                .Add(29, false, 40, false)
                .Build(),
            x);

        // Check removal of various edges

        VerifySubtract(
            x,
            TRowSliceBuilder()
                .Add(0, true, 4, true)
                .Build(),
            TRowSliceBuilder()
                .Add(4, false, 10, false)
                .Add(14, false, 20, true)
                .Add(24, true, 29, true)
                .Build());

        VerifySubtract(
            x,
            TRowSliceBuilder()
                .Add(0, true, 5, false)
                .Build(),
            TRowSliceBuilder()
                .Add(5, true, 10, false)
                .Add(14, false, 20, true)
                .Add(24, true, 29, true)
                .Build());

        VerifySubtract(
            x,
            TRowSliceBuilder()
                .Add(9, false, 15, false)
                .Build(),
            TRowSliceBuilder()
                .Add(4, true, 9, true)
                .Add(15, true, 20, true)
                .Add(24, true, 29, true)
                .Build());

        VerifySubtract(
            x,
            TRowSliceBuilder()
                .Add(20, true, 24, true)
                .Build(),
            TRowSliceBuilder()
                .Add(4, true, 10, false)
                .Add(14, false, 20, false)
                .Add(24, false, 29, true)
                .Build());

        VerifySubtract(
            x,
            TRowSliceBuilder()
                .Add(19, false, 25, false)
                .Build(),
            TRowSliceBuilder()
                .Add(4, true, 10, false)
                .Add(14, false, 19, true)
                .Add(25, true, 29, true)
                .Build());

        VerifySubtract(
            x,
            TRowSliceBuilder()
                .Add(29, true, 40, false)
                .Build(),
            TRowSliceBuilder()
                .Add(4, true, 10, false)
                .Add(14, false, 20, true)
                .Add(24, true, 29, false)
                .Build());

        VerifySubtract(
            x,
            TRowSliceBuilder()
                .Add(28, false, 40, false)
                .Build(),
            TRowSliceBuilder()
                .Add(4, true, 10, false)
                .Add(14, false, 20, true)
                .Add(24, true, 28, true)
                .Build());

        // Check removal of various intersections

        VerifySubtract(
            x,
            TRowSliceBuilder()
                .Add(6, false, 27, true)
                .Build(),
            TRowSliceBuilder()
                .Add(4, true, 6, true)
                .Add(27, false, 29, true)
                .Build());

        VerifySubtract(
            x,
            TRowSliceBuilder()
                .Add(0, true, 6, false)
                .Add(28, false, 40, false)
                .Build(),
            TRowSliceBuilder()
                .Add(6, true, 10, false)
                .Add(14, false, 20, true)
                .Add(24, true, 28, true)
                .Build());

        VerifySubtract(
            x,
            TRowSliceBuilder()
                .Add(0, true, 6, false)
                .Add(8, true, 16, true)
                .Add(18, false, 26, true)
                .Add(28, false, 40, false)
                .Build(),
            TRowSliceBuilder()
                .Add(6, true, 8, false)
                .Add(16, false, 18, true)
                .Add(26, false, 28, true)
                .Build());
    }

    Y_UNIT_TEST(ParallelCompactions)
    {
        auto full = TRowSliceBuilder()
            .Add(0, true, 29, true)
            .Build();

        // suppose there are two parallel compactions for [5,10) and [10,15)
        // compaction will always use subsets with non-inclusive bounds
        // end result should be identical regardless of replacement order
        auto a = TRowSliceBuilder()
            .Add(4, false, 10, false)
            .Build();
        auto b = TRowSliceBuilder()
            .Add(9, false, 15, false)
            .Build();
        auto compacted = TRowSliceBuilder()
            .Add(0, true, 4, true)
            .Add(15, true, 29, true)
            .Build();

        // remove a then b
        {
            VerifyEqualCall(full, a, false);
            VerifySupersetCall(full, a, true);
            auto x = VerifySubtract(
                full,
                a,
                TRowSliceBuilder()
                    .Add(0, true, 4, true)
                    .Add(10, true, 29, true)
                    .Build());
            VerifyEqualCall(x, b, false);
            VerifySupersetCall(x, b, true);
            VerifySubtract(x, b, compacted);
        }

        // remove b then a
        {
            VerifyEqualCall(full, b, false);
            VerifySupersetCall(full, b, true);
            auto x = VerifySubtract(
                full,
                b,
                TRowSliceBuilder()
                    .Add(0, true, 9, true)
                    .Add(15, true, 29, true)
                    .Build());
            VerifyEqualCall(x, a, false);
            VerifySupersetCall(x, a, true);
            VerifySubtract(x, a, compacted);
        }
    }

    Y_UNIT_TEST(UnsplitBorrow) {
        auto orig = TRowSliceBuilder()
            .Add(0, true, 29, true)
            .Add(30, true, 39, true)
            .Add(41, true, 49, true)
            .Add(50, true, 59, false)
            .Add(60, false, 69, true)
            .Build();

        auto origOpaque = TOverlay{ nullptr, orig }.Encode();

        {
            // Since opaque is smaller than 1024 bytes it should not be modified
            auto modified = TOverlay::MaybeUnsplitSlices(origOpaque, 1024);
            UNIT_ASSERT(modified.empty());
        }

        {
            // Since opaque is more than 16 bytes it should be stitched back
            auto modified = TOverlay::MaybeUnsplitSlices(origOpaque, 16);
            UNIT_ASSERT(!modified.empty());
            auto overlay = TOverlay::Decode({}, modified);
            VerifyEqual(
                overlay.Slices,
                TRowSliceBuilder()
                    .Add(0, true, 39, true)
                    .Add(41, true, 59, false)
                    .Add(60, false, 69, true)
                    .Build());
        }
    }

}

}
}
