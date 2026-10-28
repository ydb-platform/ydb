#include <ydb/core/tx/columnshard/find_empty_history_intervals/intervals.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals {
namespace {

constexpr ui64 TabletId = 123;

TLogoBlobID MakeBlobID(ui32 channel, ui32 generation, ui64 tabletId = TabletId) {
    return TLogoBlobID(tabletId, generation, 1, channel, 100, 0);
}

}   // namespace

Y_UNIT_TEST_SUITE(THistoryIntervalsTest) {
    Y_UNIT_TEST(HalfOpenGenerationRange) {
        constexpr ui32 GenerationFrom = 10;
        constexpr ui32 GenerationTo = 20;
        for (ui32 generation : { GenerationFrom - 1, GenerationFrom, GenerationTo - 1, GenerationTo }) {
            THistoryIntervals historyIntervals(TabletId, { { { 2, GenerationFrom }, { GenerationTo, 101 } } });
            historyIntervals.Exclude(MakeBlobID(2, generation));
            const bool inside = generation >= GenerationFrom && generation < GenerationTo;
            UNIT_ASSERT_VALUES_EQUAL(historyIntervals.Empty(), inside);
        }
    }

    Y_UNIT_TEST(AdjacentIntervalsAndChannels) {
        ui32 mismatches = 0;
        THistoryIntervals historyIntervals(
            TabletId, { { { 2, 10 }, { 20, 101 } }, { { 2, 20 }, { 30, 102 } }, { { 2, 30 }, { 40, 101 } }, { { 3, 10 }, { 20, 103 } } });
        const auto onMismatch = [&] {
            ++mismatches;
        };
        historyIntervals.Exclude(MakeBlobID(2, 20), 102, onMismatch);
        historyIntervals.Exclude(MakeBlobID(2, 20), 0, onMismatch);
        historyIntervals.Exclude(MakeBlobID(2, 30), 101, onMismatch);
        UNIT_ASSERT_VALUES_EQUAL(mismatches, 0);
        const THistoryIntervals::TIntervals expected{ { { 2, 10 }, { 20, 101 } }, { { 3, 10 }, { 20, 103 } } };
        UNIT_ASSERT(std::ranges::equal(historyIntervals.GetIntervals(), expected, [](const auto& actual, const auto& expected) {
            return actual.first == expected.first && actual.second.To == expected.second.To && actual.second.Group == expected.second.Group;
        }));
        historyIntervals.Exclude(MakeBlobID(3, 10), 0, onMismatch);
        UNIT_ASSERT_VALUES_EQUAL(mismatches, 1);
        historyIntervals.Exclude(MakeBlobID(2, 10));
        UNIT_ASSERT(historyIntervals.Empty());
    }

    Y_UNIT_TEST(ForeignTabletUnknownChannelAndGaps) {
        THistoryIntervals historyIntervals(TabletId, { { { 2, 10 }, { 20, 101 } }, { { 2, 30 }, { 40, 102 } }, { { 4, 10 }, { 20, 103 } } });
        // A foreign tablet, absent channel, or generation gap must not exclude a neighboring interval.
        historyIntervals.Exclude(MakeBlobID(2, 10, TabletId + 1));
        historyIntervals.Exclude(MakeBlobID(3, 15));
        historyIntervals.Exclude(MakeBlobID(2, 25));
        UNIT_ASSERT_VALUES_EQUAL(historyIntervals.GetIntervals().size(), 3);
    }
}

}   // namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals
