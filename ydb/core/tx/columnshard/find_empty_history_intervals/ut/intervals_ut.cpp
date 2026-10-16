#include <ydb/core/tx/columnshard/find_empty_history_intervals/intervals.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals {
namespace {

constexpr ui64 TabletId = 123;

TLogoBlobID Blob(ui32 channel, ui32 generation, ui64 tabletId = TabletId) {
    return TLogoBlobID(tabletId, generation, 1, channel, 100, 0);
}

}   // namespace

Y_UNIT_TEST_SUITE(TFindEmptyHistoryIntervalsFilter) {
    Y_UNIT_TEST(HalfOpenGenerationRange) {
        constexpr ui32 GenerationFrom = 10;
        constexpr ui32 GenerationTo = 20;
        for (ui32 generation : { GenerationFrom - 1, GenerationFrom, GenerationTo - 1, GenerationTo }) {
            TIntervalFilter filter(TabletId, { { { 2, GenerationFrom }, { GenerationTo, 101 } } });
            const auto group = filter.Exclude(Blob(2, generation));
            const bool inside = generation >= GenerationFrom && generation < GenerationTo;
            UNIT_ASSERT_VALUES_EQUAL(group.has_value(), inside);
            UNIT_ASSERT_VALUES_EQUAL(filter.Empty(), inside);
            if (inside) {
                UNIT_ASSERT_VALUES_EQUAL(*group, 101);
            }
        }
    }

    Y_UNIT_TEST(AdjacentIntervalsAndChannels) {
        TIntervalFilter filter(TabletId, { { { 2, 10 }, { 20, 101 } }, { { 2, 20 }, { 30, 102 } }, { { 3, 10 }, { 20, 103 } } });
        UNIT_ASSERT_VALUES_EQUAL(*filter.Exclude(Blob(2, 20)), 102);
        UNIT_ASSERT_VALUES_EQUAL(filter.GetIntervals().size(), 2);
        UNIT_ASSERT(filter.GetIntervals().contains({ 2, 10 }));
        UNIT_ASSERT(filter.GetIntervals().contains({ 3, 10 }));
        UNIT_ASSERT_VALUES_EQUAL(*filter.Exclude(Blob(3, 10)), 103);
        UNIT_ASSERT_VALUES_EQUAL(*filter.Exclude(Blob(2, 10)), 101);
        UNIT_ASSERT(filter.Empty());
    }

    Y_UNIT_TEST(ForeignTabletUnknownChannelAndGaps) {
        TIntervalFilter filter(TabletId, { { { 2, 10 }, { 20, 101 } }, { { 2, 30 }, { 40, 102 } }, { { 4, 10 }, { 20, 103 } } });
        UNIT_ASSERT(!filter.Exclude(Blob(2, 10, TabletId + 1)));
        UNIT_ASSERT(!filter.Exclude(Blob(1, 15)));
        UNIT_ASSERT(!filter.Exclude(Blob(3, 15)));
        UNIT_ASSERT(!filter.Exclude(Blob(5, 15)));
        UNIT_ASSERT(!filter.Exclude(Blob(2, 25)));
        UNIT_ASSERT_VALUES_EQUAL(filter.GetIntervals().size(), 3);
    }

    Y_UNIT_TEST(RepeatedReferencesAndEmptyCandidates) {
        TIntervalFilter filter(TabletId, { { { 2, 10 }, { 20, 0 } } });
        const auto group = filter.Exclude(Blob(2, 12));
        UNIT_ASSERT(group.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*group, 0);
        UNIT_ASSERT(!filter.Exclude(Blob(2, 12)));
        UNIT_ASSERT(!filter.Exclude(Blob(2, 15)));
        UNIT_ASSERT(filter.Empty());
        TIntervalFilter empty(TabletId, {});
        UNIT_ASSERT(!empty.Exclude(Blob(2, 12)));
    }
}

}   // namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals
