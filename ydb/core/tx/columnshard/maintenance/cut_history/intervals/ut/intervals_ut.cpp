#include <ydb/core/tx/columnshard/maintenance/cut_history/intervals/intervals.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals {
namespace {

constexpr ui64 TabletId = 123;
constexpr ui32 Channel = 2;
constexpr ui32 OtherChannel = 4;
constexpr ui32 Group = 101;
constexpr ui32 OtherGroup = 102;
constexpr ui32 GenerationStep = 10;

constexpr ui32 Generation(ui32 index) {
    return (index + 1) * GenerationStep;
}

TLogoBlobID MakeBlobID(ui32 channel, ui32 generation, ui64 tabletId = TabletId) {
    return TLogoBlobID(tabletId, generation, 1, channel, 100, 0);
}

THistoryIntervals::TIntervals MakeIntervals() {
    return { { { Channel, Generation(0) }, { Generation(1), Group } }, { { Channel, Generation(1) }, { Generation(2), OtherGroup } },
        { { Channel, Generation(2) }, { Generation(3), Group } }, { { OtherChannel, Generation(0) }, { Generation(1), Group } } };
}

class TFixture {
public:
    THistoryIntervals Intervals{ TabletId, MakeIntervals() };
    ui32 Mismatches = 0;

    void Exclude(ui32 generation, ui32 group) {
        Intervals.Exclude(MakeBlobID(Channel, generation), group, [this] {
            ++Mismatches;
        });
    }

    bool Contains(ui32 channel, ui32 generation) const {
        return Intervals.GetIntervals().contains({ channel, generation });
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(THistoryIntervalsTest) {
    Y_UNIT_TEST(HalfOpenGenerationRange) {
        constexpr ui32 GenerationFrom = Generation(0);
        constexpr ui32 GenerationTo = Generation(1);
        for (ui32 generation : { GenerationFrom - 1, GenerationFrom, GenerationTo - 1, GenerationTo }) {
            THistoryIntervals intervals(TabletId, { { { Channel, GenerationFrom }, { GenerationTo, Group } } });
            intervals.Exclude(MakeBlobID(Channel, generation));
            UNIT_ASSERT_VALUES_EQUAL(intervals.Empty(), generation >= GenerationFrom && generation < GenerationTo);
        }
    }

    Y_UNIT_TEST(AdjacentIntervalsWithRepeatedGroups) {
        TFixture f;
        f.Exclude(Generation(1), OtherGroup);
        f.Exclude(Generation(2), Group);
        UNIT_ASSERT_VALUES_EQUAL(f.Intervals.GetIntervals().size(), 2u);
        UNIT_ASSERT(f.Contains(Channel, Generation(0)));
        UNIT_ASSERT(f.Contains(OtherChannel, Generation(0)));
        UNIT_ASSERT_VALUES_EQUAL(f.Mismatches, 0u);
    }

    Y_UNIT_TEST(GroupMismatchExcludesIntervalOnce) {
        TFixture f;
        f.Exclude(Generation(1), Group);
        f.Exclude(Generation(1), Group);
        UNIT_ASSERT_VALUES_EQUAL(f.Mismatches, 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.Intervals.GetIntervals().size(), 3u);
        UNIT_ASSERT(!f.Contains(Channel, Generation(1)));
    }

    Y_UNIT_TEST(ForeignTabletKeepsIntervals) {
        TFixture f;
        f.Intervals.Exclude(MakeBlobID(Channel, Generation(0), TabletId + 1));
        UNIT_ASSERT_VALUES_EQUAL(f.Intervals.GetIntervals().size(), 4u);
    }

    Y_UNIT_TEST(MissingChannelKeepsNeighboringIntervals) {
        TFixture f;
        f.Intervals.Exclude(MakeBlobID(Channel + 1, Generation(0)));
        UNIT_ASSERT_VALUES_EQUAL(f.Intervals.GetIntervals().size(), 4u);
    }

    Y_UNIT_TEST(GenerationGapKeepsNeighboringIntervals) {
        auto intervals = MakeIntervals();
        intervals.erase({ Channel, Generation(1) });
        THistoryIntervals history(TabletId, std::move(intervals));
        history.Exclude(MakeBlobID(Channel, Generation(1) + 1));
        UNIT_ASSERT_VALUES_EQUAL(history.GetIntervals().size(), 3u);
    }
}

}   // namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals
