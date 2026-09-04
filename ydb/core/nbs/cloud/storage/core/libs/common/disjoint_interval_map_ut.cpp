#include "disjoint_interval_map.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TString Print(const TDisjointIntervalMap<TKey, TValue>& map)
{
    TStringBuilder sb;
    sb << "[";

    bool first = true;
    for (const auto& [_, value]: map) {
        if (first) {
            first = false;
        } else {
            sb << ", ";
        }
        sb << "(" << value.Begin << ", " << value.End << "): " << value.Value;
    }
    sb << "]";

    return sb;
}

template <class TKey, class TValue>
TString
PrintOverlapping(TDisjointIntervalMap<TKey, TValue>& map, TKey begin, TKey end)
{
    TDisjointIntervalMap<TKey, TValue> tmp;

    map.VisitOverlapping(
        begin,
        end,
        [&](auto it)
        { tmp.Add(it->second.Begin, it->second.End, it->second.Value); });

    return Print(tmp);
}

template <class TKey>
auto Find(const auto& map, TKey begin, TKey end)
{
    for (auto it = map.begin(); it != map.end(); ++it) {
        if (it->second.Begin == begin && it->second.End == end) {
            return it;
        }
    }
    return map.end();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

using TMap = TDisjointIntervalMap<ui64, TString>;
using TMapWithStats = TDisjointIntervalMapWithStats<ui64, TString>;

Y_UNIT_TEST_SUITE(TDisjointIntervalMapTest)
{
    Y_UNIT_TEST(SimpleAdd)
    {
        TMap map;
        map.Add(3, 5, "petya");
        map.Add(1, 3, "vasya");

        UNIT_ASSERT_VALUES_EQUAL("[(1, 3): vasya, (3, 5): petya]", Print(map));
    }

    Y_UNIT_TEST(VisitOverlapping)
    {
        TMap map;
        map.Add(3, 5, "petya");
        map.Add(1, 3, "vasya");
        map.Add(9, 12, "ben");
        map.Add(6, 7, "igor");

        for (ui64 l = 0; l <= 14; l++) {
            for (ui64 r = l + 1; r <= 15; r++) {
                TMap expected;
                if (l < 3 && r > 1) {
                    expected.Add(1, 3, "vasya");
                }
                if (l < 5 && r > 3) {
                    expected.Add(3, 5, "petya");
                }
                if (l < 7 && r > 6) {
                    expected.Add(6, 7, "igor");
                }
                if (l < 12 && r > 9) {
                    expected.Add(9, 12, "ben");
                }

                UNIT_ASSERT_VALUES_EQUAL_C(
                    Print(expected),
                    PrintOverlapping(map, l, r),
                    "Failed for l=" << l << ", r=" << r);
            }
        }
    }

    Y_UNIT_TEST(DeleteWhileVisitOverlapping)
    {
        TMap map;
        map.Add(3, 5, "petya");
        map.Add(1, 3, "vasya");
        map.Add(9, 12, "ben");
        map.Add(6, 7, "igor");

        map.VisitOverlapping(
            4UL,
            13UL,
            [&](auto it)
            {
                if (it->second.Value == "igor") {
                    map.Remove(it);
                }
            });

        UNIT_ASSERT_VALUES_EQUAL(
            "[(1, 3): vasya, (3, 5): petya, (9, 12): ben]",
            Print(map));
    }

    Y_UNIT_TEST(ShouldCalculateStats)
    {
        TMapWithStats map;

        UNIT_ASSERT_VALUES_EQUAL(0, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(0, map.GetIntervalSum());

        map.Add(1, 3, "a");

        UNIT_ASSERT_VALUES_EQUAL(1, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(2, map.GetIntervalSum());

        map.Add(7, 10, "b");

        UNIT_ASSERT_VALUES_EQUAL(2, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(5, map.GetIntervalSum());

        map.Add(0, 1, "c");

        UNIT_ASSERT_VALUES_EQUAL(2, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(6, map.GetIntervalSum());

        map.Add(3, 4, "d");

        UNIT_ASSERT_VALUES_EQUAL(2, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(7, map.GetIntervalSum());

        map.Add(6, 7, "e");

        UNIT_ASSERT_VALUES_EQUAL(2, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(8, map.GetIntervalSum());

        map.Add(10, 11, "f");

        UNIT_ASSERT_VALUES_EQUAL(2, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(9, map.GetIntervalSum());

        map.Add(4, 6, "g");

        UNIT_ASSERT_VALUES_EQUAL(1, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(11, map.GetIntervalSum());

        map.Remove(Find(map, 1UL, 3UL));

        UNIT_ASSERT_VALUES_EQUAL(2, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(9, map.GetIntervalSum());

        map.Remove(Find(map, 3UL, 4UL));

        UNIT_ASSERT_VALUES_EQUAL(2, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(8, map.GetIntervalSum());

        map.Remove(Find(map, 0UL, 1UL));

        UNIT_ASSERT_VALUES_EQUAL(1, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(7, map.GetIntervalSum());

        map.Remove(Find(map, 10UL, 11UL));

        UNIT_ASSERT_VALUES_EQUAL(1, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(6, map.GetIntervalSum());

        map.Remove(Find(map, 6UL, 7UL));

        UNIT_ASSERT_VALUES_EQUAL(2, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(5, map.GetIntervalSum());

        map.Remove(Find(map, 7UL, 10UL));

        UNIT_ASSERT_VALUES_EQUAL(1, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(2, map.GetIntervalSum());

        map.Remove(Find(map, 4UL, 6UL));

        UNIT_ASSERT_VALUES_EQUAL(0, map.GetContiguousIntervalCount());
        UNIT_ASSERT_VALUES_EQUAL(0, map.GetIntervalSum());
    }
}

}   // namespace NYdb::NBS
