#include "query_stats.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NSysView {

Y_UNIT_TEST_SUITE(QueryStats) {
    Y_UNIT_TEST(Ranges) {
        auto test = [] (TMaybe<ui64> fromInstant, TMaybe<ui32> fromRank, bool inclusiveFrom,
            TMaybe<ui64> toInstant, TMaybe<ui32> toRank, bool inclusiveTo,
            ui64 fromBucketExpected, TMaybe<ui32> fromRankExpected,
            ui64 toBucketExpected, TMaybe<ui32> toRankExpected,
            bool isEmptyExpected)
        {
            TVector<TCell> from;
            if (fromInstant) {
                from.push_back(TCell::Make<ui64>(*fromInstant));
                if (fromRank) {
                    from.push_back(TCell::Make<ui32>(*fromRank));
                }
            }
            TVector<TCell> to;
            if (toInstant) {
                to.push_back(TCell::Make<ui64>(*toInstant));
                if (toRank) {
                    to.push_back(TCell::Make<ui32>(*toRank));
                }
            }
            TSerializedTableRange tableRange(from, inclusiveFrom, to, inclusiveTo);

            TQueryStatsBucketRange range(tableRange, TDuration::MicroSeconds(10));

            UNIT_ASSERT_VALUES_EQUAL(range.FromBucket, fromBucketExpected);
            UNIT_ASSERT_VALUES_EQUAL(range.FromRank, fromRankExpected);
            UNIT_ASSERT_VALUES_EQUAL(range.ToBucket, toBucketExpected);
            UNIT_ASSERT_VALUES_EQUAL(range.ToRank, toRankExpected);
            UNIT_ASSERT_VALUES_EQUAL(range.IsEmpty, isEmptyExpected);
        };

        auto maxUi64 = std::numeric_limits<ui64>::max();

        test({}, {}, false, {}, {}, false,
            0, {}, maxUi64, {}, false);
        test({}, {}, true, {}, {}, true,
            0, {}, maxUi64, {}, false);

        test(0, {}, false, 0, {}, false,
            0, {}, 0, {}, true);
        test(0, {}, true, 0, {}, true,
            0, {}, 0, {}, true);

        test(5, {}, false, 5, {}, false,
            0, {}, 0, {}, true);
        test(5, {}, true, 5, {}, true,
            0, {}, 0, {}, true);

        test(10, {}, false, 10, {}, false,
            1, {}, 0, {}, true);
        test(10, {}, true, 10, {}, true,
            0, {}, 0, {}, false);

        test(15, {}, false, 15, {}, false,
            1, {}, 0, {}, true);
        test(15, {}, true, 15, {}, true,
            1, {}, 0, {}, true);

        test(20, {}, false, 20, {}, false,
            2, {}, 0, {}, true);
        test(20, {}, true, 20, {}, true,
            1, {}, 1, {}, false);

        test(0, 3, false, 0, 7, false,
            0, {}, 0, {}, true);
        test(0, 3, true, 0, 3, true,
            0, {}, 0, {}, true);

        test(5, 3, false, 5, 7, false,
            0, {}, 0, {}, true);
        test(5, 3, true, 5, 7, true,
            0, {}, 0, {}, true);

        test(10, 3, false, 10, 7, false,
            0, 4, 0, 6, false);
        test(10, 3, true, 10, 7, true,
            0, 3, 0, 7, false);

        test(15, 3, false, 15, 7, false,
            1, {}, 0, {}, true);
        test(15, 3, true, 15, 7, true,
            1, {}, 0, {}, true);

        test(20, 3, false, 20, 7, false,
            1, 4, 1, 6, false);
        test(20, 3, true, 20, 7, true,
            1, 3, 1, 7, false);
    }
}

} // NSysView
} // NKikimr
