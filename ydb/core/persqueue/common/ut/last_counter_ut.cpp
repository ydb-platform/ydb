#include <ydb/core/persqueue/common/last_counter.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TLastCounterTest) {

Y_UNIT_TEST(TracksLastTwoDistinctValues) {
    TLastCounter<ui32> counter;
    const TInstant t1 = TInstant::Seconds(1);
    const TInstant t2 = TInstant::Seconds(2);
    const TInstant t3 = TInstant::Seconds(3);

    counter.Use(10, t1);
    UNIT_ASSERT_VALUES_EQUAL(counter.LastValue(), 10u);
    UNIT_ASSERT_VALUES_EQUAL(counter.Count(t1), 1u);

    counter.Use(10, t2);
    UNIT_ASSERT_VALUES_EQUAL(counter.LastValue(), 10u);
    UNIT_ASSERT_VALUES_EQUAL(counter.Count(t1), 1u);

    counter.Use(20, t3);
    UNIT_ASSERT_VALUES_EQUAL(counter.LastValue(), 20u);
    UNIT_ASSERT_VALUES_EQUAL(counter.Count(t1), 2u);
    UNIT_ASSERT_VALUES_EQUAL(counter.Count(t3), 1u);
}

Y_UNIT_TEST(SameFirstValueRotatesWhenFull) {
    TLastCounter<TString> counter;
    const TInstant t1 = TInstant::Seconds(1);
    const TInstant t2 = TInstant::Seconds(2);
    const TInstant t4 = TInstant::Seconds(4);

    counter.Use("a", t1);
    counter.Use("b", t2);
    counter.Use("a", t4);
    UNIT_ASSERT_VALUES_EQUAL(counter.LastValue(), "a");
    UNIT_ASSERT_VALUES_EQUAL(counter.Count(t2), 2u);
}

Y_UNIT_TEST(SameFirstValueDoesNotRotateWhenSecondHasSameTime) {
    TLastCounter<ui32> counter;
    const TInstant t1 = TInstant::Seconds(1);
    const TInstant t2 = TInstant::Seconds(2);

    counter.Use(1, t1);
    counter.Use(2, t2);
    counter.Use(1, t2);
    UNIT_ASSERT_VALUES_EQUAL(counter.LastValue(), 2u);
}

Y_UNIT_TEST(UpdatesSecondValueWhenFull) {
    TLastCounter<ui32> counter;
    const TInstant t1 = TInstant::Seconds(1);
    const TInstant t2 = TInstant::Seconds(2);
    const TInstant t3 = TInstant::Seconds(3);

    counter.Use(1, t1);
    counter.Use(2, t2);
    counter.Use(2, t3);
    UNIT_ASSERT_VALUES_EQUAL(counter.LastValue(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(counter.Count(t3), 1u);
}

Y_UNIT_TEST(ReplacesOldestOnNewValue) {
    TLastCounter<ui32> counter;
    const TInstant t1 = TInstant::Seconds(1);
    const TInstant t2 = TInstant::Seconds(2);
    const TInstant t3 = TInstant::Seconds(3);

    counter.Use(1, t1);
    counter.Use(2, t2);
    counter.Use(3, t3);
    UNIT_ASSERT_VALUES_EQUAL(counter.LastValue(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(counter.Count(t2), 2u);
    UNIT_ASSERT_VALUES_EQUAL(counter.Count(t1), 2u);
}

Y_UNIT_TEST(IgnoresNewValueWhenOldestIsNotOlderThanNow) {
    TLastCounter<ui32> counter;
    const TInstant t = TInstant::Seconds(5);

    counter.Use(1, t);
    counter.Use(2, t);
    counter.Use(3, t);
    UNIT_ASSERT_VALUES_EQUAL(counter.LastValue(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(counter.Count(t), 2u);
}

Y_UNIT_TEST(IgnoresStaleReuseOfFirstValue) {
    TLastCounter<ui32> counter;
    const TInstant t1 = TInstant::Seconds(3);
    const TInstant t0 = TInstant::Seconds(1);

    counter.Use(7, t1);
    counter.Use(7, t0);
    UNIT_ASSERT_VALUES_EQUAL(counter.LastValue(), 7u);
}

Y_UNIT_TEST(MergeCombinesCounters) {
    TLastCounter<ui32> lhs;
    TLastCounter<ui32> rhs;
    const TInstant t1 = TInstant::Seconds(1);
    const TInstant t2 = TInstant::Seconds(2);

    lhs.Use(1, t1);
    rhs.Use(2, t2);
    lhs.Merge(rhs);
    UNIT_ASSERT_VALUES_EQUAL(lhs.LastValue(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(lhs.Count(t1), 2u);
}

} // Y_UNIT_TEST_SUITE(TLastCounterTest)

} // namespace NKikimr::NPQ
