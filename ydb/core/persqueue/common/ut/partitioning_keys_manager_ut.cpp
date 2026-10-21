#include <ydb/core/persqueue/common/partitioning_keys_manager.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TPartitioningKeysManagerCommonTest) {

Y_UNIT_TEST(MedianEmptyAndSingle) {
    TPartitioningKeysManager m(1, TDuration::Hours(1));
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(m.GetMedianKey()), 0u);

    m.Add(42, 16);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(m.GetMedianKey()), 42u);
}

Y_UNIT_TEST(ExplicitTimeAndExpiry) {
    TPartitioningKeysManager m(2, TDuration::Seconds(4));
    const TInstant t0 = TInstant::Now();
    m.Add(10, 16, t0);
    m.Add(20, 16, t0 + TDuration::Seconds(1));
    UNIT_ASSERT(m.MoreThanOneKey(t0));
    UNIT_ASSERT(!m.MoreThanOneKey(t0 + TDuration::Hours(1)));

    TPartitioningKeysManager expired(1, TDuration::Seconds(1));
    expired.Add(7, 16, t0 - TDuration::Hours(1));
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(expired.GetMedianKey()), 0u);
}

Y_UNIT_TEST(MergeIntoEmpty) {
    TPartitioningKeysManager lhs(2, TDuration::Seconds(4));
    TPartitioningKeysManager rhs(2, TDuration::Seconds(4));
    const TInstant t0 = TInstant::Now();
    rhs.Add(11, 16, t0);
    lhs.Merge(rhs);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(lhs.GetMedianKey()), 11u);
}

Y_UNIT_TEST(MergeSkipsOlderLocalSketches) {
    const TDuration window = TDuration::Seconds(4);
    TPartitioningKeysManager lhs(2, window);
    TPartitioningKeysManager rhs(2, window);
    const TInstant t0 = TInstant::Now();
    lhs.Add(1, 16, t0);
    UNIT_ASSERT(!lhs.MoreThanOneKey(t0));
    rhs.Add(50, 16, t0 + TDuration::Seconds(5));
    lhs.Merge(rhs);
    UNIT_ASSERT(lhs.MoreThanOneKey(t0));
    UNIT_ASSERT(lhs.GetMedianKey() != 0);
}

Y_UNIT_TEST(MergeAlignsToExistingSketch) {
    const TDuration window = TDuration::Seconds(4);
    TPartitioningKeysManager lhs(2, window);
    TPartitioningKeysManager rhs(2, window);
    const TInstant t0 = TInstant::Now();
    lhs.Add(1, 16, t0);
    UNIT_ASSERT(!lhs.MoreThanOneKey(t0));
    rhs.Add(3, 16, t0);
    lhs.Merge(rhs);
    UNIT_ASSERT(lhs.MoreThanOneKey(t0));
}

Y_UNIT_TEST(CreatesNewSketchWhenWindowAdvances) {
    TPartitioningKeysManager m(2, TDuration::Seconds(4));
    const TInstant t0 = TInstant::Now();
    m.Add(1, 16, t0);
    m.Add(2, 16, t0 + TDuration::Seconds(2));
    m.Add(3, 16, t0 + TDuration::Seconds(3));
    UNIT_ASSERT(m.GetMedianKey() != 0);
}

} // Y_UNIT_TEST_SUITE(TPartitioningKeysManagerCommonTest)

} // namespace NKikimr::NPQ
