#include <ydb/core/persqueue/dread_cache_service/deadline_map.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NPQ;

namespace {

struct TTestValue {
    int Payload = 0;
    TInstant Deadline;
};

using TTestDeadlineMap = TDeadlineMap<int, TTestValue>;

constexpr TDuration Ttl = TTestDeadlineMap::DefaultTtl;

} // namespace

Y_UNIT_TEST_SUITE(TDeadlineMapTest) {

Y_UNIT_TEST(InsertSetsDeadlineAndRejectsDuplicate) {
    TTestDeadlineMap deadlineMap;
    const TInstant now = TInstant::Seconds(1000);

    UNIT_ASSERT(deadlineMap.Insert(1, TTestValue{.Payload = 7}, now));
    UNIT_ASSERT_VALUES_EQUAL(deadlineMap.Find(1)->Payload, 7);
    UNIT_ASSERT_VALUES_EQUAL(deadlineMap.Find(1)->Deadline, now + Ttl);
    UNIT_ASSERT_VALUES_EQUAL(deadlineMap.Size(), 1);

    UNIT_ASSERT(!deadlineMap.Insert(1, TTestValue{.Payload = 8}, now));
    UNIT_ASSERT_VALUES_EQUAL(deadlineMap.Find(1)->Payload, 7);
}

Y_UNIT_TEST(FindOrInsertReturnsExistingWithoutOverwrite) {
    TTestDeadlineMap deadlineMap;
    const TInstant now = TInstant::Seconds(1000);

    TTestValue& first = deadlineMap.FindOrInsert(1, TTestValue{.Payload = 7}, now);
    UNIT_ASSERT_VALUES_EQUAL(first.Payload, 7);
    UNIT_ASSERT_VALUES_EQUAL(first.Deadline, now + Ttl);

    TTestValue& second = deadlineMap.FindOrInsert(
        1, TTestValue{.Payload = 8}, now + TDuration::Seconds(10));
    UNIT_ASSERT_VALUES_EQUAL(second.Payload, 7);
    UNIT_ASSERT_VALUES_EQUAL(second.Deadline, now + Ttl);
    UNIT_ASSERT_VALUES_EQUAL(deadlineMap.Size(), 1);
}

Y_UNIT_TEST(EraseThenExpireSkipsStaleQueueEntry) {
    TTestDeadlineMap deadlineMap;
    const TInstant now = TInstant::Seconds(1000);

    UNIT_ASSERT(deadlineMap.Insert(1, TTestValue{.Payload = 1}, now));
    UNIT_ASSERT(deadlineMap.Erase(1));
    UNIT_ASSERT(deadlineMap.Empty());

    UNIT_ASSERT_VALUES_EQUAL(deadlineMap.Expire(now + Ttl), 0);
    UNIT_ASSERT(deadlineMap.Empty());
}

Y_UNIT_TEST(ExpireRemovesWhenDeadlinesMatch) {
    TTestDeadlineMap deadlineMap;
    const TInstant now = TInstant::Seconds(1000);

    UNIT_ASSERT(deadlineMap.Insert(1, TTestValue{.Payload = 1}, now));
    UNIT_ASSERT(deadlineMap.Insert(2, TTestValue{.Payload = 2}, now + TDuration::Seconds(10)));

    UNIT_ASSERT_VALUES_EQUAL(deadlineMap.Expire(now + Ttl - TDuration::Seconds(1)), 0);
    UNIT_ASSERT_VALUES_EQUAL(deadlineMap.Size(), 2);

    UNIT_ASSERT_VALUES_EQUAL(deadlineMap.Expire(now + Ttl), 1);
    UNIT_ASSERT(!deadlineMap.Find(1));
    UNIT_ASSERT(deadlineMap.Find(2));

    UNIT_ASSERT_VALUES_EQUAL(deadlineMap.Expire(now + TDuration::Seconds(10) + Ttl), 1);
    UNIT_ASSERT(deadlineMap.Empty());
}

Y_UNIT_TEST(TouchDeadlineKeepsEntryPastOldDeadline) {
    TTestDeadlineMap deadlineMap;
    const TInstant now = TInstant::Seconds(1000);

    UNIT_ASSERT(deadlineMap.Insert(1, TTestValue{.Payload = 1}, now));
    const TInstant firstDeadline = deadlineMap.Find(1)->Deadline;

    const TInstant later = now + TDuration::Seconds(30);
    UNIT_ASSERT(deadlineMap.TouchDeadline(1, later));
    UNIT_ASSERT_VALUES_EQUAL(deadlineMap.Find(1)->Deadline, later + Ttl);
    UNIT_ASSERT(deadlineMap.Find(1)->Deadline > firstDeadline);

    // Old queue entry is stale: map deadline no longer matches.
    UNIT_ASSERT_VALUES_EQUAL(deadlineMap.Expire(firstDeadline), 0);
    UNIT_ASSERT(deadlineMap.Find(1));

    UNIT_ASSERT_VALUES_EQUAL(deadlineMap.Expire(later + Ttl), 1);
    UNIT_ASSERT(!deadlineMap.Find(1));
}

Y_UNIT_TEST(TouchDeadlineDoesNotMoveBackward) {
    TTestDeadlineMap deadlineMap;
    const TInstant now = TInstant::Seconds(1000);

    UNIT_ASSERT(deadlineMap.Insert(1, TTestValue{}, now));
    const TInstant deadline = deadlineMap.Find(1)->Deadline;

    UNIT_ASSERT(deadlineMap.TouchDeadline(1, now - TDuration::Seconds(10)));
    UNIT_ASSERT_VALUES_EQUAL(deadlineMap.Find(1)->Deadline, deadline);
}

Y_UNIT_TEST(TouchDeadlineMissingKey) {
    TTestDeadlineMap deadlineMap;
    UNIT_ASSERT(!deadlineMap.TouchDeadline(1, TInstant::Seconds(1)));
}

} // Y_UNIT_TEST_SUITE(TDeadlineMapTest)
