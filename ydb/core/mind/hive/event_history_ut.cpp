#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/hash_set.h>
#include "event_history.h"

using namespace NKikimr;
using namespace NHive;

Y_UNIT_TEST_SUITE(TLazyRingBufferTest) {
    template <size_t Capacity>
    using TBuffer = TLazyRingBuffer<ui32, Capacity>;

    template <size_t Capacity>
    std::vector<ui32> CollectNewestFirst(const TBuffer<Capacity>& buffer) {
        std::vector<ui32> result;
        buffer.ForEachNewestFirst([&result](const ui32& item) {
            result.push_back(item);
        });
        return result;
    }

    template <size_t Capacity>
    void CheckNewestFirst(const TBuffer<Capacity>& buffer, const std::vector<ui32>& expected) {
        UNIT_ASSERT_VALUES_EQUAL(buffer.Size(), expected.size());
        UNIT_ASSERT_VALUES_EQUAL(buffer.Empty(), expected.empty());
        for (size_t i = 0; i < expected.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(buffer.FromNewest(i), expected[i], "index " << i);
        }
        UNIT_ASSERT_VALUES_EQUAL(CollectNewestFirst(buffer), expected);
    }

    Y_UNIT_TEST(Empty) {
        TBuffer<4> buffer;
        CheckNewestFirst(buffer, {});
    }

    Y_UNIT_TEST(PartialFill) {
        TBuffer<4> buffer;
        buffer.Push(1);
        CheckNewestFirst(buffer, {1});
        buffer.Push(2);
        buffer.Push(3);
        CheckNewestFirst(buffer, {3, 2, 1});
    }

    Y_UNIT_TEST(ExactFill) {
        TBuffer<4> buffer;
        for (ui32 i = 1; i <= 4; ++i) {
            buffer.Push(ui32(i));
        }
        CheckNewestFirst(buffer, {4, 3, 2, 1});
    }

    Y_UNIT_TEST(WrapAround) {
        TBuffer<4> buffer;
        for (ui32 i = 1; i <= 5; ++i) {
            buffer.Push(ui32(i));
        }
        // the oldest item is evicted, the newest one is reported first
        CheckNewestFirst(buffer, {5, 4, 3, 2});
        for (ui32 i = 6; i <= 9; ++i) {
            buffer.Push(ui32(i));
        }
        // a full extra cycle keeps the order intact
        CheckNewestFirst(buffer, {9, 8, 7, 6});
        buffer.Push(10);
        CheckNewestFirst(buffer, {10, 9, 8, 7});
    }

    Y_UNIT_TEST(ManyWraps) {
        constexpr size_t capacity = 7;
        TBuffer<capacity> buffer;
        constexpr ui32 total = 1000;
        for (ui32 i = 0; i < total; ++i) {
            buffer.Push(ui32(i));
        }
        std::vector<ui32> expected;
        for (ui32 i = 0; i < capacity; ++i) {
            expected.push_back(total - 1 - i);
        }
        CheckNewestFirst(buffer, expected);
    }

    Y_UNIT_TEST(CapacityOne) {
        TBuffer<1> buffer;
        buffer.Push(1);
        CheckNewestFirst(buffer, {1});
        buffer.Push(2);
        CheckNewestFirst(buffer, {2});
    }

    Y_UNIT_TEST(MoveOnlyItems) {
        TLazyRingBuffer<std::unique_ptr<ui32>, 2> buffer;
        buffer.Push(std::make_unique<ui32>(1));
        buffer.Push(std::make_unique<ui32>(2));
        buffer.Push(std::make_unique<ui32>(3));
        UNIT_ASSERT_VALUES_EQUAL(buffer.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(*buffer.FromNewest(0), 3);
        UNIT_ASSERT_VALUES_EQUAL(*buffer.FromNewest(1), 2);
    }
}

Y_UNIT_TEST_SUITE(THiveEventTest) {
    Y_UNIT_TEST(PackTypeAndReason) {
        // every (type, reason) pair must survive the round trip through the packed word
        const TInstant timestamp = TInstant::ParseIso8601("2026-10-06T12:34:56.789Z");
        for (ui8 t = 0; t <= static_cast<ui8>(EHiveEventType::AvailabilityChanged); ++t) {
            for (ui8 r = 0; r <= static_cast<ui8>(EHiveEventReason::LoadedFromDatabase); ++r) {
                const auto type = static_cast<EHiveEventType>(t);
                const auto reason = static_cast<EHiveEventReason>(r);
                THiveEvent event(timestamp, type, reason, "details");
                UNIT_ASSERT_EQUAL_C(event.GetType(), type, "type " << int(t) << " reason " << int(r));
                UNIT_ASSERT_EQUAL_C(event.GetReason(), reason, "type " << int(t) << " reason " << int(r));
                UNIT_ASSERT_VALUES_EQUAL(event.Details, "details");
                UNIT_ASSERT_VALUES_EQUAL(event.GetTimestamp(), timestamp);
            }
        }
    }

    Y_UNIT_TEST(PackTimestamp) {
        // millisecond precision is kept, finer precision is dropped, fields do not overlap
        const auto type = EHiveEventType::Killed;
        const auto reason = EHiveEventReason::PingUndelivered;
        for (TInstant timestamp : {TInstant::Zero(), TInstant::MilliSeconds(1), TInstant::MicroSeconds(1999),
                                   TInstant::ParseIso8601("2026-10-06T00:00:00Z"), TInstant::ParseIso8601("9999-12-31T23:59:59.999Z")}) {
            THiveEvent event(timestamp, type, reason, {});
            UNIT_ASSERT_VALUES_EQUAL(event.GetTimestamp(), TInstant::MilliSeconds(timestamp.MilliSeconds()));
            UNIT_ASSERT_EQUAL(event.GetType(), type);
            UNIT_ASSERT_EQUAL(event.GetReason(), reason);
            UNIT_ASSERT(event.Details.empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(sizeof(THiveEvent), 16);
    }

    Y_UNIT_TEST(NamesAreDistinct) {
        THashSet<TStringBuf> names;
        for (ui8 r = 0; r <= static_cast<ui8>(EHiveEventReason::LoadedFromDatabase); ++r) {
            UNIT_ASSERT(names.insert(EHiveEventReasonName(static_cast<EHiveEventReason>(r))).second);
        }
        names.clear();
        for (ui8 t = 0; t <= static_cast<ui8>(EHiveEventType::AvailabilityChanged); ++t) {
            UNIT_ASSERT(names.insert(EHiveEventTypeName(static_cast<EHiveEventType>(t))).second);
        }
    }
}
