#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/hash_set.h>
#include "event_history.h"

using namespace NKikimr;
using namespace NHive;

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
