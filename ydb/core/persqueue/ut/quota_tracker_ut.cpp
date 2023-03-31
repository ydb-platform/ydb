#include "quota_tracker.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TQuotaTracker) {

Y_UNIT_TEST(TestSmallMessages) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    UNIT_ASSERT(quota.CanExaust(ts));

    quota.Exaust(2_MB - 1, ts);
    ui64 blobSize = 500;
    ui64 processedBlobs = 0;

    for (ui32 i = 0; i < 100'000; ++i) { // 10 sec total
        if (quota.CanExaust(ts)) {
            quota.Exaust(blobSize, ts);
            ++processedBlobs;
        }
        ts += TDuration::MicroSeconds(100);
    }
    Cerr << "processed_blobs=" << processedBlobs << " quoted_time=" << quota.GetQuotedTime(ts) << Endl;
    UNIT_ASSERT_EQUAL(processedBlobs, 41800);
    UNIT_ASSERT_EQUAL(quota.GetQuotedTime(ts), TDuration::MilliSeconds(9980));
}

Y_UNIT_TEST(TestBigMessages) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    UNIT_ASSERT(quota.CanExaust(ts));

    auto CannotExaustAfter = [&](TDuration diff) {
        ts += diff;
        UNIT_ASSERT_C(!quota.CanExaust(ts), TStringBuilder() << "at " << ts);
    };

    quota.Exaust(10_MB, ts);
    CannotExaustAfter(TDuration::Zero());
    CannotExaustAfter(TDuration::Seconds(4));

    ts += TDuration::MilliSeconds(1);
    UNIT_ASSERT(quota.CanExaust(ts));
}

} //Y_UNIT_TEST_SUITE

} // namespace NKikimr::NPQ
