#include <ydb/core/persqueue/pqtablet/quota/quota_tracker.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/generic/ylimits.h>

namespace NKikimr::NPQ {

namespace {

ui64 DrainWhilePossible(TQuotaTracker& quota, TInstant ts, ui64 blobSize, ui64 maxBlobs = 1'000'000) {
    ui64 processed = 0;
    while (processed < maxBlobs && quota.CanExaust(ts)) {
        quota.Exaust(blobSize, ts);
        ++processed;
    }
    return processed;
}

} // namespace

Y_UNIT_TEST_SUITE(TQuotaTracker) {

Y_UNIT_TEST(TestSmallMessages) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    UNIT_ASSERT(quota.CanExaust(ts));

    quota.Exaust(2_MB - 1, ts);
    const ui64 blobSize = 500;
    ui64 processedBlobs = 0;

    for (ui32 i = 0; i < 100'000; ++i) { // 10 sec total
        if (quota.CanExaust(ts)) {
            quota.Exaust(blobSize, ts);
            ++processedBlobs;
        }
        ts += TDuration::MicroSeconds(100);
    }
    Cerr << "processed_blobs=" << processedBlobs << " quoted_time=" << quota.GetQuotedTime(ts) << Endl;
    UNIT_ASSERT_GE(processedBlobs, 41800);
    UNIT_ASSERT_LE(processedBlobs, 42000);
    UNIT_ASSERT_GE(quota.GetQuotedTime(ts), TDuration::MilliSeconds(9900));
    UNIT_ASSERT_LE(quota.GetQuotedTime(ts), TDuration::Seconds(10));
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

Y_UNIT_TEST(IdleRefillsUpToBurst) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    const ui64 blobSize = 1_KB;
    UNIT_ASSERT_VALUES_EQUAL(DrainWhilePossible(quota, ts, blobSize) * blobSize, 2_MB);

    ts += TDuration::Seconds(5);
    UNIT_ASSERT_VALUES_EQUAL(DrainWhilePossible(quota, ts, blobSize) * blobSize, 2_MB);
}

Y_UNIT_TEST(OneSecondRefillMatchesSpeed) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    const ui64 blobSize = 1_KB;
    UNIT_ASSERT_VALUES_EQUAL(DrainWhilePossible(quota, ts, blobSize) * blobSize, 2_MB);

    ui64 totalBytes = 0;
    for (ui32 i = 0; i < 1000; ++i) {
        ts += TDuration::MilliSeconds(1);
        totalBytes += DrainWhilePossible(quota, ts, blobSize) * blobSize;
    }
    UNIT_ASSERT_GE(totalBytes, 2_MB - blobSize);
    UNIT_ASSERT_LE(totalBytes, 2_MB + blobSize);
}

Y_UNIT_TEST(UpdateConfigGiftsBurst) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    DrainWhilePossible(quota, ts, 1_KB);
    UNIT_ASSERT(!quota.CanExaust(ts));

    UNIT_ASSERT(quota.UpdateConfigIfChanged(4_MB, 2_MB));
    UNIT_ASSERT_VALUES_EQUAL(DrainWhilePossible(quota, ts, 1_KB) * 1_KB, 4_MB);
}

Y_UNIT_TEST(LowSpeedAccumulatesAcrossWakeups) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(1, 1, ts);

    UNIT_ASSERT(quota.CanExaust(ts));
    quota.Exaust(1, ts);
    UNIT_ASSERT(!quota.CanExaust(ts));
    UNIT_ASSERT(!quota.CanExaust(ts + TDuration::MilliSeconds(50)));
    UNIT_ASSERT(quota.CanExaust(ts + TDuration::Seconds(1)));
}

Y_UNIT_TEST(HugeValuesDoNotOverflow) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(Max<ui64>(), Max<ui64>(), ts);

    UNIT_ASSERT(quota.CanExaust(ts));
    UNIT_ASSERT_VALUES_EQUAL(quota.GetTotalSpeed(), Max<ui64>());
    quota.Exaust(Max<ui64>(), ts);
    quota.Update(ts + TDuration::Days(365));
}

} //Y_UNIT_TEST_SUITE

} // namespace NKikimr::NPQ
