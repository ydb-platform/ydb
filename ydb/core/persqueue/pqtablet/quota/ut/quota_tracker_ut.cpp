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

ui64 RunForDuration(TQuotaTracker& quota, TInstant& ts, TDuration duration, ui64 blobSize, TDuration step = TDuration::MicroSeconds(100)) {
    const TInstant end = ts + duration;
    ui64 processed = 0;
    while (ts < end) {
        if (quota.CanExaust(ts)) {
            quota.Exaust(blobSize, ts);
            ++processed;
        }
        ts += step;
    }
    return processed;
}

} // namespace

Y_UNIT_TEST_SUITE(TQuotaTracker) {

Y_UNIT_TEST(TestSmallMessages) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    UNIT_ASSERT(quota.CanExaust(ts));

    const ui64 blobSize = 500;
    const ui64 processedBlobs = RunForDuration(quota, ts, TDuration::Seconds(10), blobSize);

    const ui64 totalBytes = processedBlobs * blobSize;
    const ui64 tickBytes = 2_MB / 20;
    UNIT_ASSERT_GE(totalBytes, 10 * 2_MB - 2_MB);
    UNIT_ASSERT_LE(totalBytes, 10 * 2_MB + tickBytes + blobSize);
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

    ts += TDuration::Seconds(1);
    UNIT_ASSERT(quota.CanExaust(ts));
}

Y_UNIT_TEST(BurstEqualsSpeedDoesNotDoubleOverOneSecond) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    const ui64 blobSize = 1_KB;
    const ui64 processedBlobs = RunForDuration(quota, ts, TDuration::Seconds(1), blobSize);
    const ui64 totalBytes = processedBlobs * blobSize;

    UNIT_ASSERT_LE(totalBytes, 2_MB + 2_MB / 20 + blobSize);
    UNIT_ASSERT_GE(totalBytes, 2_MB - blobSize);
}

Y_UNIT_TEST(IdleDoesNotRefillBeyondTick) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    const ui64 blobSize = 1_KB;
    DrainWhilePossible(quota, ts, blobSize);

    ts += TDuration::Seconds(5);
    const ui64 processedBlobs = DrainWhilePossible(quota, ts, blobSize);
    const ui64 totalBytes = processedBlobs * blobSize;

    UNIT_ASSERT_LE(totalBytes, 2_MB / 20 + blobSize);
    UNIT_ASSERT_GT(totalBytes, 0);
}

Y_UNIT_TEST(BurstGreaterThanSpeedAllowsExtra) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(4_MB, 2_MB, ts);

    const ui64 blobSize = 1_KB;
    const ui64 firstSecond = RunForDuration(quota, ts, TDuration::Seconds(1), blobSize) * blobSize;
    UNIT_ASSERT_LE(firstSecond, 4_MB + 2_MB / 20 + blobSize);
    UNIT_ASSERT_GE(firstSecond, 4_MB - blobSize);

    DrainWhilePossible(quota, ts, blobSize);
    ts += TDuration::Seconds(5);
    const ui64 afterIdle = DrainWhilePossible(quota, ts, blobSize) * blobSize;
    UNIT_ASSERT_LE(afterIdle, 2_MB + 2_MB / 20 + blobSize);
    UNIT_ASSERT_GE(afterIdle, 2_MB);
}

Y_UNIT_TEST(BurstBelowSpeedUsesTickOnly) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(1_KB, 2_MB, ts);

    const ui64 blobSize = 100;
    const ui64 immediate = DrainWhilePossible(quota, ts, blobSize) * blobSize;
    UNIT_ASSERT_LE(immediate, 2_MB / 20 + blobSize);
}

Y_UNIT_TEST(UpdateConfigDoesNotGiftBurst) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    const ui64 blobSize = 1_KB;
    DrainWhilePossible(quota, ts, blobSize);
    UNIT_ASSERT(!quota.CanExaust(ts));

    UNIT_ASSERT(quota.UpdateConfigIfChanged(4_MB, 2_MB, ts));
    UNIT_ASSERT(!quota.CanExaust(ts));
}

Y_UNIT_TEST(UpdateConfigUnchangedReturnsFalse) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    UNIT_ASSERT(!quota.UpdateConfigIfChanged(2_MB, 2_MB, ts));
    UNIT_ASSERT_VALUES_EQUAL(quota.GetTotalSpeed(), 2_MB);
}

Y_UNIT_TEST(UpdateConfigClampsAvailableToNewCap) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(4_MB, 2_MB, ts);

    UNIT_ASSERT(quota.UpdateConfigIfChanged(2_MB, 2_MB, ts));

    const ui64 blobSize = 1_KB;
    const ui64 immediate = DrainWhilePossible(quota, ts, blobSize) * blobSize;
    UNIT_ASSERT_LE(immediate, 2_MB / 20 + blobSize);
}

Y_UNIT_TEST(UpdateConfigChangesSpeed) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    DrainWhilePossible(quota, ts, 1_KB);
    UNIT_ASSERT(quota.UpdateConfigIfChanged(1_MB, 1_MB, ts + TDuration::MilliSeconds(1)));
    UNIT_ASSERT_VALUES_EQUAL(quota.GetTotalSpeed(), 1_MB);
}

Y_UNIT_TEST(ZeroSpeedNeverAllowsExhaust) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(0, 0, ts);

    UNIT_ASSERT(!quota.CanExaust(ts));
    UNIT_ASSERT(!quota.CanExaust(ts + TDuration::Seconds(10)));
    UNIT_ASSERT_VALUES_EQUAL(quota.GetTotalSpeed(), 0);
}

Y_UNIT_TEST(LowSpeedKeepsAtLeastOneUnit) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(1, 1, ts);

    UNIT_ASSERT(quota.CanExaust(ts));
    quota.Exaust(1, ts);
    UNIT_ASSERT(!quota.CanExaust(ts));
    UNIT_ASSERT(quota.CanExaust(ts + TDuration::Seconds(1)));
}

Y_UNIT_TEST(QuotedTimeAccumulatesWhileThrottled) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    UNIT_ASSERT_VALUES_EQUAL(quota.GetQuotedTime(ts), TDuration::Zero());
    quota.Exaust(2_MB, ts);
    ts += TDuration::Seconds(1);
    UNIT_ASSERT_VALUES_EQUAL(quota.GetQuotedTime(ts), TDuration::Seconds(1));
}

Y_UNIT_TEST(PastTimestampDoesNotRefill) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    DrainWhilePossible(quota, ts, 1_KB);
    quota.Update(ts - TDuration::Seconds(1));
    UNIT_ASSERT(!quota.CanExaust(ts));
}

Y_UNIT_TEST(SameMillisecondUpdateIsNoop) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    DrainWhilePossible(quota, ts, 1_KB);
    quota.Update(ts + TDuration::MicroSeconds(500));
    UNIT_ASSERT(!quota.CanExaust(ts + TDuration::MicroSeconds(500)));
}

Y_UNIT_TEST(HugeValuesDoNotOverflow) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(Max<ui64>(), Max<ui64>(), ts);

    UNIT_ASSERT(quota.CanExaust(ts));
    UNIT_ASSERT_VALUES_EQUAL(quota.GetTotalSpeed(), Max<ui64>());
    quota.Exaust(Max<ui64>(), ts);
    quota.Update(ts + TDuration::Days(365));
    UNIT_ASSERT(quota.CanExaust(ts + TDuration::Days(365)));
}

Y_UNIT_TEST(HugeBurstOverSmallSpeed) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(Max<ui64>(), 1, ts);

    UNIT_ASSERT(quota.CanExaust(ts));
    quota.Update(ts + TDuration::MilliSeconds(1));
    quota.Exaust(1, ts + TDuration::MilliSeconds(1));
}

Y_UNIT_TEST(HugeExhaustGoesNegativeThenRecovers) {
    TInstant ts = TInstant::MilliSeconds(123456789);
    TQuotaTracker quota(2_MB, 2_MB, ts);

    quota.Exaust(Max<ui64>(), ts);
    UNIT_ASSERT(!quota.CanExaust(ts));
    UNIT_ASSERT(!quota.CanExaust(ts + TDuration::Seconds(1)));
}

} //Y_UNIT_TEST_SUITE

} // namespace NKikimr::NPQ
