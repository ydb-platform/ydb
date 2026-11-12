#include "download_limiter.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/thread/pool.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TDownloadThrottlingTests) {
Y_UNIT_TEST(ZeroLimitUsesMaximumQuota) {
    auto limiter = TDownloadLimiter(NSize::TSize(0_B));
    UNIT_ASSERT_VALUES_EQUAL(limiter.GetQuota(0), 0);
    UNIT_ASSERT_VALUES_EQUAL(limiter.GetQuota(1), 1);
    UNIT_ASSERT_VALUES_EQUAL(limiter.GetQuota(Max<size_t>()), Max<size_t>() - 1);
    UNIT_ASSERT_VALUES_EQUAL(limiter.GetQuota(0), 0);
    UNIT_ASSERT_VALUES_EQUAL(limiter.GetQuota(Max<size_t>()), Max<size_t>());
}

Y_UNIT_TEST(InitialQuotaEqualsConfiguredBandwidth) {
    const TVector<NSize::TSize> limits = {
        NSize::TSize(1_B), NSize::TSize(9_B), NSize::TSize(10_B),
        NSize::TSize(100_B), NSize::TSize(1_MB), NSize::TSize(Max<ui64>())};
    for (const auto& rate : limits) {
        auto limiter = TDownloadLimiter(rate);
        UNIT_ASSERT_VALUES_EQUAL(limiter.GetQuota(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(limiter.GetQuota(Max<ui64>()), rate.GetValue());
    }
}

Y_UNIT_TEST(ConcurrentReservationsShareQuota) {
    constexpr size_t RequestCount = 8;
    auto limiter = TDownloadLimiter(NSize::TSize(100_B));
    TSimpleThreadPool threadPool;
    threadPool.Start(RequestCount);
    TVector<NThreading::TFuture<ui64>> reservations(Reserve(RequestCount));
    for (size_t index = 0; index < RequestCount; ++index) {
        reservations.push_back(NThreading::Async([limiter] { return limiter.GetQuota(1); }, threadPool));
    }
    for (const auto& reservation : reservations) {
        UNIT_ASSERT_VALUES_EQUAL(reservation.GetValueSync(), 1);
    }
    UNIT_ASSERT_VALUES_EQUAL(limiter.GetQuota(100), 92);
}
} // Y_UNIT_TEST_SUITE(TDownloadThrottlingTests)

} // namespace NYql
