#include <ydb/library/shop/estimator.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NShop;

Y_UNIT_TEST_SUITE(ShopEstimator) {
    Y_UNIT_TEST(StartLwtrace) {
        NLWTrace::StartLwtraceFromEnv();
    }

    Y_UNIT_TEST(MovingAverage) {
        TMovingAverageEstimator<1, 2> est(1.0);
        UNIT_ASSERT_EQUAL(est.GetAverage(), 1.0);
        est.Update(2.0);
        UNIT_ASSERT(fabs(est.GetAverage() - 1.5) < 1e-5);
        for (int i = 1; i < 100; i++) {
            est.Update(2.0);
        }
        UNIT_ASSERT(fabs(est.GetAverage() - 2.0) < 1e-5);
    }

    Y_UNIT_TEST(MovingSlr) {
        TMovingSlrEstimator<1, 2> est(1.0, 1.0, 2.0, 2.0);

        // check that initial 2-point guess is a straight line
        UNIT_ASSERT(fabs(est.GetEstimation(3.0) - 3.0) < 1e-5);
        UNIT_ASSERT(fabs(est.GetEstimation(4.0) - 4.0) < 1e-5);
        UNIT_ASSERT(fabs(est.GetEstimation(0.0) - 0.0) < 1e-5);

        // check that update has right 1/2 weight
        est.Update(2.5, 2.5);
        UNIT_ASSERT(fabs(est.GetEstimation(3.0) - 3.0) < 1e-5);
        UNIT_ASSERT(fabs(est.GetEstimation(4.0) - 4.0) < 1e-5);
        UNIT_ASSERT(fabs(est.GetEstimation(0.0) - 0.0) < 1e-5);
        UNIT_ASSERT(fabs(est.GetAverage() - 2.0) < 1e-5);

        // check history wipe out
        for (int i = 1; i < 100; i++) {
            est.Update(10.0, 1.0);
            est.Update(11.0, 5.0);
        }
        UNIT_ASSERT(fabs(est.GetEstimation(1.0) - 10.0) < 1e-5);
        UNIT_ASSERT(fabs(est.GetEstimation(5.0) - 11.0) < 1e-5);
        UNIT_ASSERT(fabs(est.GetEstimation(9.0) - 12.0) < 1e-5);
        UNIT_ASSERT(fabs(est.GetEstimation(3.0) - 10.5) < 1e-5);

        // another check history wipe out
        for (int i = 1; i < 100; i++) {
            est.Update(10.0, 5.0);
            est.Update(11.0, 1.0);
        }
        UNIT_ASSERT(fabs(est.GetEstimation(5.0) - 10.0) < 1e-5);
        UNIT_ASSERT(fabs(est.GetEstimation(1.0) - 11.0) < 1e-5);
        UNIT_ASSERT(fabs(est.GetEstimation(9.0) - 9.0) < 1e-5);
        UNIT_ASSERT(fabs(est.GetEstimation(3.0) - 10.5) < 1e-5);
    }
}
