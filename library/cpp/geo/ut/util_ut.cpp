#include <library/cpp/geo/util.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NGeo;

Y_UNIT_TEST_SUITE(TGeoUtilTest) {
    Y_UNIT_TEST(TestPointFromString) {
        UNIT_ASSERT_EQUAL(PairFromString("27.56,53.90"), (std::pair<double, double>(27.56, 53.90)));
        UNIT_ASSERT_EQUAL(PairFromString("27.56 53.90", " "), (std::pair<double, double>(27.56, 53.90)));
        UNIT_ASSERT_EQUAL(PairFromString("27.56@@53.90", "@@"), (std::pair<double, double>(27.56, 53.90)));
        UNIT_ASSERT_EXCEPTION(PairFromString("27.56@@53.90", "@"), TBadCastException);
        UNIT_ASSERT_EXCEPTION(PairFromString(""), TBadCastException);
    }

    Y_UNIT_TEST(TestTryPointFromString) {
        std::pair<double, double> point;

        UNIT_ASSERT(TryPairFromString(point, "27.56,53.90"));
        UNIT_ASSERT_EQUAL(point, (std::pair<double, double>(27.56, 53.90)));

        UNIT_ASSERT(TryPairFromString(point, "27.56 53.90", " "));
        UNIT_ASSERT_EQUAL(point, (std::pair<double, double>(27.56, 53.90)));

        UNIT_ASSERT(TryPairFromString(point, "27.56@@53.90", "@@"));
        UNIT_ASSERT_EQUAL(point, (std::pair<double, double>(27.56, 53.90)));

        UNIT_ASSERT(!TryPairFromString(point, "27.56@@53.90", "@"));
        UNIT_ASSERT(!TryPairFromString(point, ""));
    }

    Y_UNIT_TEST(TestVisibleMapBound) {
        const double expectedLat = MercatorToLL(TMercatorPoint(0., LLToMercator(TGeoPoint(180., 0.)).X())).Lat();
        UNIT_ASSERT_DOUBLES_EQUAL(VISIBLE_LATITUDE_BOUND, expectedLat, 1.e-14);
    }
}
