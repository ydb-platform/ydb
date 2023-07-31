#include "point.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NGeo;

namespace {
    void CheckMercator(TGeoPoint input, TMercatorPoint answer, double eps = 1.e-8) {
        auto output = LLToMercator(input);
        UNIT_ASSERT_DOUBLES_EQUAL(output.X(), answer.X(), eps);
        UNIT_ASSERT_DOUBLES_EQUAL(output.Y(), answer.Y(), eps);
    }

    void CheckGeo(TMercatorPoint input, TGeoPoint answer, double eps = 1.e-8) {
        auto output = MercatorToLL(input);
        UNIT_ASSERT_DOUBLES_EQUAL(output.Lon(), answer.Lon(), eps);
        UNIT_ASSERT_DOUBLES_EQUAL(output.Lat(), answer.Lat(), eps);
    }
} // namespace

Y_UNIT_TEST_SUITE(TPointTest) {
    Y_UNIT_TEST(TestGeoPointFromString) {
        UNIT_ASSERT_EQUAL(TGeoPoint::Parse("0.15,0.67"),
                          TGeoPoint(0.15, 0.67));
        UNIT_ASSERT_EQUAL(TGeoPoint::Parse("-52.,-27."),
                          TGeoPoint(-52., -27.));
        UNIT_ASSERT_EQUAL(TGeoPoint::Parse("0.15 0.67", " "),
                          TGeoPoint(0.15, 0.67));
        UNIT_ASSERT_EQUAL(TGeoPoint::Parse("-27. -52", " "),
                          TGeoPoint(-27., -52.));
        UNIT_ASSERT_EQUAL(TGeoPoint::Parse("182,55"),
                          TGeoPoint(182., 55.));

        // current behavior
        UNIT_ASSERT(TGeoPoint::TryParse(TString{}).Empty());
        UNIT_ASSERT_EXCEPTION(TGeoPoint::Parse("Hello,world"), TBadCastException);
        UNIT_ASSERT_EXCEPTION(TGeoPoint::Parse("640 17", " "), TBadCastException);
        UNIT_ASSERT_EXCEPTION(TGeoPoint::Parse("50.,100"), TBadCastException);
        UNIT_ASSERT_EQUAL(TGeoPoint::Parse("     0.01, 0.01"), TGeoPoint(0.01, 0.01));
        UNIT_ASSERT_EXCEPTION(TGeoPoint::Parse("0.01 , 0.01"), TBadCastException);
        UNIT_ASSERT_EXCEPTION(TGeoPoint::Parse("0.01, 0.01 "), TBadCastException);
    }
}

Y_UNIT_TEST_SUITE(TConversionTest) {
    Y_UNIT_TEST(TestConversionGeoToMercator) {
        // test data is obtained using PostGIS:
        // SELECT ST_AsText(ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat), 4326), 3395))

        CheckMercator({27.547028, 53.893962}, {3066521.12982805, 7115552.47353991});
        CheckMercator({-70.862782, -53.002613}, {-7888408.80843475, -6949331.55685883});
        CheckMercator({37.588536, 55.734004}, {4184336.68718463, 7470303.90973406});
        CheckMercator({0., 0.}, {0, 0});
    }

    Y_UNIT_TEST(TestConversionMercatorToGeo) {
        // test data is obtained using PostGIS:
        // SELECT ST_AsText(ST_Transform(ST_SetSRID(ST_MakePoint(X, Y), 3395), 4326))

        CheckGeo({3066521, 7115552}, {27.5470268337348, 53.8939594873943});
        CheckGeo({-7888409, -6949332}, {-70.8627837208599, -53.0026154014032});
        CheckGeo({4184336, 7470304}, {37.5885298269154, 55.734004457522});
        CheckGeo({0, 0}, {0., 0.});
    }

    Y_UNIT_TEST(TestExactConversion) {
        // Zero maps to zero with no epsilons
        UNIT_ASSERT_VALUES_EQUAL(LLToMercator({0., 0.}).X(), 0.);
        UNIT_ASSERT_VALUES_EQUAL(LLToMercator({0., 0.}).Y(), 0.);
        UNIT_ASSERT_VALUES_EQUAL(MercatorToLL({0., 0.}).Lon(), 0.);
        UNIT_ASSERT_VALUES_EQUAL(MercatorToLL({0., 0.}).Lat(), 0.);
    }

    Y_UNIT_TEST(TestPoles) {
        UNIT_ASSERT_VALUES_EQUAL(LLToMercator({0, 90}).Y(), std::numeric_limits<double>::infinity());
        UNIT_ASSERT_VALUES_EQUAL(LLToMercator({0, -90}).Y(), -std::numeric_limits<double>::infinity());

        UNIT_ASSERT_VALUES_EQUAL(MercatorToLL({0, std::numeric_limits<double>::infinity()}).Lat(), 90.);
        UNIT_ASSERT_VALUES_EQUAL(MercatorToLL({0, -std::numeric_limits<double>::infinity()}).Lat(), -90.);
    }

    Y_UNIT_TEST(TestNearPoles) {
        // Reference values were obtained using mpmath library (floating-point arithmetic with arbitrary precision)
        CheckMercator({0., 89.9}, {0., 44884542.157175040}, 1.e-6);
        CheckMercator({0., 89.99}, {0., 59570746.872518855}, 1.e-5);
        CheckMercator({0., 89.999}, {0., 74256950.065173316}, 1.e-4);
        CheckMercator({0., 89.9999}, {0., 88943153.242600886}, 1.e-3);
        CheckMercator({0., 89.99999}, {0., 103629356.41987618}, 1.e-1);
        CheckMercator({0., 89.999999}, {0., 118315559.59714996}, 1.e-1);
        CheckMercator({0., 89.9999999}, {0., 133001762.77442373}, 1.e-0);
        CheckMercator({0., 89.99999999}, {0., 147687965.95169749}, 1.e+1);
        CheckMercator({0., 89.9999999999999857891452847979962825775146484375}, {0., 233563773.75716050}, 1.e+7);

        CheckGeo({0., 233563773.75716050}, {0., 89.9999999999999857891452847979962825775146484375}, 1.e-15);
        CheckGeo({0., 147687965.95169749}, {0., 89.99999999}, 1.e-13);
        CheckGeo({0., 133001762.77442373}, {0., 89.9999999}, 1.e-13);
        CheckGeo({0., 118315559.59714996}, {0., 89.999999}, 1.e-13);
        CheckGeo({0., 103629356.41987618}, {0., 89.99999}, 1.e-13);
        CheckGeo({0., 88943153.242600886}, {0., 89.9999}, 1.e-13);
        CheckGeo({0., 74256950.065173316}, {0., 89.999}, 1.e-13);
        CheckGeo({0., 59570746.872518855}, {0., 89.99}, 1.e-13);
        CheckGeo({0., 44884542.157175040}, {0., 89.9}, 1.e-13);
    }

    Y_UNIT_TEST(TestVisibleRange) {
        UNIT_ASSERT(TGeoPoint(37., 55.).IsVisibleOnMap());
        UNIT_ASSERT(!TGeoPoint(37., 86.).IsVisibleOnMap());
        UNIT_ASSERT(TGeoPoint(37., -85.).IsVisibleOnMap());
        UNIT_ASSERT(!TGeoPoint(37., -90.).IsVisibleOnMap());
    }

    Y_UNIT_TEST(TestRoundTripGeoMercatorGeo) {
        auto check = [](double longitude, double latitude) {
            auto pt = MercatorToLL(LLToMercator(TGeoPoint{longitude, latitude}));
            UNIT_ASSERT_DOUBLES_EQUAL_C(longitude, pt.Lon(), 1.e-12, "longitude for point (" << longitude << ", " << latitude << ")");
            UNIT_ASSERT_DOUBLES_EQUAL_C(latitude, pt.Lat(), 1.e-8, "latitude for point (" << longitude << ", " << latitude << ")");
        };

        check(37., 55.);
        check(0.1, 0.1);
        check(0.2, 89.9);
        check(181., -42.);
        check(362., -43.);
        check(-183., -87.);
        check(1000., -77.);
    }

    Y_UNIT_TEST(TestRoundTripMercatorGeoMercator) {
        auto check = [](double x, double y) {
            auto pt = LLToMercator(MercatorToLL(TMercatorPoint{x, y}));
            UNIT_ASSERT_DOUBLES_EQUAL_C(x, pt.X(), 1.e-4, "x for point (" << x << ", " << y << ")");
            UNIT_ASSERT_DOUBLES_EQUAL_C(y, pt.Y(), 1.e-4, "y for point (" << x << ", " << y << ")");
        };

        check(100., 200.);
        check(-123456., 654321.);
        check(5.e7, 1.23456789);
        check(1.e8, -2.e7);
    }
}

Y_UNIT_TEST_SUITE(TestDistance) {
    Y_UNIT_TEST(TestGeodeticDistance) {
        const TGeoPoint minsk(27.55, 53.916667);
        const TGeoPoint moscow(37.617778, 55.755833);
        const TGeoPoint newYork(-73.994167, 40.728333);
        const TGeoPoint sydney(151.208333, -33.869444);

        const double eps = 1.E-6; // absolute error

        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(minsk, minsk), 0.0, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(minsk, moscow), 677190.08871321136, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(minsk, newYork), 7129091.7536358498, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(minsk, sydney), 15110861.267782301, eps);

        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(moscow, minsk), 677190.08871321136, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(moscow, moscow), 0.0, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(moscow, newYork), 7519517.2469277605, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(moscow, sydney), 14467193.188083574, eps);

        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(newYork, minsk), 7129091.7536358498, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(newYork, moscow), 7519517.2469277605, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(newYork, newYork), 0.0, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(newYork, sydney), 15954603.669226252, eps);

        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(sydney, minsk), 15110861.267782301, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(sydney, moscow), 14467193.188083574, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(sydney, newYork), 15954603.669226252, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(GeodeticDistance(sydney, sydney), 0.0, eps);
    }
}
