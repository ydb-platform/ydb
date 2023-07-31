#include "polygon.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NGeo;

Y_UNIT_TEST_SUITE(TGeoPolygonTest) {
    Y_UNIT_TEST(TestEmptyPolygon) {
        TGeoPolygon empty;
        UNIT_ASSERT(!empty);
        UNIT_ASSERT(!empty.IsValid());
    }

    Y_UNIT_TEST(TestPolygon) {
        TGeoPolygon polygon({{1., 2.}, {2., 1.}, {2., 4.}, {1., 3.}});
        UNIT_ASSERT(polygon.IsValid());
        UNIT_ASSERT_EQUAL(polygon.GetWindow(),
                          TGeoWindow(TGeoPoint(1., 1.), TGeoPoint(2., 4.)));
    }

    Y_UNIT_TEST(TestParse) {
        UNIT_ASSERT_EQUAL(TGeoPolygon::Parse(TString{"1.23,5.67 7.89,10.11 11.10,9.87"}),
                          NGeo::TGeoPolygon({{1.23, 5.67}, {7.89, 10.11}, {11.10, 9.87}}));
        UNIT_ASSERT_EQUAL(TGeoPolygon::Parse(TString{"1.23,5.67 7.89,10.11 11.10,9.87 6.54,3.21"}),
                          NGeo::TGeoPolygon({{1.23, 5.67}, {7.89, 10.11}, {11.10, 9.87}, {6.54, 3.21}}));

        UNIT_ASSERT(TGeoPolygon::TryParse(TString{"1.23,5.67 7.89,10.11"}).Empty());
        UNIT_ASSERT_EQUAL(TGeoPolygon::Parse(TString{"1.23+5.67~7.89+10.11~11.10+9.87"}, "+", "~"),
                          NGeo::TGeoPolygon({{1.23, 5.67}, {7.89, 10.11}, {11.10, 9.87}}));

        UNIT_ASSERT_EQUAL(TGeoPolygon::Parse(TString{"1.23+5.67+~7.89+10.11+~11.10+9.87"}, "+", "+~"),
                          NGeo::TGeoPolygon({{1.23, 5.67}, {7.89, 10.11}, {11.10, 9.87}}));
    }
}
