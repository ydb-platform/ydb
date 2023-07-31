#include "window.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ymath.h>

using namespace NGeo;

namespace {
    constexpr double DEFAULT_EPS = 1.E-5;

    bool CheckGeoPointEqual(const TGeoPoint& found, const TGeoPoint& expected, const double eps = DEFAULT_EPS) {
        if (std::isnan(found.Lon()) || std::isnan(found.Lat())) {
            Cerr << "NaNs found: (" << found.Lon() << ", " << found.Lat() << ")" << Endl;
            return false;
        }
        if (Abs(found.Lon() - expected.Lon()) > eps) {
            Cerr << "longitude differs: " << found.Lon() << " found, " << expected.Lon() << " expected" << Endl;
            return false;
        }
        if (Abs(found.Lat() - expected.Lat()) > eps) {
            Cerr << "latitude differs: " << found.Lat() << " found, " << expected.Lat() << " expected" << Endl;
            return false;
        }
        return true;
    }

    bool CheckSizeEqual(const TSize& found, const TSize& expected, const double eps = DEFAULT_EPS) {
        if (std::isnan(found.GetWidth()) || std::isnan(found.GetHeight())) {
            Cerr << "NaNs found: (" << found.GetWidth() << ", " << found.GetHeight() << ")" << Endl;
            return false;
        }
        if (Abs(found.GetWidth() - expected.GetWidth()) > eps) {
            Cerr << "width differs: " << found.GetWidth() << " found, " << expected.GetWidth() << " expected" << Endl;
            return false;
        }
        if (Abs(found.GetHeight() - expected.GetHeight()) > eps) {
            Cerr << "height differs: " << found.GetHeight() << " found, " << expected.GetHeight() << " expected" << Endl;
            return false;
        }
        return true;
    }

    bool CheckGeoWindowEqual(const TGeoWindow& lhs, const TGeoWindow& rhs, const double eps = DEFAULT_EPS) {
        return CheckGeoPointEqual(lhs.GetCenter(), rhs.GetCenter(), eps) && CheckSizeEqual(lhs.GetSize(), rhs.GetSize(), eps);
    }
} // namespace

/**
 * TGeoWindow
 */
Y_UNIT_TEST_SUITE(TGeoWindowTest) {
    Y_UNIT_TEST(TestParser) {
        UNIT_ASSERT_EQUAL(TGeoWindow::ParseFromCornersPoints("1.23,5.67", "7.65,3.21"),
                          TGeoWindow(TGeoPoint(1.23, 3.21), TGeoPoint(7.65, 5.67)));
        UNIT_ASSERT_EQUAL(TGeoWindow::ParseFromCornersPoints("1.23~5.67", "7.65~3.21", "~"),
                          TGeoWindow(TGeoPoint(1.23, 3.21), TGeoPoint(7.65, 5.67)));
        UNIT_ASSERT_EXCEPTION(TGeoWindow::ParseFromCornersPoints("1.23~5.67", "7.65~3.21"), TBadCastException);

        UNIT_ASSERT(TGeoWindow::TryParseFromCornersPoints("1.23~5.67", "7.65~3.21").Empty());
        UNIT_ASSERT(TGeoWindow::TryParseFromCornersPoints("1.23,5.67", "7.65,3.21").Defined());
        UNIT_ASSERT_EQUAL(TGeoWindow::TryParseFromCornersPoints("1.23,5.67", "7.65,3.21").GetRef(),
                          TGeoWindow(TGeoPoint(1.23, 3.21), TGeoPoint(7.65, 5.67)));
        UNIT_ASSERT(TGeoWindow::TryParseFromCornersPoints("1.23+++5.67+", "7.65+++3.21+", "+++").Empty());

        UNIT_ASSERT_EQUAL(TGeoWindow::ParseFromLlAndSpn("1.23,5.67", "0.1,0.2"),
                          TGeoWindow(TGeoPoint(1.23, 5.67), TSize(0.1, 0.2)));
        UNIT_ASSERT_EQUAL(TGeoWindow::ParseFromLlAndSpn("1.23~5.67", "0.1~0.2", "~"),
                          TGeoWindow(TGeoPoint(1.23, 5.67), TSize(0.1, 0.2)));
        UNIT_ASSERT_EXCEPTION(TGeoWindow::ParseFromLlAndSpn("1.23~5.67", "0.1~0.2"), TBadCastException);
        UNIT_ASSERT(TGeoWindow::TryParseFromLlAndSpn("1.23~5.67", "0.1~0.2").Empty());
        UNIT_ASSERT(TGeoWindow::TryParseFromLlAndSpn("1.23~5.67", "0.1~0.2", "~").Defined());
        UNIT_ASSERT_EQUAL(TGeoWindow::TryParseFromLlAndSpn("1.23~5.67", "0.1~0.2", "~").GetRef(),
                          TGeoWindow(TGeoPoint(1.23, 5.67), TSize(0.1, 0.2)));
    }

    Y_UNIT_TEST(TestConstructor) {
        TGeoPoint center{55.50, 82.50};
        TSize size{5.00, 3.00};
        TGeoWindow window(center, size);

        UNIT_ASSERT_EQUAL(window.GetCenter(), center);
        UNIT_ASSERT_EQUAL(window.GetSize(), size);
    }

    Y_UNIT_TEST(TestPoles) {
        {
            TGeoWindow northPole{TGeoPoint{180., 90.}, TSize{1.5, 1.5}};
            UNIT_ASSERT(CheckGeoPointEqual(northPole.GetCenter(), TGeoPoint{180., 90.}));
            UNIT_ASSERT(CheckGeoPointEqual(northPole.GetLowerLeftCorner(), TGeoPoint{179.25, 88.5}));
            UNIT_ASSERT(CheckGeoPointEqual(northPole.GetUpperRightCorner(), TGeoPoint{180.75, 90.0}));
        }
        {
            TGeoWindow tallWindow{TGeoPoint{37., 55.}, TSize{10., 180.}};
            UNIT_ASSERT(CheckGeoPointEqual(tallWindow.GetCenter(), TGeoPoint{37., 55.}));
            UNIT_ASSERT(CheckGeoPointEqual(tallWindow.GetLowerLeftCorner(), TGeoPoint{32., -90.}));
            UNIT_ASSERT(CheckGeoPointEqual(tallWindow.GetUpperRightCorner(), TGeoPoint{42., 90.}));
        }
        {
            TGeoWindow world{TGeoPoint{0., 0.}, TSize{360., 180.}};
            UNIT_ASSERT(CheckGeoPointEqual(world.GetCenter(), TGeoPoint{0., 0.}));
            UNIT_ASSERT(CheckGeoPointEqual(world.GetLowerLeftCorner(), TGeoPoint{-180., -90.}));
            UNIT_ASSERT(CheckGeoPointEqual(world.GetUpperRightCorner(), TGeoPoint{180., 90.}));
        }
        {
            TGeoWindow world{TGeoPoint{0., 0.}, TSize{360., 360.}};
            UNIT_ASSERT(CheckGeoPointEqual(world.GetCenter(), TGeoPoint{0., 0.}));
            UNIT_ASSERT(CheckGeoPointEqual(world.GetLowerLeftCorner(), TGeoPoint{-180., -90.}));
            UNIT_ASSERT(CheckGeoPointEqual(world.GetUpperRightCorner(), TGeoPoint{180., 90.}));
        }
    }

    Y_UNIT_TEST(TestBigSize) {
        {
            TGeoWindow w{TGeoPoint{37., 55.}, TSize{100., 179.}};
            UNIT_ASSERT(CheckGeoPointEqual(w.GetCenter(), TGeoPoint{37., 55.}));
            UNIT_ASSERT(CheckGeoPointEqual(w.GetLowerLeftCorner(), TGeoPoint{-13., -89.09540675}));
            UNIT_ASSERT(CheckGeoPointEqual(w.GetUpperRightCorner(), TGeoPoint{87., 89.90907637}));
        }
    }

    Y_UNIT_TEST(TestCenterWhenInitWithCorners) {
        UNIT_ASSERT(CheckGeoPointEqual(TGeoWindow(TGeoPoint{5.00, 40.00}, TGeoPoint{25.00, 80.00}).GetCenter(), TGeoPoint{15.00, 67.17797}));
        UNIT_ASSERT(CheckGeoPointEqual(TGeoWindow(TGeoPoint{-5.00, -40.00}, TGeoPoint{-25.00, -80.00}).GetCenter(), TGeoPoint{-15.00, -67.17797}));
    }

    Y_UNIT_TEST(TestCornersWhenInitWithCenter) {
        // check lat calc
        UNIT_ASSERT_DOUBLES_EQUAL(TGeoWindow(TGeoPoint{25.00, 50.00}, TSize{10.00, 10.00}).GetLowerLeftCorner().Lat(), 44.73927, DEFAULT_EPS);

        // lat equals to 90
        UNIT_ASSERT_DOUBLES_EQUAL(TGeoWindow(TGeoPoint{25.00, 50.00}, TSize{10.00, 179.99999}).GetUpperRightCorner().Lat(), 90, DEFAULT_EPS);

        // lat equals to -90
        UNIT_ASSERT_DOUBLES_EQUAL(TGeoWindow(TGeoPoint{25.00, -50.00}, TSize{10.00, -179.99999}).GetUpperRightCorner().Lat(), -90, DEFAULT_EPS);

        // check naive lon calc
        UNIT_ASSERT_DOUBLES_EQUAL(TGeoWindow(TGeoPoint{10, 10}, TSize{10, 5}).GetLowerLeftCorner().Lon(), 5, DEFAULT_EPS);

        // check lon equals to 190 (no wrapping)
        UNIT_ASSERT_DOUBLES_EQUAL(TGeoWindow(TGeoPoint{20, 0}, TSize{340, 5}).GetUpperRightCorner().Lon(), 190, DEFAULT_EPS);

        UNIT_ASSERT_DOUBLES_EQUAL(TGeoWindow(TGeoPoint{-40, 0}, TSize{-280, 5}).GetUpperRightCorner().Lon(), -180, DEFAULT_EPS);

        // naive calculating when point is (0, 0)
        UNIT_ASSERT(CheckGeoPointEqual(TGeoWindow(TGeoPoint{0, 0}, TSize{160, 160}).GetLowerLeftCorner(), TGeoPoint{-80, -80}, DEFAULT_EPS));
        UNIT_ASSERT(CheckGeoPointEqual(TGeoWindow(TGeoPoint{0, 0}, TSize{160, 160}).GetUpperRightCorner(), TGeoPoint{80, 80}, DEFAULT_EPS));
    }

    Y_UNIT_TEST(TestCenterSetter) {
        TGeoPoint center{27.56, 53.90};
        TGeoWindow window{};
        window.SetCenter(center);
        UNIT_ASSERT_EQUAL(window.GetCenter(), center);
    }

    Y_UNIT_TEST(TestEqualOperator) {
        TGeoWindow window{TGeoPoint{27.56, 53.90}, TGeoPoint{30.35, 56.89}};
        UNIT_ASSERT(window == window);

        TGeoWindow anotherWindow{TGeoPoint{60.10, 57.90}, TGeoPoint{60.70, 58.25}};
        UNIT_ASSERT(!(window == anotherWindow));
    }

    Y_UNIT_TEST(TestAssignmentOperator) {
        TGeoWindow lhs{TGeoPoint{27.56, 53.90}, TGeoPoint{30.35, 53.89}};
        TGeoWindow rhs{};
        rhs = lhs;
        UNIT_ASSERT_EQUAL(lhs, rhs);
    }

    Y_UNIT_TEST(TestContainsMethod) {
        // you could see cases here https://tech.yandex.ru/maps/jsbox/2.1/rectangle
        // (pay attention that the first coord is lat and the second one is lon)
        TGeoWindow window{TGeoPoint{27.45, 53.82}, TGeoPoint{27.65, 53.97}};

        // point is inside the window
        UNIT_ASSERT(window.Contains(TGeoPoint{27.55, 53.90}));

        // point is to the right of the window
        UNIT_ASSERT(!window.Contains(TGeoPoint{27.66, 53.95}));

        // point is to the left of the window
        UNIT_ASSERT(!window.Contains(TGeoPoint{27.44, 53.95}));

        // point is under the window
        UNIT_ASSERT(!window.Contains(TGeoPoint{27.50, 53.81}));

        // point is above the window
        UNIT_ASSERT(!window.Contains(TGeoPoint{27.50, 53.98}));

        // point is on border
        UNIT_ASSERT(window.Contains(TGeoPoint{27.45, 53.86}));
        UNIT_ASSERT(window.Contains(TGeoPoint{27.65, 53.86}));
        UNIT_ASSERT(window.Contains(TGeoPoint{27.55, 53.82}));
        UNIT_ASSERT(window.Contains(TGeoPoint{27.55, 53.97}));

        // negate coord
        UNIT_ASSERT(TGeoWindow(TGeoPoint{-72.17, -38.82}, TGeoPoint{-68.95, -36.70}).Contains(TGeoPoint{-70.40, -37.75}));

        // special cases
        UNIT_ASSERT(!TGeoWindow{}.Contains(TGeoPoint{60.09, 57.90}));

        UNIT_ASSERT(TGeoWindow(TGeoPoint{}, TGeoPoint{27.55, 53.90}).Contains(TGeoPoint{27.55, 53.90}));
        UNIT_ASSERT(TGeoWindow(TGeoPoint{27.55, 53.90}, TGeoPoint{}).Contains(TGeoPoint{27.55, 53.90}));
    }

    Y_UNIT_TEST(TestIntersectsMethod) {
        // intersect only by lat
        UNIT_ASSERT(
            !Intersects(
                TGeoWindow{TGeoPoint{27.60, 53.90}, TGeoPoint{27.80, 53.95}},
                TGeoWindow{TGeoPoint{27.30, 53.88}, TGeoPoint{27.50, 53.98}}));

        // intersect only by lon
        UNIT_ASSERT(
            !Intersects(
                TGeoWindow{TGeoPoint{27.35, 54}, TGeoPoint{27.45, 54.10}},
                TGeoWindow{TGeoPoint{27.30, 53.88}, TGeoPoint{27.50, 53.98}}));

        // one inside another
        UNIT_ASSERT(
            Intersects(
                TGeoWindow{TGeoPoint{27.35, 53.90}, TGeoPoint{27.45, 53.95}},
                TGeoWindow{TGeoPoint{27.30, 53.88}, TGeoPoint{27.50, 53.98}}));

        // intersection is point
        UNIT_ASSERT(
            !Intersects(
                TGeoWindow{TGeoPoint{27.50, 53.98}, TGeoPoint{27.70, 54.00}},
                TGeoWindow{TGeoPoint{27.30, 53.88}, TGeoPoint{27.50, 53.98}}));

        // intersection is segment
        UNIT_ASSERT(
            !Intersects(
                TGeoWindow{TGeoPoint{27.40, 53.98}, TGeoPoint{27.70, 54.00}},
                TGeoWindow{TGeoPoint{27.30, 53.88}, TGeoPoint{27.50, 53.98}}));

        // intersection is area
        UNIT_ASSERT(
            Intersects(
                TGeoWindow{TGeoPoint{27.40, 53.90}, TGeoPoint{27.70, 54.00}},
                TGeoWindow{TGeoPoint{27.30, 53.88}, TGeoPoint{27.50, 53.98}}));

        // equal windows
        TGeoWindow window{TGeoPoint{27.60, 53.88}, TGeoPoint{27.80, 53.98}};
        UNIT_ASSERT(Intersects(window, window));
    }

    Y_UNIT_TEST(TestIntersectionMethod) {
        // non-intersecting window
        UNIT_ASSERT(
            !(Intersection(
                TGeoWindow{TGeoPoint{37.66, 55.66}, TGeoPoint{37.53, 55.64}},
                TGeoWindow{TGeoPoint{37.67, 55.66}, TGeoPoint{37.69, 55.71}})));

        // one inside another
        UNIT_ASSERT(CheckGeoWindowEqual(
            Intersection(
                TGeoWindow{TGeoPoint{37.00, 55.00}, TSize{10.00, 10.00}},
                TGeoWindow{TGeoPoint{37.00, 55.00}, TSize{2.00, 2.00}})
                .GetRef(),
            (TGeoWindow{TGeoPoint{37.00, 55.00}, TSize{2.00, 2.00}})));

        // cross
        UNIT_ASSERT(CheckGeoWindowEqual(
            Intersection(
                TGeoWindow{TGeoPoint{37.00, 55.00}, TSize{10.00, 2.00}},
                TGeoWindow{TGeoPoint{37.00, 55.00}, TSize{2.00, 10.00}})
                .GetRef(),
            (TGeoWindow{TGeoPoint{37.00, 55.00}, TSize{2.00, 2.00}})));

        // intersection is a point
        UNIT_ASSERT(CheckGeoWindowEqual(
            Intersection(
                TGeoWindow{TGeoPoint{27.50, 53.98}, TGeoPoint{27.70, 54.00}},
                TGeoWindow{TGeoPoint{27.30, 53.88}, TGeoPoint{27.50, 53.98}})
                .GetRef(),
            (TGeoWindow{TGeoPoint{27.50, 53.98}, TSize{0, 0}})));

        // intersection is a segment
        UNIT_ASSERT(CheckGeoWindowEqual(
            Intersection(
                TGeoWindow{TGeoPoint{27.40, 53.98}, TGeoPoint{27.70, 54.00}},
                TGeoWindow{TGeoPoint{27.30, 53.88}, TGeoPoint{27.50, 53.98}})
                .GetRef(),
            (TGeoWindow{TGeoPoint{27.45, 53.98}, TSize{0.10, 0}})));

        // intersection is area
        UNIT_ASSERT(CheckGeoWindowEqual(
            Intersection(
                TGeoWindow{TGeoPoint{27.40, 53.90}, TGeoPoint{27.70, 54.00}},
                TGeoWindow{TGeoPoint{27.30, 53.88}, TGeoPoint{27.50, 53.98}})
                .GetRef(),
            (TGeoWindow{TGeoPoint{27.40, 53.90}, TGeoPoint{27.50, 53.98}})));

        // special cases
        UNIT_ASSERT(
            !(Intersection(
                TGeoWindow{TGeoPoint{27.30, 53.88}, TGeoPoint{27.50, 53.98}},
                TGeoWindow{})));
    }

    Y_UNIT_TEST(TestDistanceMethod) {
        // one window inside another
        UNIT_ASSERT_DOUBLES_EQUAL(
            (TGeoWindow{TGeoPoint{27.50, 53.98}, TGeoPoint{27.80, 54.10}})
                .Distance(TGeoWindow{TGeoPoint{27.55, 54.00}, TGeoPoint{27.70, 54.07}}),
            0,
            1.E-5);

        // gap only by lon
        UNIT_ASSERT_DOUBLES_EQUAL(
            (TGeoWindow{TGeoPoint{27.50, 53.98}, TGeoPoint{27.60, 54.10}})
                .Distance(TGeoWindow{TGeoPoint{27.69, 54.10}, TGeoPoint{27.90, 54.20}}),
            0.052773,
            1.E-5);

        // gap only by lat
        UNIT_ASSERT_DOUBLES_EQUAL(
            (TGeoWindow{TGeoPoint{27.50, 53.98}, TGeoPoint{27.60, 54.10}})
                .Distance(TGeoWindow{TGeoPoint{27.50, 54.20}, TGeoPoint{27.70, 54.30}}),
            0.1,
            1.E-5);

        // gap by lot and lat, you can calculate answer using two previous tests
        UNIT_ASSERT_DOUBLES_EQUAL(
            (TGeoWindow{TGeoPoint{27.50, 53.98}, TGeoPoint{27.60, 54.10}}
                 .Distance(TGeoWindow{TGeoPoint{27.69, 54.20}, TGeoPoint{27.70, 54.30}})),
            0.11304,
            1.E-5);

        // negate coord
        UNIT_ASSERT_DOUBLES_EQUAL(
            (TGeoWindow{TGeoPoint{-27.50, -53.98}, TGeoPoint{-27.60, -54.10}}
                 .Distance(TGeoWindow{TGeoPoint{-27.69, -54.20}, TGeoPoint{-27.70, -54.30}})),
            0.11304,
            1.E-5);
    }

    Y_UNIT_TEST(TestApproxDistanceMethod) {
        // point inside
        UNIT_ASSERT_DOUBLES_EQUAL(
            (TGeoWindow{TGeoPoint{27.50, 53.98}, TGeoPoint{27.80, 54.10}})
                .GetApproxDistance(TGeoPoint{27.60, 54.05}),
            0,
            1.E-5);

        // gap only by lon
        UNIT_ASSERT_DOUBLES_EQUAL(
            (TGeoWindow{TGeoPoint{27.50, 54.00}, TGeoPoint{27.60, 54.10}})
                .GetApproxDistance(TGeoPoint{27.70, 54.05}),
            6535.3,
            0.1);

        // gap only by lat
        UNIT_ASSERT_DOUBLES_EQUAL(
            (TGeoWindow{TGeoPoint{27.50, 54.00}, TGeoPoint{27.60, 54.10}})
                .GetApproxDistance(TGeoPoint{27.55, 53.95}),
            5566.0,
            0.1);

        // gap by lot and lat
        UNIT_ASSERT_DOUBLES_EQUAL(
            (TGeoWindow{TGeoPoint{27.50, 54.00}, TGeoPoint{27.60, 54.10}})
                .GetApproxDistance(TGeoPoint{27.70, 54.20}),
            12900.6,
            0.1);

        // negate coord
        UNIT_ASSERT_DOUBLES_EQUAL(
            (TGeoWindow{TGeoPoint{-27.50, -54.00}, TGeoPoint{-27.60, -54.10}})
                .GetApproxDistance(TGeoPoint{-27.70, -54.20}),
            12900.6,
            0.1);
    }

    Y_UNIT_TEST(TestUnionMethod) {
        // one inside another
        UNIT_ASSERT(CheckGeoWindowEqual(
            Union(
                TGeoWindow{TGeoPoint{37.00, 55.00}, TSize{2.00, 3.00}},
                TGeoWindow{TGeoPoint{37.10, 55.20}, TSize{1.50, 1.00}}),
            TGeoWindow(TGeoPoint{37.00, 55.00}, TSize{2.00, 3.00})));

        // non-intersecting windows
        UNIT_ASSERT(CheckGeoWindowEqual(
            Union(
                TGeoWindow{TGeoPoint{37.00, 55.00}, TGeoPoint{37.10, 55.10}},
                TGeoWindow{TGeoPoint{37.20, 55.20}, TGeoPoint{37.30, 55.30}}),
            TGeoWindow(TGeoPoint{37.00, 55.00}, TGeoPoint{37.30, 55.30})));

        // negate coords, one inside another
        UNIT_ASSERT(CheckGeoWindowEqual(
            Union(
                TGeoWindow{TGeoPoint{-57.62, -20.64}, TSize{2.00, 4.00}},
                TGeoWindow{TGeoPoint{-57.62, -20.64}, TSize{12.00, 10.00}}),
            TGeoWindow(TGeoPoint{-57.62, -20.64}, TSize{12.00, 10.00}), 1.E-2));

        // cross
        UNIT_ASSERT(CheckGeoWindowEqual(
            Union(
                TGeoWindow{TGeoPoint{-3.82, 5.52}, TGeoPoint{0.10, 6.50}},
                TGeoWindow{TGeoPoint{-1.5, 4.20}, TGeoPoint{-0.5, 7.13}}),
            TGeoWindow(TGeoPoint{-3.82, 4.20}, TGeoPoint{0.10, 7.13})));

        // special cases
        UNIT_ASSERT(CheckGeoWindowEqual(
            Union(
                TGeoWindow{TGeoPoint{-3.82, 5.52}, TGeoPoint{0.10, 6.50}},
                TGeoWindow{}),
            TGeoWindow(TGeoPoint{-3.82, 5.52}, TGeoPoint{361., 181.})));

        UNIT_ASSERT(CheckGeoWindowEqual(
            Union(
                TGeoWindow{},
                TGeoWindow{TGeoPoint{-3.82, 5.52}, TGeoPoint{0.10, 6.50}}),
            TGeoWindow(TGeoPoint{-3.82, 5.52}, TGeoPoint{361., 181.})));
    }

    Y_UNIT_TEST(TestStretchMethod) {
        TSize size{0.5, 1};
        TGeoPoint center{27.40, 53.90};
        TGeoWindow window{};
        double multiplier = 0;

        // multiplier is less than 1.
        window = {center, size};
        multiplier = 0.5;

        UNIT_ASSERT(CheckGeoPointEqual(window.GetLowerLeftCorner(), TGeoPoint{27.14999, 53.39699}));
        UNIT_ASSERT(CheckGeoPointEqual(window.GetUpperRightCorner(), TGeoPoint{27.65000, 54.39699}));

        window.Stretch(multiplier);
        UNIT_ASSERT(CheckGeoWindowEqual(window, TGeoWindow{center, TSize{0.25, 0.5}}));
        UNIT_ASSERT(CheckGeoPointEqual(window.GetLowerLeftCorner(), TGeoPoint{27.27499, 53.64925}));
        UNIT_ASSERT(CheckGeoPointEqual(window.GetUpperRightCorner(), TGeoPoint{27.52500, 54.14924}));

        // multiplier is greater than 1.
        window = {center, size};
        multiplier = 2.2;

        window.Stretch(multiplier);
        UNIT_ASSERT(CheckGeoWindowEqual(window, TGeoWindow{center, TSize{1.1, 2.2}}));
        UNIT_ASSERT(CheckGeoPointEqual(window.GetLowerLeftCorner(), TGeoPoint{26.84999, 52.78545}));
        UNIT_ASSERT(CheckGeoPointEqual(window.GetUpperRightCorner(), TGeoPoint{27.95000, 54.98545}));

        // invalid multiplier
        window = {center, size};
        multiplier = 100.;

        window.Stretch(multiplier);
        UNIT_ASSERT(CheckGeoWindowEqual(window, TGeoWindow{center, TSize{50, 100}}));
        UNIT_ASSERT(CheckGeoPointEqual(window.GetLowerLeftCorner(), TGeoPoint{2.40000, -18.88352}));
        UNIT_ASSERT(CheckGeoPointEqual(window.GetUpperRightCorner(), TGeoPoint{52.39999, 81.26212}));

        // invalid multiplier
        window = {center, size};
        multiplier = 0;

        window.Stretch(multiplier);
        UNIT_ASSERT(CheckGeoWindowEqual(window, TGeoWindow{center, TSize{0, 0}}));
        UNIT_ASSERT(CheckGeoPointEqual(window.GetLowerLeftCorner(), TGeoPoint{27.39999, 53.90000}));
        UNIT_ASSERT(CheckGeoPointEqual(window.GetUpperRightCorner(), TGeoPoint{27.39999, 53.90000}));

        // invalid multiplier
        window = {center, size};
        multiplier = -5.;

        window.Stretch(multiplier);
        UNIT_ASSERT(CheckGeoWindowEqual(window, TGeoWindow{center, TSize{-2.5, -5}}));
        UNIT_ASSERT(CheckGeoPointEqual(window.GetLowerLeftCorner(), TGeoPoint{28.64999, 56.32495}));
        UNIT_ASSERT(CheckGeoPointEqual(window.GetUpperRightCorner(), TGeoPoint{26.15000, 51.32491}));
    }
}

/**
 * TMercatorWindow
 */
Y_UNIT_TEST_SUITE(TMercatorWindowTest) {
    Y_UNIT_TEST(TestConstructor) {
        // init with two corners
        TMercatorPoint lowerLeft{5, 3};
        TMercatorPoint upperRight{10, 20};
        TMercatorWindow window{lowerLeft, upperRight};

        UNIT_ASSERT_EQUAL(window.GetWidth(), 5.);
        UNIT_ASSERT_EQUAL(window.GetHeight(), 17.);
        UNIT_ASSERT_EQUAL(window.GetCenter(), (TMercatorPoint{7.5, 11.5}));

        TMercatorPoint center{8, 12};
        TSize size{5, 17};
        window = {center, size};
        UNIT_ASSERT_EQUAL(window.GetUpperRightCorner().X(), 10.5);
        UNIT_ASSERT_EQUAL(window.GetUpperRightCorner().Y(), 20.5);
        UNIT_ASSERT_EQUAL(window.GetLowerLeftCorner().X(), 5.5);
        UNIT_ASSERT_EQUAL(window.GetLowerLeftCorner().Y(), 3.5);
    }

    Y_UNIT_TEST(TestInflateMethod) {
        TSize size{200, 500};
        TMercatorPoint center{441, 688};
        TMercatorWindow window{};
        int add = 10;

        window = {center, size};
        UNIT_ASSERT_EQUAL(window.GetLowerLeftCorner(), TMercatorPoint(341, 438));
        UNIT_ASSERT_EQUAL(window.GetUpperRightCorner(), TMercatorPoint(541, 938));
        window.Inflate(add);
        UNIT_ASSERT_EQUAL(window, TMercatorWindow(center, TSize{220, 520}));
        UNIT_ASSERT_EQUAL(window.GetLowerLeftCorner(), TMercatorPoint(331, 428));
        UNIT_ASSERT_EQUAL(window.GetUpperRightCorner(), TMercatorPoint(551, 948));

        // negate coords
        center = {-441, -688};
        window = {center, size};
        UNIT_ASSERT_EQUAL(window.GetLowerLeftCorner(), TMercatorPoint(-541, -938));
        UNIT_ASSERT_EQUAL(window.GetUpperRightCorner(), TMercatorPoint(-341, -438));
        window.Inflate(add);
        UNIT_ASSERT_EQUAL(window, TMercatorWindow(center, TSize{220, 520}));
        UNIT_ASSERT_EQUAL(window.GetLowerLeftCorner(), TMercatorPoint(-551, -948));
        UNIT_ASSERT_EQUAL(window.GetUpperRightCorner(), TMercatorPoint(-331, -428));

        // size becomes negate
        size = {6, 12};
        center = {0, 0};
        window = {center, size};
        UNIT_ASSERT_EQUAL(window.GetLowerLeftCorner(), TMercatorPoint(-3, -6));
        UNIT_ASSERT_EQUAL(window.GetUpperRightCorner(), TMercatorPoint(3, 6));

        add = -20;
        window.Inflate(add);
        UNIT_ASSERT_EQUAL(window, TMercatorWindow(center, TSize{-34, -28}));
        UNIT_ASSERT_EQUAL(window.GetLowerLeftCorner(), TMercatorPoint(17, 14));
        UNIT_ASSERT_EQUAL(window.GetUpperRightCorner(), TMercatorPoint(-17, -14));
        UNIT_ASSERT_EQUAL(window.GetSize(), TSize(-34, -28));

        // big add param
        size = {10, 15};
        center = {5, 10};
        window = {center, size};

        add = static_cast<int>(1E5);
        window.Inflate(add);
        UNIT_ASSERT_EQUAL(window, TMercatorWindow(center, TSize{200'010, 200'015}));
        UNIT_ASSERT_EQUAL(window.GetLowerLeftCorner(), TMercatorPoint(-100'000, -99'997.5));
        UNIT_ASSERT_EQUAL(window.GetUpperRightCorner(), TMercatorPoint(100'010, 100'017.5));
    }
}
