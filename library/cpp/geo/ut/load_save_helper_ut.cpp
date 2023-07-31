#include "load_save_helper.h"
#include "point.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/str.h>
#include <util/ysaveload.h>

namespace {
    void CheckSave(const NGeo::TGeoPoint& point) {
        TStringStream output;
        ::Save(&output, point);
        TStringStream answer;
        ::Save(&answer, static_cast<double>(point.Lon()));
        ::Save(&answer, static_cast<double>(point.Lat()));
        UNIT_ASSERT_EQUAL(output.Str(), answer.Str());
    }

    void CheckLoad(const double x, const double y) {
        TStringStream input;
        ::Save(&input, x);
        ::Save(&input, y);
        NGeo::TGeoPoint output;
        ::Load(&input, output);

        const double eps = 1.E-8;
        UNIT_ASSERT_DOUBLES_EQUAL(static_cast<double>(output.Lon()), x, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(static_cast<double>(output.Lat()), y, eps);
    }

    void CheckLoadAfterSavePointLL(double x, double y) {
        NGeo::TGeoPoint answer = {x, y};
        TStringStream iostream;
        ::Save(&iostream, answer);
        NGeo::TGeoPoint output;
        ::Load(&iostream, output);

        const double eps = 1.E-8;
        UNIT_ASSERT_DOUBLES_EQUAL(static_cast<double>(output.Lon()), x, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(static_cast<double>(output.Lat()), y, eps);
    }

    void CheckLoadAfterSaveWindowLL(NGeo::TGeoPoint center, NGeo::TSize size) {
        NGeo::TGeoWindow answer = {center, size};
        TStringStream iostream;
        ::Save(&iostream, answer);
        NGeo::TGeoWindow output;
        ::Load(&iostream, output);
        UNIT_ASSERT_EQUAL(output.GetCenter(), answer.GetCenter());
        UNIT_ASSERT_EQUAL(output.GetSize(), answer.GetSize());
    }
} // namespace

Y_UNIT_TEST_SUITE(TSaveLoadForPointLL) {
    Y_UNIT_TEST(TestSave) {
        // {27.561481, 53.902496} Minsk Lon and Lat
        CheckSave({27.561481, 53.902496});
        CheckSave({-27.561481, 53.902496});
        CheckSave({27.561481, -53.902496});
        CheckSave({-27.561481, -53.902496});
    }

    Y_UNIT_TEST(TestLoad) {
        CheckLoad(27.561481, 53.902496);
        CheckLoad(-27.561481, 53.902496);
        CheckLoad(27.561481, -53.902496);
        CheckLoad(-27.561481, -53.902496);
    }

    Y_UNIT_TEST(TestSaveLoad) {
        CheckLoadAfterSavePointLL(27.561481, 53.902496);
        CheckLoadAfterSavePointLL(-27.561481, 53.902496);
        CheckLoadAfterSavePointLL(27.561481, -53.902496);
        CheckLoadAfterSavePointLL(-27.561481, -53.902496);
        CheckLoadAfterSavePointLL(0, 0);
    }
}

Y_UNIT_TEST_SUITE(TSaveLoadForWindowLL) {
    Y_UNIT_TEST(TestSave) {
        CheckLoadAfterSaveWindowLL({27.561481, 53.902496}, {1, 2});
        CheckLoadAfterSaveWindowLL({27.561481, 53.902496}, {2, 1});
        CheckLoadAfterSaveWindowLL({-27.561481, 53.902496}, {1, 2});
        CheckLoadAfterSaveWindowLL({-27.561481, 53.902496}, {2, 1});
        CheckLoadAfterSaveWindowLL({27.561481, -53.902496}, {1, 2});
        CheckLoadAfterSaveWindowLL({27.561481, -53.902496}, {2, 1});
        CheckLoadAfterSaveWindowLL({-27.561481, -53.902496}, {1, 2});
        CheckLoadAfterSaveWindowLL({-27.561481, -53.902496}, {2, 1});
        CheckLoadAfterSaveWindowLL({0, 0}, {0, 0});
    }
}
