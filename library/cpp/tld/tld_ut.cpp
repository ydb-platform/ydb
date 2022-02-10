#include "tld.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/charset/doccodes.h>

using namespace NTld;

Y_UNIT_TEST_SUITE(TTldTest) {
    Y_UNIT_TEST(TestFindTld) {
        UNIT_ASSERT(FindTld("yandex.ru") == "ru");
        UNIT_ASSERT(FindTld("YandeX.Ru") == "Ru");
        UNIT_ASSERT(FindTld("yandex.com.tr") == "tr");
        UNIT_ASSERT(FindTld("com.tr") == "tr");
        UNIT_ASSERT(FindTld("abc.def.ghi") == "ghi");
        UNIT_ASSERT(FindTld("abc.def.aaaaaaaaaa") == "aaaaaaaaaa");
        UNIT_ASSERT(FindTld("a.b.c.d.e.f.g") == "g");

        UNIT_ASSERT(FindTld(".diff") == "diff");
        UNIT_ASSERT(FindTld(".") == "");
        UNIT_ASSERT(FindTld("ru") == "");
        UNIT_ASSERT(FindTld("") == "");
    }

    Y_UNIT_TEST(TestTLDs) {
        UNIT_ASSERT(IsTld("ru"));
        UNIT_ASSERT(IsTld("Ru"));
        UNIT_ASSERT(IsTld("BMW"));
        UNIT_ASSERT(IsTld("TiReS"));
        UNIT_ASSERT(IsTld("xn--p1ai"));
        UNIT_ASSERT(IsTld("YaHOO"));
        UNIT_ASSERT(!IsTld("xn"));

        UNIT_ASSERT(InTld("ru.ru"));
        UNIT_ASSERT(!InTld("ru"));
        UNIT_ASSERT(!InTld("ru."));
        UNIT_ASSERT(!InTld("ru.xn"));
    }

    Y_UNIT_TEST(TestVeryGoodTlds) {
        UNIT_ASSERT(IsVeryGoodTld("ru"));
        UNIT_ASSERT(IsVeryGoodTld("Ru"));
        UNIT_ASSERT(!IsVeryGoodTld("BMW"));
        UNIT_ASSERT(!IsVeryGoodTld("TiReS"));
        UNIT_ASSERT(IsVeryGoodTld("рф"));
        UNIT_ASSERT(!IsVeryGoodTld("РФ"));       // note that uppercase non-ascii tlds cannot be found
        UNIT_ASSERT(IsVeryGoodTld("xn--p1ai"));  // "рф"
        UNIT_ASSERT(!IsVeryGoodTld("xn--p1ag")); // "ру"
        UNIT_ASSERT(!IsVeryGoodTld("YaHOO"));
        UNIT_ASSERT(!IsVeryGoodTld("xn"));

        UNIT_ASSERT(InVeryGoodTld("ru.ru"));
        UNIT_ASSERT(InVeryGoodTld("яндекс.рф"));
        UNIT_ASSERT(InVeryGoodTld("http://xn--d1acpjx3f.xn--p1ai"));
        UNIT_ASSERT(!InVeryGoodTld("ru"));
        UNIT_ASSERT(!InVeryGoodTld("ru."));
        UNIT_ASSERT(!InVeryGoodTld("ru.xn"));
    }
}
