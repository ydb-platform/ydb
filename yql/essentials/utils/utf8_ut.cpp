#include "utf8.h"

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TUtf8Tests) {
    Y_UNIT_TEST(Simple) {
        UNIT_ASSERT(NYql::IsUtf8(""));
        UNIT_ASSERT(NYql::IsUtf8("\x01_ASCII_\x7F"));
        UNIT_ASSERT(NYql::IsUtf8("Привет!"));
        UNIT_ASSERT(NYql::IsUtf8("\xF0\x9F\x94\xA2"));

        UNIT_ASSERT(!NYql::IsUtf8("\xf5\x80\x80\x80"));
        UNIT_ASSERT(!NYql::IsUtf8("\xed\xa6\x80"));
        UNIT_ASSERT(!NYql::IsUtf8("\xF0\x9F\x94"));
        UNIT_ASSERT(!NYql::IsUtf8("\xE3\x85\xB6\xE7\x9C\xB0\xE3\x9C\xBA\xE2\xAA\x96\xEE\xA2\x8C\xEC\xAF\xB8\xE1\xB2\xBB\xEC\xA3\x9C\xE3\xAB\x8B\xEC\x95\x92\xE1\x8A\xBF\xE2\x8E\x86\xEC\x9B\x8D\xE2\x8E\xAE\xE3\x8A\xA3\xE0\xAC\xBC\xED\xB6\x85"));
        UNIT_ASSERT(!NYql::IsUtf8("\xc0\xbe\xd0\xb1\xd0\xbd\xd0\xbe\xd0\xb2\xd0\xbb\xd0\xb5\xd0\xbd\xd0\xb8\xd1\x8e"));
    }

    Y_UNIT_TEST(CharSize) {
        UNIT_ASSERT_VALUES_EQUAL(NYql::WideCharSize(' '), 1);
        UNIT_ASSERT_VALUES_EQUAL(NYql::WideCharSize('\x00'), 1);
        UNIT_ASSERT_VALUES_EQUAL(NYql::WideCharSize('\x7F'), 1);
        UNIT_ASSERT_VALUES_EQUAL(NYql::WideCharSize('\xD1'), 2);
        UNIT_ASSERT_VALUES_EQUAL(NYql::WideCharSize('\xF0'), 4);
        UNIT_ASSERT_VALUES_EQUAL(NYql::WideCharSize('\xFF'), 0);
    }

    Y_UNIT_TEST(RoundingDown) {
        auto checkDown = [](std::string_view in, std::string_view out) {
            auto res = NYql::RoundToNearestValidUtf8(in, true);
            UNIT_ASSERT(res);
            UNIT_ASSERT(NYql::IsUtf8(*res));
            UNIT_ASSERT_VALUES_EQUAL(*res, out);
            UNIT_ASSERT(*res <= in);
        };
        checkDown("привет", "привет");
        checkDown("тест\x80", "тест\x7f");
        checkDown("привет\xf5", "привет\xf4\x8f\xbf\xbf");
        checkDown("тест2\xee\x80\x7f", "тест2\xed\x9f\xbf");
        checkDown("ага\xf0\xaa\xaa\xff", "ага\xf0\xaa\xaa\xbf");
    }

    Y_UNIT_TEST(RoundingUp) {
        auto checkUp = [](std::string_view in, std::string_view out) {
            auto res = NYql::RoundToNearestValidUtf8(in, false);
            UNIT_ASSERT(res);
            UNIT_ASSERT(NYql::IsUtf8(*res));
            UNIT_ASSERT_VALUES_EQUAL(*res, out);
            UNIT_ASSERT(*res >= in);
        };

        checkUp("", "");
        checkUp("привет", "привет");
        checkUp("а\xf6", "б");
        checkUp("\xf4\x8f\xbf\xbfа\xf4\x8f\xbf\xbf\xf5", "\xf4\x8f\xbf\xbfб");
        UNIT_ASSERT(!NYql::RoundToNearestValidUtf8("\xf4\x8f\xbf\xbf\xf5", false));
        UNIT_ASSERT(!NYql::RoundToNearestValidUtf8("\xf5", false));
        checkUp("тест\x80", "тест\xc2\x80");
        checkUp("тест\xdf", "тест\xdf\x80");
        checkUp("тест\xf0\x90\xff", "тест\xf0\x91\x80\x80");
        checkUp("ааа\xff", "ааб");
    }

    Y_UNIT_TEST(NextValid) {
        auto checkNext = [](std::string_view in, std::string_view out) {
            auto res = NYql::NextValidUtf8(in);
            UNIT_ASSERT(res);
            UNIT_ASSERT(NYql::IsUtf8(*res));
            UNIT_ASSERT_VALUES_EQUAL(*res, out);
            UNIT_ASSERT(*res > in);
        };

        UNIT_ASSERT(!NYql::NextValidUtf8(""));
        checkNext("привет", "привеу");
        checkNext("а", "б");
        checkNext(std::string_view("\x00", 1), "\x01");
        checkNext("\xf4\x8f\xbf\xbfа\xf4\x8f\xbf\xbf", "\xf4\x8f\xbf\xbfб");
        UNIT_ASSERT(!NYql::NextValidUtf8("\xf4\x8f\xbf\xbf"));
        UNIT_ASSERT(!NYql::NextValidUtf8("\xf4\x8f\xbf\xbf\xf4\x8f\xbf\xbf"));
    }

    Y_UNIT_TEST(NextValidString) {
        auto checkNext = [](std::string_view in, std::string_view out) {
            auto res = NYql::NextLexicographicString(in);
            UNIT_ASSERT(res);
            UNIT_ASSERT_VALUES_EQUAL(*res, out);
            UNIT_ASSERT(*res > in);
        };

        UNIT_ASSERT(!NYql::NextLexicographicString(""));
        checkNext("привет", "привеу");
        checkNext("а", "б");
        checkNext(std::string_view("\x00", 1), "\x01");
        checkNext("\xf4\x8f\xbf\xbfа\xf4\x8f\xbf\xbf", "\xf4\x8f\xbf\xbfа\xf4\x8f\xbf\xc0");
        UNIT_ASSERT(!NYql::NextLexicographicString("\xff"));
        UNIT_ASSERT(!NYql::NextLexicographicString("\xff\xff"));
        checkNext(std::string_view("x\x00\xff\xff", 4), "x\x01");
    }
}
