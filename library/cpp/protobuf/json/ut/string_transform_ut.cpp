#include "json.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/protobuf/json/proto2json.h>

Y_UNIT_TEST_SUITE(TDoubleEscapeTransform) {
    Y_UNIT_TEST(TestEmptyString) {
        const NProtobufJson::IStringTransform& transform = NProtobufJson::TDoubleEscapeTransform();
        TString s;
        s = "";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL(s, "");
    }

    Y_UNIT_TEST(TestAlphabeticString) {
        const NProtobufJson::IStringTransform& transform = NProtobufJson::TDoubleEscapeTransform();
        TString s;
        s = "abacaba";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL(s, "abacaba");
    }

    Y_UNIT_TEST(TestRussianSymbols) {
        const NProtobufJson::IStringTransform& transform = NProtobufJson::TDoubleEscapeTransform();
        TString s;
        s = "тест";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL(s, "\\\\321\\\\202\\\\320\\\\265\\\\321\\\\201\\\\321\\\\202");
    }

    Y_UNIT_TEST(TestEscapeSpecialSymbols) {
        const NProtobufJson::IStringTransform& transform = NProtobufJson::TDoubleEscapeTransform();
        TString s;
        s = "aba\\ca\"ba";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL(s, "aba\\\\\\\\ca\\\\\\\"ba");
    }
}

Y_UNIT_TEST_SUITE(TDoubleUnescapeTransform) {
    Y_UNIT_TEST(TestEmptyString) {
        const NProtobufJson::IStringTransform& transform = NProtobufJson::TDoubleUnescapeTransform();
        TString s;
        s = "";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL("", s);
    }

    Y_UNIT_TEST(TestAlphabeticString) {
        const NProtobufJson::IStringTransform& transform = NProtobufJson::TDoubleUnescapeTransform();
        TString s;
        s = "abacaba";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL("abacaba", s);
    }

    Y_UNIT_TEST(TestRussianSymbols) {
        const NProtobufJson::IStringTransform& transform = NProtobufJson::TDoubleUnescapeTransform();
        TString s;
        s = "\\\\321\\\\202\\\\320\\\\265\\\\321\\\\201\\\\321\\\\202";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL("тест", s);
    }

    Y_UNIT_TEST(TestEscapeSpecialSymbols) {
        const NProtobufJson::IStringTransform& transform = NProtobufJson::TDoubleUnescapeTransform();
        TString s;
        s = "aba\\\\\\\\ca\\\\\\\"ba";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL("aba\\ca\"ba", s);
    }

    Y_UNIT_TEST(TestEscapeSpecialSymbolsDifficultCases) {
        const NProtobufJson::IStringTransform& transform = NProtobufJson::TDoubleUnescapeTransform();
        TString s;
        s = "\\\\\\\\\\\\\\\\";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL("\\\\", s);

        s = "\\\\\\\\\\\\\\\"";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL("\\\"", s);

        s = "\\\\\\\"\\\\\\\\";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL("\"\\", s);

        s = "\\\\\\\"\\\\\\\"";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL("\"\"", s);

        s = "\\\\\\\\\\\\\\\\\\\\\\\\";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL("\\\\\\", s);

        s = "\\\\\\\\\\\\\\\\\\\\\\\\abacaba\\\\";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL("\\\\\\abacaba", s);

        s = "\\\\\\\\\\\\\\\\\\\\\\\\abacaba\\\"";
        transform.Transform(s);
        UNIT_ASSERT_EQUAL("\\\\\\abacaba\"", s);
    }
}
