#include "position.h"

#include <yql/essentials/tools/yql_language_server/lsp/message/exception.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NLsp;

Y_UNIT_TEST_SUITE(ToBytesTests) {

Y_UNIT_TEST(ZeroPosition) {
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 0}, "hello"), 0U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 0}, ""), 0U);
}

Y_UNIT_TEST(AsciiSameLine) {
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 3}, "hello"), 3U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 5}, "hello"), 5U);
}

Y_UNIT_TEST(AsciiMultiline) {
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({1, 0}, "hello\nworld"), 6U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({1, 5}, "hello\nworld"), 11U);
}

Y_UNIT_TEST(EmptyLines) {
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({1, 0}, "a\n\nb"), 2U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({2, 0}, "a\n\nb"), 3U);
}

Y_UNIT_TEST(CrLfLineEndings) {
    TStringBuf text = "hello\r\nworld";
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 5}, text), 5U);
    UNIT_ASSERT_EXCEPTION(ToBytes({0, 6}, text), NLsp::TLspException);
    UNIT_ASSERT_EXCEPTION(ToBytes({0, 7}, text), NLsp::TLspException);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({1, 0}, text), 7U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({1, 5}, text), 12U);
}

Y_UNIT_TEST(LoneCrIsACharacter) {
    TStringBuf text = "hello\rworld";
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 5}, text), 5U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 6}, text), 6U);
}

Y_UNIT_TEST(MultibyteUtf8SameLine) {
    TStringBuf text = "привет";
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 0}, text), 0U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 1}, text), 2U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 3}, text), 6U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 6}, text), 12U);
}

Y_UNIT_TEST(MultibyteUtf8Multiline) {
    TStringBuf text = "привет\nмир";
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({1, 0}, text), 13U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({1, 3}, text), 19U);
}

Y_UNIT_TEST(InvalidLine) {
    UNIT_ASSERT_EXCEPTION(ToBytes({1, 0}, "hello"), NLsp::TLspException);
    UNIT_ASSERT_EXCEPTION(ToBytes({1, 0}, ""), NLsp::TLspException);
}

Y_UNIT_TEST(InvalidCharacter) {
    UNIT_ASSERT_EXCEPTION(ToBytes({0, 6}, "hello"), NLsp::TLspException);
}

Y_UNIT_TEST(InvalidCharacterAtNewline) {
    UNIT_ASSERT_EXCEPTION(ToBytes({0, 6}, "hello\nworld"), NLsp::TLspException);
}

Y_UNIT_TEST(TabCharacters) {
    TStringBuf text = "\thello";
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 0}, text), 0U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 1}, text), 1U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 6}, text), 6U);
}

Y_UNIT_TEST(ThreeByteUtf8) {
    TStringBuf text = "中文";
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 0}, text), 0U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 1}, text), 3U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 2}, text), 6U);
}

Y_UNIT_TEST(MixedAsciiAndMultibyte) {
    TStringBuf text = "aпbq";
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 1}, text), 1U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 2}, text), 3U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 3}, text), 4U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 4}, text), 5U);
}

Y_UNIT_TEST(MultibyteBeforeNewlineSkip) {
    TStringBuf text = "привет\nмир\nконец";
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({2, 0}, text), 20U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({2, 5}, text), 30U);
}

Y_UNIT_TEST(PositionAtEndOfLine) {
    TStringBuf text = "hi\nbye";
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 2}, text), 2U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({1, 3}, text), 6U);
}

Y_UNIT_TEST(TrailingNewline) {
    TStringBuf text = "hello\n";
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({1, 0}, text), 6U);
}

Y_UNIT_TEST(TrailingNewlineCharacterOutOfRange) {
    TStringBuf text = "hello\n";
    UNIT_ASSERT_EXCEPTION(ToBytes({1, 1}, text), NLsp::TLspException);
}

Y_UNIT_TEST(ManyEmptyLines) {
    TStringBuf text = "\n\n\n";
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 0}, text), 0U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({1, 0}, text), 1U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({2, 0}, text), 2U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({3, 0}, text), 3U);
}

Y_UNIT_TEST(LineWayOutOfRange) {
    UNIT_ASSERT_EXCEPTION(ToBytes({100, 0}, "a\nb"), NLsp::TLspException);
}

Y_UNIT_TEST(InvalidUtf8InCharacterLoop) {
    TStringBuf text = "\xFF";
    UNIT_ASSERT_EXCEPTION(ToBytes({0, 1}, text), NLsp::TLspException);
}

Y_UNIT_TEST(InvalidUtf8WhileSkippingLine) {
    TStringBuf text = "\xFF\nworld";
    UNIT_ASSERT_EXCEPTION(ToBytes({1, 0}, text), NLsp::TLspException);
}

Y_UNIT_TEST(TruncatedMultibyteSequence) {
    TStringBuf text = "\xD0";
    UNIT_ASSERT_EXCEPTION(ToBytes({0, 1}, text), NLsp::TLspException);
}

Y_UNIT_TEST(NonBmpCodePoints) {
    TStringBuf text = "a😀b";

    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 0}, text), 0U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 1}, text), 1U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 3}, text), 5U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 4}, text), 6U);
}

Y_UNIT_TEST(SingleEmoji) {
    TStringBuf text = "😀";
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 2}, text), 4U);
}

Y_UNIT_TEST(MultipleNonBmp) {
    TStringBuf text = "😀😃";
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 2}, text), 4U);
    UNIT_ASSERT_VALUES_EQUAL(ToBytes({0, 4}, text), 8U);
}

} // Y_UNIT_TEST_SUITE(ToBytesTests)
