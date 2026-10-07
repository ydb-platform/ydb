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

Y_UNIT_TEST_SUITE(FromBytesTests) {

Y_UNIT_TEST(ZeroBytes) {
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(0, "hello"), (TPosition{0, 0}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(0, ""), (TPosition{0, 0}));
}

Y_UNIT_TEST(AsciiSameLine) {
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(3, "hello"), (TPosition{0, 3}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(5, "hello"), (TPosition{0, 5}));
}

Y_UNIT_TEST(AsciiMultiline) {
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(6, "hello\nworld"), (TPosition{1, 0}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(11, "hello\nworld"), (TPosition{1, 5}));
}

Y_UNIT_TEST(EmptyLines) {
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(2, "a\n\nb"), (TPosition{1, 0}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(3, "a\n\nb"), (TPosition{2, 0}));
}

Y_UNIT_TEST(CrLfLineEndings) {
    TStringBuf text = "hello\r\nworld";
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(5, text), (TPosition{0, 5}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(7, text), (TPosition{1, 0}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(12, text), (TPosition{1, 5}));
}

Y_UNIT_TEST(LoneCrIsACharacter) {
    TStringBuf text = "hello\rworld";
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(5, text), (TPosition{0, 5}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(6, text), (TPosition{0, 6}));
}

Y_UNIT_TEST(MultibyteUtf8SameLine) {
    TStringBuf text = "привет";
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(0, text), (TPosition{0, 0}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(2, text), (TPosition{0, 1}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(6, text), (TPosition{0, 3}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(12, text), (TPosition{0, 6}));
}

Y_UNIT_TEST(MultibyteUtf8Multiline) {
    TStringBuf text = "привет\nмир";
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(13, text), (TPosition{1, 0}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(19, text), (TPosition{1, 3}));
}

Y_UNIT_TEST(TabCharacters) {
    TStringBuf text = "\thello";
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(0, text), (TPosition{0, 0}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(1, text), (TPosition{0, 1}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(6, text), (TPosition{0, 6}));
}

Y_UNIT_TEST(ThreeByteUtf8) {
    TStringBuf text = "中文";
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(0, text), (TPosition{0, 0}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(3, text), (TPosition{0, 1}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(6, text), (TPosition{0, 2}));
}

Y_UNIT_TEST(MixedAsciiAndMultibyte) {
    TStringBuf text = "aпbq";
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(1, text), (TPosition{0, 1}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(3, text), (TPosition{0, 2}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(4, text), (TPosition{0, 3}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(5, text), (TPosition{0, 4}));
}

Y_UNIT_TEST(NonBmpCodePoints) {
    TStringBuf text = "a😀b";
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(0, text), (TPosition{0, 0}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(1, text), (TPosition{0, 1}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(5, text), (TPosition{0, 3}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(6, text), (TPosition{0, 4}));
}

Y_UNIT_TEST(SingleEmoji) {
    TStringBuf text = "😀";
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(4, text), (TPosition{0, 2}));
}

Y_UNIT_TEST(MultipleNonBmp) {
    TStringBuf text = "😀😃";
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(4, text), (TPosition{0, 2}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(8, text), (TPosition{0, 4}));
}

Y_UNIT_TEST(TrailingNewline) {
    TStringBuf text = "hello\n";
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(6, text), (TPosition{1, 0}));
}

Y_UNIT_TEST(ManyEmptyLines) {
    TStringBuf text = "\n\n\n";
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(0, text), (TPosition{0, 0}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(1, text), (TPosition{1, 0}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(2, text), (TPosition{2, 0}));
    UNIT_ASSERT_VALUES_EQUAL(FromBytes(3, text), (TPosition{3, 0}));
}

Y_UNIT_TEST(BytesOutOfRange) {
    UNIT_ASSERT_EXCEPTION(FromBytes(6, "hello"), NLsp::TLspException);
    UNIT_ASSERT_EXCEPTION(FromBytes(1, ""), NLsp::TLspException);
}

Y_UNIT_TEST(MidMultibyteSequence) {
    TStringBuf text = "п";
    UNIT_ASSERT_EXCEPTION(FromBytes(1, text), NLsp::TLspException);
    UNIT_ASSERT_EXCEPTION(FromBytes(1, "中文"), NLsp::TLspException);
    UNIT_ASSERT_EXCEPTION(FromBytes(2, "中文"), NLsp::TLspException);
}

Y_UNIT_TEST(RoundtripOnQueryWithInterestingSymbols) {
    const TString text =
        "SELECT 'привет', x AS столбец\n"
        "FROM таблица -- комментарий 中文\n"
        "WHERE 😊 = 1 AND tag = \"a\\\"b\"\r\n"
        "\tORDER BY цена; -- emoji: 🎉\n";

    for (size_t byteA = 0; byteA <= text.size(); ++byteA) {
        const bool isCRLF =
            (0 < byteA && byteA < text.size() &&
             text[byteA - 1] == '\r' && text[byteA] == '\n');

        TPosition positionA;
        try {
            positionA = FromBytes(byteA, text);
        } catch (const TLspException& e) {
            continue; // unlucky byte
        }

        const size_t byteB = ToBytes(positionA, text);
        UNIT_ASSERT_VALUES_EQUAL_C(
            byteB,
            byteA + isCRLF,
            "" << "position " << positionA << ", "
               << "byte A " << byteA << ", "
               << "byte B " << byteB);

        const TPosition positionB = FromBytes(byteB, text);
        UNIT_ASSERT_VALUES_EQUAL_C(
            positionB,
            positionA,
            "" << "byte " << byteA << ", "
               << "position A " << positionA << ", "
               << "position B " << positionB);
    }
}

} // Y_UNIT_TEST_SUITE(FromBytesTests)
