#include <ydb/public/lib/ydb_cli/common/utf8_utils.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/charset/utf8.h>
#include <util/generic/string.h>

namespace NYdb::NConsoleClient {

namespace {

// Widen [begin, end) to UTF-8 boundaries and return the resulting substring, mirroring how callers
// turn the snapped range into a snippet.
TString Slice(TStringBuf text, size_t begin, size_t end) {
    const auto [b, e] = WidenToUtf8CharBoundaries(text, begin, end);
    return TString(text.substr(b, e - b));
}

bool IsValidUtf8(TStringBuf text) {
    size_t chars = 0;
    return GetNumberOfUTF8Chars(text.data(), text.size(), chars);
}

} // namespace

// "а", "Ы", "б" are 2-byte Cyrillic letters, so "аЫб" occupies bytes а=[0,1] Ы=[2,3] б=[4,5].
Y_UNIT_TEST_SUITE(Utf8UtilsTests) {
    Y_UNIT_TEST(AsciiRangeIsKeptIntact) {
        TStringBuf text = "hello world";
        UNIT_ASSERT_VALUES_EQUAL(Slice(text, 0, 5), "hello");
        UNIT_ASSERT_VALUES_EQUAL(Slice(text, 6, 11), "world");
    }

    Y_UNIT_TEST(BeginInsideMultibyteMovesLeft) {
        UNIT_ASSERT_VALUES_EQUAL(Slice("аЫб", 3, 6), "Ыб");
    }

    Y_UNIT_TEST(EndInsideMultibyteMovesRight) {
        UNIT_ASSERT_VALUES_EQUAL(Slice("аЫб", 0, 3), "аЫ");
    }

    Y_UNIT_TEST(BothEndsInsideMultibyte) {
        UNIT_ASSERT_VALUES_EQUAL(Slice("аЫб", 1, 5), "аЫб");
    }

    Y_UNIT_TEST(BoundaryOffsetsAreUnchanged) {
        UNIT_ASSERT_VALUES_EQUAL(Slice("аЫб", 2, 4), "Ы");
    }

    Y_UNIT_TEST(OffsetsClampedToSize) {
        UNIT_ASSERT_VALUES_EQUAL(Slice("abc", 2, 100), "c");
        UNIT_ASSERT_VALUES_EQUAL(Slice("abc", 100, 100), "");
    }

    Y_UNIT_TEST(InvertedRangeYieldsEmpty) {
        UNIT_ASSERT_VALUES_EQUAL(Slice("аЫб", 4, 2), "");
    }

    Y_UNIT_TEST(EmptyText) {
        UNIT_ASSERT_VALUES_EQUAL(Slice("", 0, 0), "");
    }

    Y_UNIT_TEST(EveryWindowOfMixedTextIsValidUtf8) {
        const TString text = "Привет, мир! CREATE INDEX по таблице ✓ end";
        UNIT_ASSERT_C(IsValidUtf8(text), "test fixture must be valid UTF-8");
        for (size_t begin = 0; begin <= text.size(); ++begin) {
            for (size_t end = begin; end <= text.size(); ++end) {
                const auto [b, e] = WidenToUtf8CharBoundaries(text, begin, end);
                const TStringBuf slice = TStringBuf(text).SubString(b, e - b);
                UNIT_ASSERT_C(IsValidUtf8(slice),
                    "window [" << begin << ", " << end << ") produced invalid UTF-8");
            }
        }
    }
}

} // namespace NYdb::NConsoleClient
