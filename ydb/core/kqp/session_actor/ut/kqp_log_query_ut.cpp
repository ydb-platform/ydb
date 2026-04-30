#include <util/charset/utf8.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

namespace {

// Truncate helper matching the production logic: Utf8TruncateRobust, fallback to head on invalid UTF-8.
TString TruncateQueryText(TStringBuf text, size_t maxSize) {
    if (maxSize == 0 || text.size() <= maxSize) {
        return TString(text);
    }
    TStringBuf truncated = Utf8TruncateRobust(text, maxSize);
    if (truncated.empty()) {
        truncated = text.Head(maxSize);
    }
    return TString(truncated);
}

} // namespace

Y_UNIT_TEST_SUITE(Utf8TruncationTest) {

    Y_UNIT_TEST(ShortQueryNotTruncated) {
        TStringBuf sql = "SELECT 1";
        TString result = TruncateQueryText(sql, 100);
        UNIT_ASSERT_VALUES_EQUAL(result, sql);
    }

    Y_UNIT_TEST(EmptyQuery) {
        TString result = TruncateQueryText("", 100);
        UNIT_ASSERT_VALUES_EQUAL(result, "");
    }

    Y_UNIT_TEST(TruncateAscii) {
        TStringBuf sql = "SELECT * FROM users WHERE id = 1";
        TString result = TruncateQueryText(sql, 10);
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 10u);
        UNIT_ASSERT_VALUES_EQUAL(result, "SELECT * F");
    }

    Y_UNIT_TEST(TruncateAtCyrillicBoundary) {
        // "SELECT 1 -- " = 12 ASCII bytes, then 'ш' (0xD1 0x88) = 14 bytes total
        TString sql = "SELECT 1 -- \xD1\x88";
        UNIT_ASSERT_VALUES_EQUAL(sql.size(), 14u);
        // Truncate at 13 would split 'ш' — must back up to 12
        TString result = TruncateQueryText(sql, 13);
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 12u);
        UNIT_ASSERT_VALUES_EQUAL(result, "SELECT 1 -- ");
    }

    Y_UNIT_TEST(TruncateAtEuroSign) {
        // '€' is U+20AC = 0xE2 0x82 0xAC (3 bytes)
        TString sql = "SELECT '\xE2\x82\xAC'";
        UNIT_ASSERT_VALUES_EQUAL(sql.size(), 12u);
        // Truncate at 9 or 10 would split € — must back up to 8
        TString result = TruncateQueryText(sql, 9);
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 8u);
    }

    Y_UNIT_TEST(TruncateAtEmoji) {
        // U+1F600 = 0xF0 0x9F 0x98 0x80 (4 bytes)
        TString sql = "-- \xF0\x9F\x98\x80\nSELECT 1";
        // Truncate at 4 would split emoji — must back up to 3
        TString result = TruncateQueryText(sql, 4);
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(result, "-- ");
    }

    Y_UNIT_TEST(ExactlyAtLimit) {
        TStringBuf sql = "SELECT 1";
        TString result = TruncateQueryText(sql, sql.size());
        UNIT_ASSERT_VALUES_EQUAL(result, sql);
    }

    Y_UNIT_TEST(ZeroLimitMeansNoTruncation) {
        TStringBuf sql = "SELECT 1 FROM very_long_table_name";
        TString result = TruncateQueryText(sql, 0);
        UNIT_ASSERT_VALUES_EQUAL(result, sql);
    }

    Y_UNIT_TEST(LongAsciiQueryTruncatedAt20000) {
        TString sql(25000, 'A');
        TString result = TruncateQueryText(sql, 20000);
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 20000u);
    }

    Y_UNIT_TEST(LongCyrillicQueryTruncatedAtCharBoundary) {
        // Each 'а' is 2 bytes (0xD0 0xB0)
        TString cyrillicBlock;
        for (int i = 0; i < 15000; ++i) {
            cyrillicBlock += "\xD0\xB0";
        }
        // 30000 bytes total, limit 20000 — must land on even boundary
        TString result = TruncateQueryText(cyrillicBlock, 20000);
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 20000u);
        // Result must be valid UTF-8: re-truncating at its own size returns the same string
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(result, result.size()), result);
    }

    Y_UNIT_TEST(TruncatedResultIsValidUtf8) {
        TString sql =
            "-- \xD0\x97\xD0\xB0\xD0\xBF\xD1\x80\xD0\xBE\xD1\x81 "
            "\xD0\xB4\xD0\xBB\xD1\x8F "
            "\xD0\xBF\xD0\xBE\xD0\xBB\xD1\x83\xD1\x87\xD0\xB5\xD0\xBD\xD0\xB8\xD1\x8F "
            "\xD0\xB4\xD0\xB0\xD0\xBD\xD0\xBD\xD1\x8B\xD1\x85\n"
            "SELECT u.id, u.name, o.amount FROM users AS u "
            "JOIN orders AS o ON u.id = o.user_id";

        for (size_t limit : {5u, 10u, 20u, 30u, 50u}) {
            TString result = TruncateQueryText(sql, limit);
            UNIT_ASSERT(result.size() <= limit);
            UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(result, result.size()), result);
        }
    }
}

} // namespace NKikimr::NKqp
