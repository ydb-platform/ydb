#include <util/charset/utf8.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

namespace {

// Helper: simulate the chunking loop from WriteJsonChunks and reassemble
TString ChunkAndReassemble(TStringBuf text, size_t chunkSize, size_t* partCount = nullptr) {
    TString reassembled;
    size_t parts = 0;
    TStringBuf remaining = text;
    while (!remaining.empty()) {
        TStringBuf chunk = Utf8TruncateRobust(remaining, chunkSize);
        if (chunk.empty()) {
            chunk = remaining.Head(std::min(remaining.size(), chunkSize));
        }
        reassembled += chunk;
        remaining.Skip(chunk.size());
        ++parts;
    }
    if (partCount) {
        *partCount = parts;
    }
    return reassembled;
}

} // namespace

Y_UNIT_TEST_SUITE(Utf8ChunkingTest) {

    // -- Utf8TruncateRobust boundary tests with SQL --

    Y_UNIT_TEST(SimpleSelectNotTruncated) {
        TStringBuf sql = "SELECT 1";
        // Size well beyond text — returns full string
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 100), sql);
    }

    Y_UNIT_TEST(TruncateInsideAsciiKeyword) {
        TStringBuf sql = "SELECT * FROM users WHERE id = 1";
        // Truncate at 10 — all ASCII, returns exactly 10 bytes
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 10).size(), 10u);
    }

    Y_UNIT_TEST(EmptyQuery) {
        TStringBuf sql = "";
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 0), "");
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 5), "");
    }

    Y_UNIT_TEST(CyrillicCommentTwoByteBoundary) {
        // "SELECT 1 -- " = 12 ASCII bytes, then 'ш' (0xD1 0x88) = 14 bytes total
        TString sql = "SELECT 1 -- \xD1\x88";
        UNIT_ASSERT_VALUES_EQUAL(sql.size(), 14u);
        // Truncate at 13 would split 'ш' — must back up to 12
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 13).size(), 12u);
        // Truncate at 12 — exact boundary before 'ш'
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 12).size(), 12u);
        // Truncate at 14 — full string
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 14).size(), 14u);
    }

    Y_UNIT_TEST(EuroSignInLiteralThreeByteBoundary) {
        // '€' is U+20AC = 0xE2 0x82 0xAC (3 bytes)
        // "SELECT '" = 8 bytes, then € (3 bytes), then "'" = 12 total
        TString sql = "SELECT '\xE2\x82\xAC'";
        UNIT_ASSERT_VALUES_EQUAL(sql.size(), 12u);
        // Truncate at 9 or 10 would split € — must back up to 8
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 9).size(), 8u);
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 10).size(), 8u);
        // Truncate at 11 — includes full € and closing quote
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 11).size(), 11u);
    }

    Y_UNIT_TEST(EmojiInCommentFourByteBoundary) {
        // U+1F600 = 0xF0 0x9F 0x98 0x80 (4 bytes)
        // "-- " = 3 bytes, then emoji (4 bytes), then "\nSELECT 1" = 16 total
        TString sql = "-- \xF0\x9F\x98\x80\nSELECT 1";
        // Truncate at 4,5,6 would split emoji — must back up to 3
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 4).size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 5).size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 6).size(), 3u);
        // Truncate at 7 — includes full emoji
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 7).size(), 7u);
    }

    Y_UNIT_TEST(UpsertWithGreekLiteral) {
        // 'Ω' is U+03A9 = 0xCE 0xA9 (2 bytes)
        TString sql = "UPSERT INTO t (val) VALUES ('\xCE\xA9')";
        // "UPSERT INTO t (val) VALUES ('" = 29 bytes, Ω at 29-30, "'" at 31
        UNIT_ASSERT_VALUES_EQUAL(sql.size(), 33u);
        // Truncate at 30 would split Ω — must back up to 29
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 30).size(), 29u);
        // Truncate at 31 — includes Ω and closing quote
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 31).size(), 31u);
    }

    Y_UNIT_TEST(CyrillicColumnAlias) {
        // "SELECT col AS " = 14 bytes, "цена" = ц(D1 86) е(D0 B5) н(D0 BD) а(D0 B0) = 8 bytes
        TString sql = "SELECT col AS \xD1\x86\xD0\xB5\xD0\xBD\xD0\xB0";
        UNIT_ASSERT_VALUES_EQUAL(sql.size(), 22u);
        // Truncate at 15 splits 'ц' — backs up to 14
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 15).size(), 14u);
        // Truncate at 16 — includes full 'ц'
        UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(sql, 16).size(), 16u);
    }

    // -- Chunking round-trip tests --

    Y_UNIT_TEST(ChunkingShortAsciiQuery) {
        TStringBuf sql = "SELECT * FROM users WHERE id = 42";
        size_t parts = 0;
        TString result = ChunkAndReassemble(sql, 10, &parts);
        UNIT_ASSERT_VALUES_EQUAL(result, sql);
        UNIT_ASSERT(parts > 1);
    }

    Y_UNIT_TEST(ChunkingQueryWithCyrillicComments) {
        // Realistic SQL with Cyrillic comments, small chunk size to force splits
        TString sql =
            "-- \xD0\x97\xD0\xB0\xD0\xBF\xD1\x80\xD0\xBE\xD1\x81 "  // "Запрос "
            "\xD0\xB4\xD0\xBB\xD1\x8F "                                // "для "
            "\xD0\xBF\xD0\xBE\xD0\xBB\xD1\x83\xD1\x87\xD0\xB5\xD0\xBD\xD0\xB8\xD1\x8F "  // "получения "
            "\xD0\xB4\xD0\xB0\xD0\xBD\xD0\xBD\xD1\x8B\xD1\x85\n"    // "данных\n"
            "SELECT u.id, u.name, o.amount\n"
            "FROM users AS u\n"
            "JOIN orders AS o ON u.id = o.user_id\n"
            "WHERE o.created_at > CurrentUtcDate() - Interval('P30D')\n"
            "ORDER BY o.amount DESC\n"
            "LIMIT 100;";

        size_t parts = 0;
        TString result = ChunkAndReassemble(sql, 20, &parts);
        UNIT_ASSERT_VALUES_EQUAL(result, sql);
        UNIT_ASSERT(parts > 5);
    }

    Y_UNIT_TEST(ChunkingLargeUpsertWithCyrillicValues) {
        // Large query with INSERT rows containing Cyrillic "Товар"
        TStringBuilder sql;
        sql << "UPSERT INTO products (id, name, price) VALUES\n";
        for (int i = 0; i < 200; ++i) {
            if (i > 0) sql << ",\n";
            // "Товар" = D0 A2 D0 BE D0 B2 D0 B0 D1 80 (10 bytes)
            sql << "(" << i << ", '\xD0\xA2\xD0\xBE\xD0\xB2\xD0\xB0\xD1\x80 #" << i << "', " << i * 100 << ")";
        }
        sql << ";";
        TString query = sql;

        size_t parts = 0;
        TString result = ChunkAndReassemble(query, 4000, &parts);
        UNIT_ASSERT_VALUES_EQUAL(result, query);
        UNIT_ASSERT(parts >= 2);
    }

    Y_UNIT_TEST(ChunkingQuerySmallerThanChunkSize) {
        TStringBuf sql = "SELECT 1";
        size_t parts = 0;
        TString result = ChunkAndReassemble(sql, 4000, &parts);
        UNIT_ASSERT_VALUES_EQUAL(result, sql);
        UNIT_ASSERT_VALUES_EQUAL(parts, 1u);
    }

    Y_UNIT_TEST(ChunkingCyrillicTableAndColumnNames) {
        // "SELECT `имя`, `фамилия` FROM `пользователи` WHERE `возраст` > 18"
        TString sql =
            "SELECT `\xD0\xB8\xD0\xBC\xD1\x8F`, `\xD1\x84\xD0\xB0\xD0\xBC\xD0\xB8\xD0\xBB\xD0\xB8\xD1\x8F`"
            " FROM `\xD0\xBF\xD0\xBE\xD0\xBB\xD1\x8C\xD0\xB7\xD0\xBE\xD0\xB2\xD0\xB0\xD1\x82\xD0\xB5\xD0\xBB\xD0\xB8`"
            " WHERE `\xD0\xB2\xD0\xBE\xD0\xB7\xD1\x80\xD0\xB0\xD1\x81\xD1\x82` > 18";

        size_t parts = 0;
        TString result = ChunkAndReassemble(sql, 15, &parts);
        UNIT_ASSERT_VALUES_EQUAL(result, sql);
        UNIT_ASSERT(parts > 1);
    }

    Y_UNIT_TEST(ChunkBoundaryNeverSplitsCyrillicInInsert) {
        // INSERT with a long Cyrillic string value
        TString cyrillicVal;
        for (int i = 0; i < 50; ++i) {
            cyrillicVal += "\xD0\xB0";  // 'а' — 2 bytes each
        }
        TString sql = "INSERT INTO t (col) VALUES ('" + cyrillicVal + "')";

        constexpr size_t chunkSize = 7;
        TStringBuf remaining = sql;
        while (!remaining.empty()) {
            TStringBuf chunk = Utf8TruncateRobust(remaining, chunkSize);
            if (chunk.empty()) {
                chunk = remaining.Head(std::min(remaining.size(), chunkSize));
            }
            // Verify chunk is valid UTF-8 by re-truncating at its own size
            UNIT_ASSERT_VALUES_EQUAL(Utf8TruncateRobust(chunk, chunk.size()), chunk);
            remaining.Skip(chunk.size());
        }
    }
}

} // namespace NKikimr::NKqp
