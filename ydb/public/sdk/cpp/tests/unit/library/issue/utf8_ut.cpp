#include <ydb/public/sdk/cpp/src/library/issue/utf8.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TUtf8Tests) {
    Y_UNIT_TEST(Simple) {
        UNIT_ASSERT(NYdb::NIssue::IsUtf8(""));
        UNIT_ASSERT(NYdb::NIssue::IsUtf8("\x01_ASCII_\x7F"));
        UNIT_ASSERT(NYdb::NIssue::IsUtf8("Привет!"));
        UNIT_ASSERT(NYdb::NIssue::IsUtf8("\xF0\x9F\x94\xA2"));

        UNIT_ASSERT(!NYdb::NIssue::IsUtf8("\xf5\x80\x80\x80"));
        UNIT_ASSERT(!NYdb::NIssue::IsUtf8("\xed\xa6\x80"));
        UNIT_ASSERT(!NYdb::NIssue::IsUtf8("\xF0\x9F\x94"));
        UNIT_ASSERT(!NYdb::NIssue::IsUtf8("\xE3\x85\xB6\xE7\x9C\xB0\xE3\x9C\xBA\xE2\xAA\x96\xEE\xA2\x8C\xEC\xAF\xB8\xE1\xB2\xBB\xEC\xA3\x9C\xE3\xAB\x8B\xEC\x95\x92\xE1\x8A\xBF\xE2\x8E\x86\xEC\x9B\x8D\xE2\x8E\xAE\xE3\x8A\xA3\xE0\xAC\xBC\xED\xB6\x85"));
        UNIT_ASSERT(!NYdb::NIssue::IsUtf8("\xc0\xbe\xd0\xb1\xd0\xbd\xd0\xbe\xd0\xb2\xd0\xbb\xd0\xb5\xd0\xbd\xd0\xb8\xd1\x8e"));
    }
}
