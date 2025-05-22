#include "metadata.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBackup {

Y_UNIT_TEST_SUITE(PathsNormalizationTest) {
    Y_UNIT_TEST(NormalizeItemPath) {
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("/a/b/c/"), "a/b/c");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("/"), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath(""), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("//"), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("///a///b///"), "a/b");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPath("a/b/c"), "a/b/c");
    }

    Y_UNIT_TEST(NormalizeItemPrefix) {
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix("///a///b///c///"), "a///b///c");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix("a///b///c"), "a///b///c");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix("//"), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix("/"), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeItemPrefix(""), "");
    }

    Y_UNIT_TEST(NormalizeExportPrefix) {
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("///a///"), "///a");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("a/"), "a");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("a"), "a");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("/prefix//"), "/prefix");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix(""), "");
        UNIT_ASSERT_STRINGS_EQUAL(NormalizeExportPrefix("/prefix"), "/prefix");
    }
}

} // namespace NKikimr::NBackup
