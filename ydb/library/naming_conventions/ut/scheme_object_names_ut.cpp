#include <ydb/library/naming_conventions/scheme_object_names.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NNaming {

Y_UNIT_TEST_SUITE(SchemeObjectNames) {

    Y_UNIT_TEST(Names) {
        const struct {
            TStringBuf Name;
            bool System;
            bool Temporary;
            bool Internal;
        } cases[] = {
            {"", false, false, false},
            {"user", false, false, false},
            {"user~suffix", false, false, false},
            {"~my_directory", false, false, false},
            {"~", false, false, false},
            {"~backup", false, false, false},
            {"~backup-12345", false, false, false},
            {"user~backup_12345", false, false, false},
            {"~Backup_12345", false, false, false},
            {"~backup_", false, true, true},
            {"~backup_12345", false, true, true},
            {"export", false, false, false},
            {"export_user", false, false, false},
            {"user-export-12345", false, false, false},
            {"Export-12345", false, false, false},
            {"export-", false, true, true},
            {"export-12345", false, true, true},
            {".sys", true, false, true},
            {".sys_health", true, false, true},
            {".metadata", true, false, true},
            {".metadata_custom", true, false, true},
            {".tmp", true, false, true},
            {".tmp_custom", true, false, true},
            {".backups", true, false, true},
            {".backups_custom", true, false, true},
            {"user.sys", false, false, false},
            {"user.metadata", false, false, false},
            {"user.tmp", false, false, false},
            {"user.backups", false, false, false},
            {".SYS", false, false, false},
        };

        for (const auto& testCase : cases) {
            UNIT_ASSERT_VALUES_EQUAL_C(IsSystemName(testCase.Name), testCase.System, testCase.Name);
            UNIT_ASSERT_VALUES_EQUAL_C(IsTemporaryName(testCase.Name), testCase.Temporary, testCase.Name);
            UNIT_ASSERT_VALUES_EQUAL_C(IsInternalName(testCase.Name), testCase.Internal, testCase.Name);
        }
    }

} // Y_UNIT_TEST_SUITE

} // namespace NKikimr::NNaming
