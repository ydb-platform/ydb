#include "sys.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NConsoleClient {

Y_UNIT_TEST_SUITE(SchemeObjectClassification) {

    Y_UNIT_TEST(EntryTypes) {
        using NScheme::ESchemeEntryType;

        const struct {
            TStringBuf Name;
            ESchemeEntryType Type;
            bool SystemDir;
            bool TemporaryDir;
            bool InternalDir;
            bool SystemObject;
            bool InternalObject;
        } cases[] = {
            {"user", ESchemeEntryType::Directory, false, false, false, false, false},
            {"user~suffix", ESchemeEntryType::Directory, false, false, false, false, false},
            {"~my_directory", ESchemeEntryType::Directory, false, false, false, false, false},
            {"~backup", ESchemeEntryType::Directory, false, false, false, false, false},
            {"user~backup_12345", ESchemeEntryType::Directory, false, false, false, false, false},
            {"~backup_12345", ESchemeEntryType::Directory, false, true, true, false, true},
            {"export-12345", ESchemeEntryType::Directory, false, true, true, false, true},
            {".sys", ESchemeEntryType::Directory, true, false, true, true, true},
            {".metadata", ESchemeEntryType::Directory, true, false, true, true, true},
            {".tmp", ESchemeEntryType::Directory, true, false, true, true, true},
            {".backups", ESchemeEntryType::Directory, true, false, true, true, true},
            {".sys_health", ESchemeEntryType::Directory, true, false, true, true, true},
            {".metadata_user", ESchemeEntryType::Directory, true, false, true, true, true},
            {".tmp_user", ESchemeEntryType::Directory, true, false, true, true, true},
            {".backups_user", ESchemeEntryType::Directory, true, false, true, true, true},
            {"~table", ESchemeEntryType::Table, false, false, false, false, false},
            {"~backup_table", ESchemeEntryType::Table, false, false, false, false, false},
            {"export-table", ESchemeEntryType::Table, false, false, false, false, false},
            {".sys", ESchemeEntryType::Table, false, false, false, false, false},
            {".sys_health", ESchemeEntryType::Table, false, false, false, false, false},
            {"view", ESchemeEntryType::SysView, false, false, false, true, true},
            {"~view", ESchemeEntryType::SysView, false, false, false, true, true},
            {"~backup_view", ESchemeEntryType::SysView, false, false, false, true, true},
        };

        for (const auto& testCase : cases) {
            NScheme::TSchemeEntry entry;
            entry.Name.assign(testCase.Name.data(), testCase.Name.size());
            entry.Type = testCase.Type;
            UNIT_ASSERT_VALUES_EQUAL_C(IsSystemDir(entry), testCase.SystemDir, testCase.Name);
            UNIT_ASSERT_VALUES_EQUAL_C(IsTemporaryDir(entry), testCase.TemporaryDir, testCase.Name);
            UNIT_ASSERT_VALUES_EQUAL_C(IsInternalDir(entry), testCase.InternalDir, testCase.Name);
            UNIT_ASSERT_VALUES_EQUAL_C(IsSystemObject(entry), testCase.SystemObject, testCase.Name);
            UNIT_ASSERT_VALUES_EQUAL_C(IsInternalObject(entry), testCase.InternalObject, testCase.Name);
        }
    }

} // Y_UNIT_TEST_SUITE

} // namespace NYdb::NConsoleClient
