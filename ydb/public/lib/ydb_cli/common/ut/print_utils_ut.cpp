#include <ydb/public/lib/ydb_cli/common/print_utils.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(FormatBytesTests) {
    Y_UNIT_TEST(Zero) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(0), "0 B");
    }

    Y_UNIT_TEST(Bytes) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1), "1 B");
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(512), "512 B");
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1023), "1023 B");
    }

    Y_UNIT_TEST(KiB) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1024), "1 KiB");
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1536), "1.5 KiB");
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(10240), "10 KiB");
    }

    Y_UNIT_TEST(MiB) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1024 * 1024), "1 MiB");
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1024 * 1024 * 100), "100 MiB");
    }

    Y_UNIT_TEST(GiB) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1024ULL * 1024 * 1024), "1 GiB");
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1024ULL * 1024 * 1024 * 10), "10 GiB");
    }

    Y_UNIT_TEST(TiB) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1024ULL * 1024 * 1024 * 1024), "1 TiB");
    }
}

Y_UNIT_TEST_SUITE(FormatSpeedTests) {
    Y_UNIT_TEST(Zero) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatSpeed(0), "0 B/s");
    }

    Y_UNIT_TEST(BytesPerSecond) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatSpeed(100), "100 B/s");
    }

    Y_UNIT_TEST(KiBPerSecond) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatSpeed(1024), "1 KiB/s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatSpeed(10240), "10 KiB/s");
    }

    Y_UNIT_TEST(MiBPerSecond) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatSpeed(1024 * 1024), "1 MiB/s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatSpeed(1024 * 1024 * 50), "50 MiB/s");
    }
}

Y_UNIT_TEST_SUITE(FormatEtaTests) {
    Y_UNIT_TEST(LessThanSecond) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatEta(TDuration::MilliSeconds(500)), "<1s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatEta(TDuration::Zero()), "<1s");
    }

    Y_UNIT_TEST(Seconds) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatEta(TDuration::Seconds(1)), "1s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatEta(TDuration::Seconds(30)), "30s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatEta(TDuration::Seconds(59)), "59s");
    }

    Y_UNIT_TEST(Minutes) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatEta(TDuration::Seconds(60)), "1m");
        UNIT_ASSERT_STRINGS_EQUAL(FormatEta(TDuration::Seconds(90)), "1m 30s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatEta(TDuration::Seconds(3599)), "59m 59s");
    }

    Y_UNIT_TEST(Hours) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatEta(TDuration::Seconds(3600)), "1h");
        UNIT_ASSERT_STRINGS_EQUAL(FormatEta(TDuration::Seconds(3660)), "1h 1m");
        UNIT_ASSERT_STRINGS_EQUAL(FormatEta(TDuration::Seconds(3661)), "1h 1m");  // seconds ignored when hours present
        UNIT_ASSERT_STRINGS_EQUAL(FormatEta(TDuration::Seconds(7200)), "2h");
    }
}

Y_UNIT_TEST_SUITE(FormatDurationTests) {
    Y_UNIT_TEST(Zero) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Zero()), "0s");
    }

    Y_UNIT_TEST(Milliseconds) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::MilliSeconds(1)), "1ms");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::MilliSeconds(500)), "500ms");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::MilliSeconds(999)), "999ms");
    }

    Y_UNIT_TEST(Seconds) {
        // Less than 10 seconds - shows decimal
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::MilliSeconds(1500)), "1.5s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Seconds(5)), "5.0s");
        // 10 seconds or more - no decimal
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Seconds(10)), "10s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Seconds(59)), "59s");
    }

    Y_UNIT_TEST(Minutes) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Seconds(60)), "1m");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Seconds(90)), "1m 30s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Seconds(3599)), "59m 59s");
    }

    Y_UNIT_TEST(Hours) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Seconds(3600)), "1h");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Seconds(3660)), "1h 1m");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Seconds(7200)), "2h");
    }
}

Y_UNIT_TEST_SUITE(FormatTimeTests) {
    Y_UNIT_TEST(Zero) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatTime(TInstant::Zero()), "Unknown");
    }

    Y_UNIT_TEST(NonZero) {
        TInstant time = TInstant::Seconds(1000000000);
        TString result = FormatTime(time);
        // Should be a valid RFC822 string, not "Unknown"
        UNIT_ASSERT(result != "Unknown");
        UNIT_ASSERT(result.size() > 0);
    }
}

Y_UNIT_TEST_SUITE(PrettySizeTests) {
    Y_UNIT_TEST(Zero) {
        UNIT_ASSERT_STRINGS_EQUAL(PrettySize(0), "0B");
    }

    Y_UNIT_TEST(Small) {
        UNIT_ASSERT_STRINGS_EQUAL(PrettySize(100), "100B");
    }

    Y_UNIT_TEST(Kilo) {
        UNIT_ASSERT_STRINGS_EQUAL(PrettySize(1000), "1KB");
        UNIT_ASSERT_STRINGS_EQUAL(PrettySize(1500), "1.5KB");
    }

    Y_UNIT_TEST(Mega) {
        UNIT_ASSERT_STRINGS_EQUAL(PrettySize(1000000), "1MB");
    }
}

Y_UNIT_TEST_SUITE(PrettyNumberTests) {
    Y_UNIT_TEST(Zero) {
        UNIT_ASSERT_STRINGS_EQUAL(PrettyNumber(0), "0");
    }

    Y_UNIT_TEST(Small) {
        UNIT_ASSERT_STRINGS_EQUAL(PrettyNumber(100), "100");
    }

    Y_UNIT_TEST(Thousands) {
        UNIT_ASSERT_STRINGS_EQUAL(PrettyNumber(1000), "1K");
        UNIT_ASSERT_STRINGS_EQUAL(PrettyNumber(1500), "1.5K");
    }

    Y_UNIT_TEST(Millions) {
        UNIT_ASSERT_STRINGS_EQUAL(PrettyNumber(1000000), "1M");
    }
}

Y_UNIT_TEST_SUITE(BlurSecretTests) {
    Y_UNIT_TEST(Empty) {
        UNIT_ASSERT_STRINGS_EQUAL(BlurSecret(""), "");
    }

    Y_UNIT_TEST(Short) {
        // Very short strings - less blurring
        TString result = BlurSecret("abc");
        UNIT_ASSERT_EQUAL(result.size(), 3u);
    }

    Y_UNIT_TEST(Medium) {
        TString result = BlurSecret("12345678901234567890");
        // First 5 and last 5 chars should be visible (10 = min(10, 20/4))
        UNIT_ASSERT(result.StartsWith("12345"));
        UNIT_ASSERT(result.EndsWith("67890"));
        // Middle should be asterisks
        UNIT_ASSERT(result.Contains("*"));
    }

    Y_UNIT_TEST(Long) {
        TString input(100, 'x');
        TString result = BlurSecret(input);
        UNIT_ASSERT_EQUAL(result.size(), 100u);
        // First 10 and last 10 should be 'x'
        UNIT_ASSERT(result.substr(0, 10) == TString(10, 'x'));
        UNIT_ASSERT(result.substr(90, 10) == TString(10, 'x'));
        // Middle should be asterisks
        UNIT_ASSERT(result[50] == '*');
    }
}

Y_UNIT_TEST_SUITE(EntryTypeToStringTests) {
    Y_UNIT_TEST(Directory) {
        UNIT_ASSERT_STRINGS_EQUAL(EntryTypeToString(NYdb::NScheme::ESchemeEntryType::Directory), "dir");
    }

    Y_UNIT_TEST(Table) {
        UNIT_ASSERT_STRINGS_EQUAL(EntryTypeToString(NYdb::NScheme::ESchemeEntryType::Table), "table");
    }

    Y_UNIT_TEST(ColumnTable) {
        UNIT_ASSERT_STRINGS_EQUAL(EntryTypeToString(NYdb::NScheme::ESchemeEntryType::ColumnTable), "column-table");
    }

    Y_UNIT_TEST(Topic) {
        UNIT_ASSERT_STRINGS_EQUAL(EntryTypeToString(NYdb::NScheme::ESchemeEntryType::Topic), "topic");
    }

    Y_UNIT_TEST(SubDomain) {
        UNIT_ASSERT_STRINGS_EQUAL(EntryTypeToString(NYdb::NScheme::ESchemeEntryType::SubDomain), "sub-domain");
    }

    Y_UNIT_TEST(View) {
        UNIT_ASSERT_STRINGS_EQUAL(EntryTypeToString(NYdb::NScheme::ESchemeEntryType::View), "view");
    }

    Y_UNIT_TEST(Unknown) {
        UNIT_ASSERT_STRINGS_EQUAL(EntryTypeToString(NYdb::NScheme::ESchemeEntryType::Unknown), "unknown");
    }
}

Y_UNIT_TEST_SUITE(PrintPermissionsTests) {
    Y_UNIT_TEST(Empty) {
        TStringStream ss;
        std::vector<NYdb::NScheme::TPermissions> permissions;
        PrintPermissions(permissions, ss);
        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(), "none\n");
    }

    Y_UNIT_TEST(Single) {
        TStringStream ss;
        std::vector<NYdb::NScheme::TPermissions> permissions;
        NYdb::NScheme::TPermissions perm("user@domain", {"read", "write"});
        permissions.push_back(perm);
        PrintPermissions(permissions, ss);
        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(), "user@domain:read,write\n");
    }
}
