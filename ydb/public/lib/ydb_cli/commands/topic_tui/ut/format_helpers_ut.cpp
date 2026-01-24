#include "../widgets/sparkline.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(FormatHelpersTests) {

    // === FormatBytes Tests ===
    
    Y_UNIT_TEST(FormatBytes_Zero) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(0).c_str(), "0 B");
    }
    
    Y_UNIT_TEST(FormatBytes_Bytes) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1).c_str(), "1 B");
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(512).c_str(), "512 B");
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1023).c_str(), "1023 B");
    }
    
    Y_UNIT_TEST(FormatBytes_Kilobytes) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1024).c_str(), "1.0 KB");
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1536).c_str(), "1.5 KB");
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(10 * 1024).c_str(), "10.0 KB");
    }
    
    Y_UNIT_TEST(FormatBytes_Megabytes) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1024 * 1024).c_str(), "1.0 MB");
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(5 * 1024 * 1024 + 512 * 1024).c_str(), "5.5 MB");
    }
    
    Y_UNIT_TEST(FormatBytes_Gigabytes) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1024ULL * 1024 * 1024).c_str(), "1.0 GB");
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(10ULL * 1024 * 1024 * 1024).c_str(), "10.0 GB");
    }
    
    // === FormatDuration Tests ===
    
    Y_UNIT_TEST(FormatDuration_Seconds) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Seconds(0)).c_str(), "0s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Seconds(45)).c_str(), "45s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Seconds(59)).c_str(), "59s");
    }
    
    Y_UNIT_TEST(FormatDuration_Minutes) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Minutes(1)).c_str(), "1m 0s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Seconds(90)).c_str(), "1m 30s");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Minutes(59)).c_str(), "59m 0s");
    }
    
    Y_UNIT_TEST(FormatDuration_Hours) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Hours(1)).c_str(), "1h 0m");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Hours(2) + TDuration::Minutes(30)).c_str(), "2h 30m");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Hours(23)).c_str(), "23h 0m");
    }
    
    Y_UNIT_TEST(FormatDuration_Days) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Hours(24)).c_str(), "1d 0h");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Hours(48)).c_str(), "2d 0h");
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Hours(25)).c_str(), "1d 1h");
    }
    
    // === FormatNumber Tests ===
    
    Y_UNIT_TEST(FormatNumber_Small) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatNumber(0).c_str(), "0");
        UNIT_ASSERT_STRINGS_EQUAL(FormatNumber(1).c_str(), "1");
        UNIT_ASSERT_STRINGS_EQUAL(FormatNumber(999).c_str(), "999");
    }
    
    Y_UNIT_TEST(FormatNumber_Thousands) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatNumber(1000).c_str(), "1,000");
        UNIT_ASSERT_STRINGS_EQUAL(FormatNumber(12345).c_str(), "12,345");
        UNIT_ASSERT_STRINGS_EQUAL(FormatNumber(999999).c_str(), "999,999");
    }
    
    Y_UNIT_TEST(FormatNumber_Millions) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatNumber(1000000).c_str(), "1,000,000");
        UNIT_ASSERT_STRINGS_EQUAL(FormatNumber(1234567890).c_str(), "1,234,567,890");
    }
}
